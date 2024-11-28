package storelistener

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/AlekSi/pointer"
	"github.com/caddyserver/caddy/v2"
	"github.com/getsentry/sentry-go"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Flag to toggle on/off some inner loop logging I don't want to take the cost of in production
const (
	DEBUGGING        = false
	SIMULATE_FAILURE = false
)

type Projection interface {
	Collection() string
	Update(context.Context, *firestore.DocumentSnapshot) error
	Swap(*Listener)
}

type Listener struct {
	ctx             context.Context
	cancel          context.CancelFunc
	firestoreClient *firestore.Client
	logger          *zap.Logger

	failureBudget         *atomic.Int64
	restartCount          *atomic.Int64
	lastCheckFailuresTime time.Time
	lastRestartTime       time.Time

	infra      *InfraProjection
	infraMutex sync.RWMutex
}

func NewFirestoreListener(ctx context.Context, logger *zap.Logger) (*Listener, error) {
	client, err := firestore.NewClient(context.Background(), GCP_PROJECT)
	if err != nil {
		return nil, err
	}

	// This use of context is a bit hacky, some refactoring can probably make the code cleaner
	ctx, cancel := context.WithCancel(ctx)

	l := &Listener{
		ctx:             ctx,
		cancel:          cancel,
		firestoreClient: client,
		logger:          logger,

		failureBudget:         new(atomic.Int64),
		restartCount:          new(atomic.Int64),
		lastCheckFailuresTime: time.Now(),
		lastRestartTime:       time.Now(),

		infra: NewInfraProjection(),
	}

	return l, nil
}

// Destruct implements caddy.Destructor
func (l *Listener) Destruct() error {
	// This should make the background goroutine exit
	l.cancel()

	l.logger.Info("Running project listener destructor")

	// Wait for graceful shutdown of goroutine
	time.Sleep(100 * time.Millisecond)

	// This will make the background goroutine start failing
	return l.firestoreClient.Close()
}

var _ caddy.Destructor = (*Listener)(nil)

// Note: If this panics we want it to blow up the service
func (l *Listener) Start(firstSyncDone func()) {
	l.logger.Info("firestorelistener.Start")

	go l.retryForever(l.ctx, firstSyncDone)
}

func (l *Listener) DieNow() {
	l.cancel()
	sentry.Flush(2 * time.Second)
	l.logger.Fatal("KILLING SERVICE FROM LISTENER")
	// I don't think we get past the fatal log
	panic(ErrUnalivingService)
}

// Note: If this panics we want it to blow up the service
func (l *Listener) retryForever(ctx context.Context, firstSyncDone func()) {
	l.logger.Info("firestorelistener.retryForever")

	hub := sentry.GetHubFromContext(ctx)
	defer func() {
		l.logger.Error("firestorelistener.retryForever exiting")
		hub.CaptureMessage("listenToSnapshots returning, canceled or panic")
		sentry.Flush(2 * time.Second)
	}()

	// - If time since last restart is < 60 seconds, count restarts
	// - If restarts > 5, die to restart service
	restartCount := 0

	// Loop until canceled
	for ctx.Err() == nil {
		lastRestartTime := time.Now()

		err := l.listenToSnapshots(ctx, firstSyncDone)
		if err != nil {
			l.logger.Error("listenToSnapshots returned error", zap.Error(err))
			hub.CaptureException(errors.Wrap(err, "listenToSnapshots returned error"))
		} else {
			l.logger.Error("listenToSnapshots returned without error")
			hub.CaptureMessage("listenToSnapshots returned without error")
		}

		// If the listener has worked for some time,
		// just reset and restart right away with no delay
		const acceptableRestartPeriod = 5 * time.Minute
		timeSince := time.Since(lastRestartTime)
		if timeSince > acceptableRestartPeriod {
			restartCount = 0
			l.logger.Error("restarting listenToSnapshots immediately")
			hub.CaptureMessage("restarting listenToSnapshots immediately")
			continue
		}

		// As long as we're looking at more rapid restarts, let the counter run
		restartCount++
		// and if we get a lot of them just die
		if restartCount > 10 {
			// Not expecting this to happen a lot so we should ideally never get here,
			// but if firestore connections are rotten or something then a forceful
			// full service restart may be better
			hub.CaptureException(ErrSnapshotListenerFailed)
			l.DieNow()
		}

		// Don't restart too quickly
		l.logger.Warn("firestorelistener.retryForever sleeping before next listenToSnapshots attempt", zap.Int("restartCount", restartCount))
		time.Sleep(time.Second)
	}
}

// Note: If this panics we want it to blow up the service.
// Note: If this returns error we want it to be called again.
func (l *Listener) listenToSnapshots(ctx context.Context, firstSyncDone func()) error {
	l.logger.Info("firestorelistener.listenToSnapshots")

	collection := CollectionAppbutlers

	col := l.firestoreClient.Collection(collection)
	if col == nil {
		panic(errors.Errorf("Collection %s not found", collection))
	}

	query := col.
		// 'select' clauses are not supported for real-time queries."
		// Select(AppbutlerDoc{}.Fields()...).
		// Skip when marked for garbage collection
		Where("markedForDeletionAt", "==", nil).
		// Skip free pool appbutlers without assigned projects, there's nothing to route
		// "firestore: must use '==' when comparing <nil>"
		// Where("projectId", "!=", nil).
		// Skip before service is ready to receive requests
		Where("serviceIsReady", "==", true)

	// TODO: Get this from env var to run locally
	// TODO: Review query performance explanation!
	// const getMetrics = false
	// if getMetrics {
	// 	query = query.WithRunOptions(firestore.ExplainOptions{Analyze: getMetrics})
	// 	snapIter := query.Snapshots(ctx)
	// 	docsIter := snapIter.Query.Documents(ctx)
	// 	docs, err := docsIter.GetAll()
	// 	// docsIter.Stop()
	// 	metrics, err := docsIter.ExplainMetrics()
	// 	l.dumpMetrics(snap)
	// 	return nil
	// }

	// Create a fresh blank infra projection to populate
	infra := NewInfraProjection()
	first := true

	totalChangeCount := new(atomic.Int64)
	totalErrorCount := new(atomic.Int64)
	l.resetFailures()

	// Check for failures regularly and use cancel to restart listener
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			err := l.checkFailures()
			l.logger.Info("Listener health check", zap.Time("lastRestartTime", l.lastRestartTime), zap.Int64("totalChanges", totalChangeCount.Load()), zap.Int64("totalErrors", totalErrorCount.Load()), zap.Error(err))
			if err != nil {
				cancel()
				return
			}
			time.Sleep(10 * time.Second)
		}
	}()

	// Until canceled
	for ctx.Err() == nil {
		snapIter := query.Snapshots(ctx)
		for ctx.Err() == nil {
			snap, err := snapIter.Next()
			if err != nil {
				l.logger.Error("Snapshot iterator error", zap.Error(err))
				return err
			}

			batchChangeCount, batchErrorCount := l.processSnapshot(ctx, infra, snap)
			totalChangeCount.Add(int64(batchChangeCount))
			totalErrorCount.Add(int64(batchErrorCount))

			// Nice to have in logs while the system is new
			// if DEBUGGING {
			l.logger.Info("Processed batch", zap.Int("changes", batchChangeCount), zap.Int("errors", batchErrorCount))
			//}

			if first {
				first = false

				// Swap out the infra projection after the first snapshot has completed processing
				l.SetInfra(infra)

				// Notify the provisioner that we've done the first sync.
				firstSyncDone()
			} else {
				if SIMULATE_FAILURE {
					return ErrSimulatedFailure
				}
			}
		}
	}

	return nil
}

func (l *Listener) dumpMetrics(snap *firestore.QuerySnapshot) {
	metrics, err := snap.Documents.ExplainMetrics()
	if err != nil || metrics == nil || metrics.PlanSummary == nil {
		panic(errors.Wrap(err, "Failed to get metrics"))
	}
	for _, idx := range metrics.PlanSummary.IndexesUsed {
		if idx != nil {
			for k, v := range *idx {
				// TODO: Adjust this as needed, v is type any
				fmt.Printf("%s: %+v\n", k, v)
			}
		}
	}
}

func kindAsString(kind firestore.DocumentChangeKind) string {
	switch kind {
	case firestore.DocumentAdded:
		return "added"
	case firestore.DocumentModified:
		return "modified"
	case firestore.DocumentRemoved:
		return "removed"
	}
	return "unknown"
}

// processSnapshot modifies infra projection with changes from snapshot,
// logging errors and returning the number of errors
func (l *Listener) processSnapshot(ctx context.Context, infra *InfraProjection, snap *firestore.QuerySnapshot) (int, int) {
	if DEBUGGING {
		l.logger.Info("processSnapshot")
	}

	errorCount := 0
	for i, change := range snap.Changes {
		var err error
		switch change.Kind {
		case firestore.DocumentAdded, firestore.DocumentModified:
			err = infra.Upsert(change.Doc)
		case firestore.DocumentRemoved:
			err = infra.Remove(change.Doc)
		default:
			panic("Invalid change kind")
		}

		if DEBUGGING {
			l.logger.Info(
				"processSnapshot.change",
				zap.String("kind", kindAsString(change.Kind)),
				zap.String("path", change.Doc.Ref.Path),
				zap.Error(err),
			)
		}

		// If processing one document fails, notify on sentry
		if err != nil {
			errorCount += 1
			l.logger.Error("Processing docucment change", zap.Error(err), zap.Int("errorsInBatch", errorCount))
			hub := sentry.GetHubFromContext(ctx)
			hub.WithScope(func(scope *sentry.Scope) {
				scope.SetTag("kind", kindAsString(change.Kind))
				scope.SetTag("ref", change.Doc.Ref.Path)
				scope.SetTag("docIndex", strconv.Itoa(i))
				scope.SetTag("failedSoFar", strconv.Itoa(errorCount))
				scope.SetLevel(sentry.LevelError)
				hub.CaptureException(err)
			})
		}
	}

	return len(snap.Changes), errorCount
}

func (l *Listener) SetInfra(infra *InfraProjection) {
	l.infraMutex.Lock()
	defer l.infraMutex.Unlock()
	l.infra = infra
}

func (l *Listener) GetInfra() *InfraProjection {
	l.infraMutex.RLock()
	defer l.infraMutex.RUnlock()
	return l.infra
}

// LookupUpstreamHost does an optimized cache lookup to get the upstream url.
// The rest of this Listener code is basically written to support this fast lookup.
func (l *Listener) LookupUpstreamHost(projectID, serviceType string) string {
	service, ok := l.GetInfra().GetService(projectID, serviceType)
	if !ok {
		l.registerFailure()
	}
	return service.Upstream
}

// LookupUsername does an optimized cache lookup to get the username for a project.
// The rest of this Listener code is basically written to support this fast lookup.
func (l *Listener) LookupUsername(projectID, serviceType string) string {
	service, _ := l.GetInfra().GetService(projectID, serviceType)
	return service.Username
}

// LookupUpProjectIdFromDomain does an optimized cache lookup to get the projectId a custom domain is associated with.
// The rest of this Listener code is basically written to support this fast lookup.
func (l *Listener) LookupUpProjectIdFromDomain(customDomain string) string {
	domain, _ := l.GetInfra().GetDomain(customDomain)
	return domain.ProjectId
}

// Parameters for failure budget tuning
const (
	maxFailureBudget           = 100
	budgetPerSecond            = 0.1
	minimumTimeBetweenRestarts = 30 * time.Second
)

func (l *Listener) resetFailures() {
	l.failureBudget.Store(maxFailureBudget)
	l.lastRestartTime = time.Now()
}

// NB! This is called from upstreams module threads!
func (l *Listener) registerFailure() int {
	l.logger.Info("listener: registering failure")
	return int(l.failureBudget.Add(-1))
}

// TODO: If we don't get any appbutler changes this never runs...
func (l *Listener) checkFailures() error {
	budget := l.failureBudget.Load()

	// Add some failure budget over time, this way we'll always tend to go back to the max,
	// and need sustained errors over some time to actually call it failure
	addToBudget := int64(time.Since(l.lastCheckFailuresTime).Seconds() * budgetPerSecond)
	l.lastCheckFailuresTime = time.Now()

	// Add and clamp to max
	budget += addToBudget
	if budget > maxFailureBudget {
		budget = maxFailureBudget
	}

	// Store the new budget, ignore if it has been touched between, this is not that accurate anyway
	l.failureBudget.Store(budget)

	// Don't restart too rapidly
	if time.Since(l.lastRestartTime) < minimumTimeBetweenRestarts {
		return nil
	}

	// Break circuit if we're out of failure budget
	if budget <= 0 {
		return ErrCircuitBroken
	}
	return nil
}

func (l *Listener) CountUpstreams() int {
	return sizeOfSynMap(l.GetInfra().Services)
}

func (l *Listener) CountDomains() int {
	return sizeOfSynMap(l.GetInfra().Domains)
}

func sizeOfSynMap(m *sync.Map) int {
	n := pointer.To(0)
	m.Range(func(k, v any) bool { *n += 1; return true })
	return *n
}

// Interface guards
var (
	_ caddy.Destructor = (*Listener)(nil)
)
