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

	cancelSnapshots context.CancelFunc
	failureCount    *atomic.Int64
	lastRestartTime time.Time

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

		cancelSnapshots: nil,
		failureCount:    &atomic.Int64{},
		lastRestartTime: time.Now(),

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

func (l *Listener) DieNow(err error) {
	l.cancel()
	sentry.Flush(2 * time.Second)
	l.logger.Fatal("KILLING SERVICE FROM LISTENER", zap.Error(err))
	panic(err)
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

	// Only quit if canceled
	attempt := 0
	for ctx.Err() == nil {
		err := l.listenToSnapshots(ctx, firstSyncDone)
		if err != nil {
			l.logger.Error("listenToSnapshots failed", zap.Error(err))
			hub.CaptureException(fmt.Errorf("listenToSnapshots failed: %+v", err))
		} else {
			l.logger.Error("listenToSnapshots returned without error")
			hub.CaptureMessage("listenToSnapshots returned without error")
		}

		attempt += 1
		if attempt > 4 {
			// Not expecting this to happen a lot so we should ideally never get here,
			// but if firestore connections are rotten or something then a forceful
			// full service restart may be better
			err := fmt.Errorf("Snapshot listener has failed too many times, killing service")
			hub.CaptureException(err)
			l.DieNow(err)
		}

		l.logger.Warn("firestorelistener.retryForever sleeping before new snapshot listening attempt", zap.Int("attempt", attempt))
		time.Sleep(500 * time.Millisecond)
	}
}

// Note: If this panics we want it to blow up the service.
// Note: If this returns error we want it to be called again.
func (l *Listener) listenToSnapshots(ctx context.Context, firstSyncDone func()) error {
	l.logger.Info("firestorelistener.listenToSnapshots")

	collection := CollectionAppbutlers

	col := l.firestoreClient.Collection(collection)
	if col == nil {
		panic(fmt.Errorf("could not get collection %s", collection))
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
	const getMetrics = false

	if getMetrics {
		query = query.WithRunOptions(firestore.ExplainOptions{Analyze: getMetrics})
	}

	// Create a fresh blank infra projection to populate
	infra := NewInfraProjection()
	first := true
	totalErrorCount := 0

	// Until canceled
	for ctx.Err() == nil {
		snapIter := query.Snapshots(ctx)
		for ctx.Err() == nil {
			snap, err := snapIter.Next()
			if err != nil {
				return err
			}

			batchChangeCount, batchErrorCount := l.processSnapshot(ctx, infra, snap)
			totalErrorCount += batchErrorCount

			if DEBUGGING {
				l.logger.Info("Processed batch", zap.Int("changes", batchChangeCount), zap.Int("errors", batchErrorCount))
			}

			if err := l.CircuitBroken(); err != nil {
				return err
			}

			if first {
				first = false

				if getMetrics {
					// TODO: panic: Failed to get metrics firestore: ExplainMetrics are available only after the iterator reaches the end
					snapIter.Stop()
					l.dumpMetrics(snap)
					l.cancel()
					return nil
				}

				// Swap out the infra projection after the first snapshot has completed processing
				l.SetInfra(infra)

				// Notify the provisioner that we've done the first sync.
				firstSyncDone()
			} else {
				if SIMULATE_FAILURE {
					return fmt.Errorf("SIMULATING FAILURE")
				}
			}
		}
	}

	return nil
}

func (l *Listener) dumpMetrics(snap *firestore.QuerySnapshot) {
	metrics, err := snap.Documents.ExplainMetrics()
	if err != nil || metrics == nil || metrics.PlanSummary == nil {
		panic(fmt.Errorf("Failed to get metrics %+v", err))
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

func (l *Listener) CountLookupFailure() {
	l.failureCount.Add(1)
}

func (l *Listener) CircuitBroken() error {
	// Note about this number: multiple lookups may add to it, so I think it shouldn't be set too small
	const restartAfterFailureCount = 100
	if l.failureCount.Load() < restartAfterFailureCount {
		return nil
	}

	const minimumRestartWait = 10 * time.Second
	if time.Since(l.lastRestartTime) < minimumRestartWait {
		return nil
	}

	return fmt.Errorf("Too many errors, restarting snapshot listener")
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
		l.CountLookupFailure()
	}
	return service.Upstream
}

// LookupUsername does an optimized cache lookup to get the username for a project.
// The rest of this Listener code is basically written to support this fast lookup.
func (l *Listener) LookupUsername(projectID, serviceType string) string {
	service, ok := l.GetInfra().GetService(projectID, serviceType)
	if !ok {
		l.CountLookupFailure()
	}
	return service.Username
}

// LookupUpProjectIdFromDomain does an optimized cache lookup to get the projectId a custom domain is associated with.
// The rest of this Listener code is basically written to support this fast lookup.
func (l *Listener) LookupUpProjectIdFromDomain(customDomain string) string {
	domain, ok := l.GetInfra().GetDomain(customDomain)
	if !ok {
		l.CountLookupFailure()
	}
	return domain.ProjectId
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
