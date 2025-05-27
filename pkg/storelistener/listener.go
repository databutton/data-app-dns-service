package storelistener

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/firestore"
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

	failureBucket         *atomic.Int64
	failedProjects        *sync.Map
	restartCount          *atomic.Int64
	lastCheckFailuresTime *atomic.Int64
	lastRestartTime       *atomic.Int64

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

		failureBucket:         new(atomic.Int64),
		failedProjects:        new(sync.Map),
		restartCount:          new(atomic.Int64),
		lastCheckFailuresTime: new(atomic.Int64),
		lastRestartTime:       new(atomic.Int64),

		infra: NewInfraProjection(),
	}

	l.lastCheckFailuresTime.Store(time.Now().UnixMilli())
	l.lastRestartTime.Store(time.Now().UnixMilli())

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
	hub.CaptureMessage("about to enter listenToSnapshots loop, logging sentry event to confirm it gets through") // OBSERVED
	defer func() {
		l.logger.Error("firestorelistener.retryForever exiting")             // NOT OBSERVED
		hub.CaptureMessage("listenToSnapshots returning, canceled or panic") // NOT OBSERVED
		sentry.Flush(2 * time.Second)
	}()

	// - If time since last restart is < 60 seconds, count restarts
	// - If restarts > 5, die to restart service
	restartCount := 0

	const usePolling = true // FIXME: Use this always after testing and clean up code

	// Loop until canceled
	for ctx.Err() == nil {
		l.lastRestartTime.Store(time.Now().UnixMilli())

		var err error
		if usePolling {
			err = l.listenByPolling(ctx, firstSyncDone)
		} else {
			err = l.listenToSnapshots(ctx, firstSyncDone)
		}

		if err != nil {
			l.logger.Error("listenToSnapshots returned error", zap.Error(err))         // NOT OBSERVED
			hub.CaptureException(errors.Wrap(err, "listenToSnapshots returned error")) // NOT OBSERVED
		} else {
			l.logger.Error("listenToSnapshots returned without error")     // NOT OBSERVED
			hub.CaptureMessage("listenToSnapshots returned without error") // NOT OBSERVED
		}

		// If the listener has worked for some time,
		// just reset and restart right away with no delay
		const acceptableRestartPeriod = 5 * time.Minute
		timeSince := time.Since(time.UnixMilli(l.lastRestartTime.Load()))
		if timeSince > acceptableRestartPeriod {
			restartCount = 0
			l.logger.Error("restarting listenToSnapshots immediately")     // NOT OBSERVED
			hub.CaptureMessage("restarting listenToSnapshots immediately") // NOT OBSERVED
			continue
		}

		// As long as we're looking at more rapid restarts, let the counter run
		restartCount++
		// and if we get a lot of them just die
		if restartCount > 10 {
			// Not expecting this to happen a lot so we should ideally never get here,
			// but if firestore connections are rotten or something then a forceful
			// full service restart may be better
			hub.CaptureException(ErrSnapshotListenerFailed) // NOT OBSERVED
			l.DieNow()
		}

		// Don't restart too quickly
		l.logger.Warn("firestorelistener.retryForever sleeping before next listenToSnapshots attempt", zap.Int("restartCount", restartCount))
		time.Sleep(time.Second)
	}
}

func (l *Listener) doSingleLookup(ctx context.Context, projectID, serviceType string) error {
	infra := l.GetInfra()
	if infra == nil {
		return errors.Errorf("infra is nil")
	}

	collectionName := CollectionAppbutlers
	collection := l.firestoreClient.Collection(collectionName)
	if collection == nil {
		return errors.Errorf("Collection %s not found", collectionName)
	}

	// There should be only one doc with this combination of projectId and serviceType
	query := collection.
		Select(AppbutlerDoc{}.Fields()...).
		Where("projectId", "==", projectID).
		Where("serviceType", "==", serviceType).
		Limit(1)

	docs, err := query.Documents(ctx).GetAll()
	if err != nil {
		return err
	}
	if len(docs) != 1 {
		return errors.Errorf("Expected 1 doc, got %d", len(docs))
	}

	doc := docs[0]
	if err := l.infra.UpsertPolled(doc); err != nil {
		// Shouldn't happen, we need to look at the doc here
		hub := sentry.GetHubFromContext(ctx)
		hub.WithScope(func(scope *sentry.Scope) {
			scope.SetTag("ref", doc.Ref.Path)
			scope.SetLevel(sentry.LevelError)
			hub.CaptureException(err)
		})
		l.logger.Error(
			"firestorelistener.singleLookup upsert error",
			zap.Error(err),
			zap.String("ref", doc.Ref.Path),
		)
	}

	return nil
}

func (l *Listener) listenByPolling(ctx context.Context, firstSyncDone func()) error {
	l.logger.Info("firestorelistener.listenByPolling")

	collectionName := CollectionAppbutlers
	collection := l.firestoreClient.Collection(collectionName)
	if collection == nil {
		panic(errors.Errorf("Collection %s not found", collectionName))
	}

	// Page tracking
	const pageSize = 2000
	const nextSleepDuration = 1 * time.Second
	type cursorType struct {
		ts time.Time
		id string
	}
	var cursor cursorType

	// Create a fresh blank infra projection to populate
	infra := NewInfraProjection()
	first := true

	// FIXME: run health check goroutine or just skip it and simplify?

	// Poll until canceled
	for ctx.Err() == nil {
		query := collection.
			Select(AppbutlerDoc{}.Fields()...).
			OrderBy(AppbutlerCursorTimestampField, firestore.Asc).
			OrderBy(firestore.DocumentID, firestore.Asc)
		if cursor != (cursorType{}) {
			query = query.StartAfter(cursor.ts, cursor.id)
		}
		docs, err := query.
			Limit(pageSize).
			Documents(ctx).
			GetAll()
		if err != nil {
			l.logger.Error("firestorelistener.listenByPolling documents getall error", zap.Error(err))
			return err
		}

		if SIMULATE_FAILURE {
			return ErrSimulatedFailure
		}

		if len(docs) == 0 {
			if first {
				l.logger.Info("firestorelistener.listenByPolling all docs fetched once")
				first = false

				// Swap out the infra projection after the first snapshot has completed processing
				l.SetInfra(infra)

				firstSyncDone()
			}

			l.logger.Info("firestorelistener.listenByPolling no new docs found")
			time.Sleep(nextSleepDuration)
			continue
		}

		// Process docs and update infra
		for _, doc := range docs {
			err := infra.UpsertPolled(doc)
			if err != nil {
				// Shouldn't happen, we need to look at the doc here
				hub := sentry.GetHubFromContext(ctx)
				hub.WithScope(func(scope *sentry.Scope) {
					scope.SetTag("ref", doc.Ref.Path)
					scope.SetLevel(sentry.LevelError)
					hub.CaptureException(err)
				})
				l.logger.Error(
					"firestorelistener.listenByPolling upsert error",
					zap.Error(err),
					zap.String("ref", doc.Ref.Path),
				)
			}
		}

		// Get cursor from last doc
		last := docs[len(docs)-1]
		type AppbutlerCursorDoc struct {
			RoutingChangedAt time.Time `firestore:"routingChangedAt"`
		}
		var appbutlerCursorDoc AppbutlerCursorDoc
		if err := last.DataTo(&appbutlerCursorDoc); err != nil {
			// Shouldn't happen, we need to look at the doc here
			hub := sentry.GetHubFromContext(ctx)
			hub.WithScope(func(scope *sentry.Scope) {
				scope.SetTag("ref", last.Ref.Path)
				scope.SetLevel(sentry.LevelError)
				hub.CaptureException(err)
			})
			l.logger.Error(
				"firestorelistener.listenByPolling failed to read cursor from last doc",
				zap.Error(err),
				zap.String("ref", last.Ref.Path),
			)
			// Can't continue here
			return err
		}
		if appbutlerCursorDoc.RoutingChangedAt.IsZero() {
			return errors.Errorf("RoutingChangedAt is zero, this should never happen")
		}

		// Store cursor for next poll batch
		cursor = cursorType{appbutlerCursorDoc.RoutingChangedAt, last.Ref.ID}

		l.logger.Info(
			"firestorelistener.listenByPolling up to date until cursor",
			zap.String("lastRef", last.Ref.Path),
			zap.Time("cursorTs", cursor.ts),
			zap.String("cursorId", cursor.id),
		)
	}

	return ctx.Err()
}

// Note: If this panics we want it to blow up the service.
// Note: If this returns error we want it to be called again.
func (l *Listener) listenToSnapshots(ctx context.Context, firstSyncDone func()) error {
	l.logger.Info("firestorelistener.listenToSnapshots") // OBSERVED

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
			logArgs := []zap.Field{
				zap.Time("lastRestartTime", time.UnixMilli(l.lastRestartTime.Load())),
				zap.Int64("restartCount", l.restartCount.Load()),
				zap.Int64("failureBucket", l.failureBucket.Load()),
				zap.Int64("totalChanges", totalChangeCount.Load()),
				zap.Int64("totalErrors", totalErrorCount.Load()),
			}
			if err != nil {
				logArgs = append(logArgs, zap.Error(err))
				l.logger.Error("firestorelistener.listenToSnapshots listener health check FAILED", logArgs...) // NOT OBSERVED
				cancel()
				return
			}
			l.logger.Info("firestorelistener.listenToSnapshots listener health check OK", logArgs...) // OBSERVED
			time.Sleep(10 * time.Second)
		}
	}()

	// Iterate over snapshots until canceled
	snapIter := query.Snapshots(ctx)
	for ctx.Err() == nil {
		snap, err := snapIter.Next()
		if err != nil {
			l.logger.Error("firestorelistener.listenToSnapshots snapshot iterator error", zap.Error(err)) // NOT OBSERVED
			return err
		}

		batchChangeCount, batchErrorCount := l.processSnapshot(ctx, infra, snap)
		totalChangeCount.Add(int64(batchChangeCount))
		totalErrorCount.Add(int64(batchErrorCount))

		// Nice to have in logs while the system is new
		// if DEBUGGING {
		l.logger.Info("firestorelistener.listenToSnapshots processed batch", zap.Int("changes", batchChangeCount), zap.Int("errors", batchErrorCount)) // OBSERVED
		//}

		if first {
			first = false

			// Swap out the infra projection after the first snapshot has completed processing
			l.SetInfra(infra)

			// Notify the provisioner that we've done the first sync.
			firstSyncDone()
		} else if SIMULATE_FAILURE {
			return ErrSimulatedFailure
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
			err = infra.UpsertForSnapshotListener(change.Doc)
		case firestore.DocumentRemoved:
			err = infra.RemoveForSnapshotListener(change.Doc)
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

// UpstreamForProject does an optimized cache lookup to get the upstream url.
// The rest of this Listener code is basically written to support this fast lookup.
func (l *Listener) UpstreamForProject(ctx context.Context, projectID, serviceType string) string {
	service, ok := l.GetInfra().GetService(projectID, serviceType)
	if ok && service.Upstream != "" {
		return service.Upstream
	}

	// Try a single lookup to see if it's just a temporary failure
	// TODO: Rate limit these lookups and perhaps blacklist projects that don't get anywhere.
	time.Sleep(time.Second)
	if err := l.doSingleLookup(ctx, projectID, serviceType); err == nil {
		service, ok := l.GetInfra().GetService(projectID, serviceType)
		if ok && service.Upstream != "" {
			return service.Upstream
		}
	}

	l.registerFailure(ctx, projectID, serviceType)
	return ""
}

// UsernameForProject does an optimized cache lookup to get the username for a project.
// The rest of this Listener code is basically written to support this fast lookup.
func (l *Listener) UsernameForProject(projectID, serviceType string) string {
	service, _ := l.GetInfra().GetService(projectID, serviceType)
	return service.Username
}

// ProjectIdForDomain does an optimized cache lookup to get the projectId a custom domain is associated with.
// The rest of this Listener code is basically written to support this fast lookup.
func (l *Listener) ProjectIdForDomain(originHost string) string {
	infra := l.GetInfra()

	domain, ok := infra.GetDomain(originHost)
	if ok {
		return domain.ProjectId
	}

	if strings.HasPrefix(originHost, "www.") {
		domain, ok = infra.GetDomain(strings.TrimPrefix(originHost, "www."))
		if ok {
			return domain.ProjectId
		}
	}

	return ""
}

// Parameters for leaky bucket failure count tuning
// fails if errors per second is higher than bucketLeakPerSecond
// long enough that the bucket flows over the maxFailureBudget limit
const (
	maxFailureBudget           = 5
	bucketLeakPerSecond        = 0.1
	minimumTimeBetweenRestarts = 3 * time.Minute
)

func (l *Listener) resetFailures() {
	l.failureBucket.Store(0)
	l.failedProjects = new(sync.Map) // TODO: Wrap in mutex?
	l.lastRestartTime.Store(time.Now().UnixMilli())
}

// NB! This is called from upstreams module threads!
func (l *Listener) registerFailure(ctx context.Context, projectID string, serviceType string) {
	// Don't count the same serviceType+projectID, sometimes there's a single project that just keeps failing
	// e.g. because something external tries to hit it but the project has been hibernated or deleted
	if _, loaded := l.failedProjects.LoadOrStore(serviceType+projectID, true); !loaded {
		l.failureBucket.Add(1)

		// Log failure (happens only once per projectID)
		hub := sentry.GetHubFromContext(ctx)
		l.logger.Warn("Upstream lookup failure registered", zap.String("projectID", projectID), zap.String("serviceType", serviceType), zap.Bool("alsoInSentry", hub != nil))
		if hub != nil {
			hub.WithScope(func(scope *sentry.Scope) {
				scope.SetTag("projectID", projectID)
				scope.SetLevel(sentry.LevelWarning)
				hub.CaptureMessage("Upstream lookup failure registered")
			})
		}
	}
}

func (l *Listener) checkFailures() error {
	sinceLastCheck := time.Since(time.UnixMilli(l.lastCheckFailuresTime.Load()))
	l.lastCheckFailuresTime.Store(time.Now().UnixMilli())

	// Add some failure budget over time, this way we'll always tend to go back to the max,
	// and need sustained errors over some time to actually call it failure
	bucketLeak := int64(sinceLastCheck.Seconds() * bucketLeakPerSecond)

	// Subtract leak but clamp to zero
	bucket := l.failureBucket.Load()
	bucket -= bucketLeak
	if bucket < 0 {
		bucket = 0
	}
	// Store the new budget, ignore if it has been touched between, this is not that accurate anyway
	l.failureBucket.Store(bucket)

	// Don't restart too rapidly
	if time.Since(time.UnixMilli(l.lastRestartTime.Load())) < minimumTimeBetweenRestarts {
		return nil
	}

	// Break circuit if we're out of failure budget
	if bucket > maxFailureBudget {
		return errors.Wrapf(ErrCircuitBroken, "Exceeded failure budget (%d > %d)", bucket, maxFailureBudget)
	}
	return nil
}

func (l *Listener) CountUpstreams() int {
	return l.GetInfra().CountServices()
}

func (l *Listener) CountDomains() int {
	return l.GetInfra().CountDomains()
}

// Interface guards
var (
	_ caddy.Destructor = (*Listener)(nil)
)
