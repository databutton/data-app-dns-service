package dataappdnsservice

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/caddyserver/caddy/v2"
	"github.com/getsentry/sentry-go"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func DontPanic(f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	err = f()
	return
}

// Partial document to be parsed from firestore document
type ProjectDoc struct {
	Region string `firestore:"region"`

	// These won't exist before after the service is ready,
	// also we don't want to use them here because they're
	// within a document writable by the frontend / user.
	// DevxURL  string `firestore:"devxUrl"`
	// ProdxURL string `firestore:"prodxUrl"`
}

type CloudRunService struct {
	Name   string
	Region string
	Url    string
}

// Cached project document
type Project struct {
	ProjectDoc

	ProjectID string

	RegionCode string
	Devx       CloudRunService
	Prodx      CloudRunService
}

type ProjectListener struct {
	collection      string
	firestoreClient *firestore.Client
	projectMap      sync.Map
	upstreamMap     sync.Map
	wgDoneOnce      sync.Once
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
	logger          *zap.Logger
}

func NewProjectListener(collection string, logger *zap.Logger) (*ProjectListener, error) {
	if collection == "" {
		return nil, fmt.Errorf("collection name is empty")
	}

	client, err := firestore.NewClient(context.Background(), GCP_PROJECT)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	l := &ProjectListener{
		collection:      collection,
		ctx:             ctx,
		cancel:          cancel,
		firestoreClient: client,
		logger:          logger,
	}

	// This blocks WaitOnFirstSync() until wg.Done() is called
	l.wg.Add(1)

	return l, nil
}

// Destruct implements caddy.Destructor
func (l *ProjectListener) Destruct() error {
	// This should make the background goroutine exit
	l.cancel()

	l.logger.Info("Running project listener destructor")

	// Wait for graceful shutdown of goroutine
	time.Sleep(100 * time.Millisecond)

	// This will make the background goroutine start failing
	return l.firestoreClient.Close()
}

var _ caddy.Destructor = (*ProjectListener)(nil)

// Wait until first sync has happened
func (l *ProjectListener) WaitOnFirstSync(ctx context.Context) error {
	l.logger.Info("Waiting on first sync")
	l.wg.Wait()
	l.logger.Info("Done waiting on first sync")
	return nil
}

// Notify that first sync has happened
func (l *ProjectListener) notifyFirstSync() error {
	// wg.Done() panics if it's called more than once per wg.Add(1)
	// and this will be called repeatedly
	l.wgDoneOnce.Do(func() {
		l.wg.Done()
		l.logger.Info("Initial sync complete!")
	})
	return nil
}

func (l *ProjectListener) ProcessDoc(ctx context.Context, doc *firestore.DocumentSnapshot) error {
	projectID := doc.Ref.ID

	log := l.logger.With(zap.String("id", projectID))
	// log.Debug("Processing")

	// Parse document
	var projectData ProjectDoc
	err := doc.DataTo(&projectData)
	if err != nil {
		return err
	}
	if projectData.Region == "" {
		// Set fallback region if missing
		projectData.Region = "europe-west1"
		log.Debug("Could not find project region, assuming ew", zap.String("region", projectData.Region))
	}

	// Look up short region code and fail if unknown
	regionCode, ok := REGION_LOOKUP_MAP[projectData.Region]
	if !ok {
		log.Error("Could not find project region", zap.String("region", projectData.Region))
		sentry.CaptureMessage("Could not find project region")
		return errors.Wrapf(ErrInvalidRegion, "Region=%s", projectData.Region)
	}

	devxUrl := fmt.Sprintf("%s-%s-%s-%s.a.run.app:443", "devx", projectID, GCP_PROJECT_HASH, regionCode)
	prodxUrl := fmt.Sprintf("%s-%s-%s-%s.a.run.app:443", "prodx", projectID, GCP_PROJECT_HASH, regionCode)

	// We don't really need all this at the moment,
	// but I think we want to extend this for use in authorization
	project := Project{
		ProjectDoc: projectData,
		ProjectID:  projectID,
		RegionCode: regionCode,
		Devx: CloudRunService{
			Name:   "devx",
			Region: regionCode,
			Url:    devxUrl,
		},
		Prodx: CloudRunService{
			Name:   "prodx",
			Region: regionCode,
			Url:    prodxUrl,
		},
	}

	// Write to threadsafe map
	l.projectMap.Store(projectID, project)

	// Optimization for direct lookup
	l.upstreamMap.Store("devx"+projectID, devxUrl)
	l.upstreamMap.Store("prodx"+projectID, prodxUrl)

	// log.Debug("Done processing")

	return nil
}

// Re-entrant project cache lookup
func (l *ProjectListener) LookupProject(projectID string) (Project, bool) {
	value, ok := l.projectMap.Load(projectID)
	if !ok {
		return Project{}, false
	}
	project, ok := value.(Project)
	return project, ok
}

// Re-entrant optimized cache lookup for the upstream url only
func (l *ProjectListener) LookupUpUrl(projectID, serviceType string) string {
	value, ok := l.upstreamMap.Load(serviceType + projectID)
	if !ok {
		return ""
	}
	return value.(string)
}

// Count how many projects are in the cache
func (l *ProjectListener) Count() int {
	n := 0
	l.projectMap.Range(
		func(key, value any) bool {
			n += 1
			return true
		})
	return n
}

// Debug dump of map
func (l *ProjectListener) Dump() {
	n := 0
	l.upstreamMap.Range(
		func(key, value any) bool {
			n += 1
			l.logger.Debug(
				"DUMP",
				zap.String("key", key.(string)),
				zap.String("value", value.(string)),
			)
			return true
		})
	l.logger.Debug(
		"COUNT",
		zap.Int("count", n),
	)
}

// TODO: Update this to also delete projects (although deleted projects will be gone on a restart so not critical for a long time)
func (l *ProjectListener) RunUntilCanceled() error {
	ctx := l.ctx
	log := l.logger

	col := l.firestoreClient.Collection(l.collection)
	if col == nil {
		return fmt.Errorf("Could not get collection %s", l.collection)
	}

	initial := true

	log.Info("Starting query")
	it := col.Where("markedForDeletionAt", "==", nil).Snapshots(ctx)
	for {
		snap, err := it.Next()

		if status.Code(err) == codes.Canceled {
			log.Warn("Shutting down gracefully, I've been cancelled.", zap.Error(err))
			return nil
		}

		if err != nil {
			log.Error("Snapshots.Next err", zap.Error(err))
			sentry.CaptureException(err)
			// The way I understand it, once we get an error here the iterator won't recover
			return err
		}

		docCount := 0
		for {
			docCount++
			doc, err := snap.Documents.Next()

			if err == iterator.Done {
				// Notify the provisioner that we've done the first sync.
				if initial {
					l.notifyFirstSync()
					initial = false
				}

				// Once we get Done from Next we'll always get Done.
				log.Debug("Done processing snapshots")
				break
			}

			if err != nil {
				sentry.CaptureException(err)
				log.Error("Documents.Next error", zap.Error(err))
				break
			}

			// Process a single document
			if err := l.ProcessDoc(ctx, doc); err != nil {
				// Notify us but keep going if it fails
				sentry.WithScope(func(scope *sentry.Scope) {
					scope.SetLevel(sentry.LevelError)
					scope.SetTag("projectId", doc.Ref.ID)
					sentry.CaptureException(err)
				})

				log.Error("ProcessDoc error", zap.Error(err))
			}
		}
		log.Debug("Processed documents in snapshot", zap.Int("documents", docCount))
	}
}

func (l *ProjectListener) RunWithRestarts() error {
	// Cheap attempt at a few retries
	const maxErrors = 3
	const delayBetweenRetry = time.Millisecond * 200
	var lastError error
	for attempt := 0; attempt < maxErrors; attempt++ {
		if attempt > 0 {
			time.Sleep(100 * time.Millisecond)
		}

		log := l.logger.With(zap.Int("attempt", attempt))

		l.logger.Info("RunWithRestarts top of loop")

		startTime := time.Now()
		err := DontPanic(func() error {
			return l.RunUntilCanceled()
		})
		runTime := time.Since(startTime)

		log = log.With(zap.Duration("runTime", runTime))

		// Returns nil for graceful shutdown
		if err == nil {
			log.Info("RunWithRestarts graceful shutdown")
			return nil
		}

		// Returns error for other cases
		sentry.WithScope(func(scope *sentry.Scope) {
			scope.SetLevel(sentry.LevelError)
			scope.SetTag("attempt", strconv.Itoa(attempt))
			scope.SetTag("runTime", runTime.String())
			sentry.CaptureException(err)
		})
		log.Error("RunWithRestarts got error", zap.Error(err))
		lastError = err
	}
	return lastError
}
