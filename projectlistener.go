package dataappdnsservice

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/caddyserver/caddy/v2"
	"github.com/getsentry/sentry-go"
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
	ctx, cancel := context.WithCancel(context.Background())

	client, err := firestore.NewClient(ctx, GCP_PROJECT)
	if err != nil {
		return nil, err
	}

	l := &ProjectListener{
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
	log.Info("Processing")

	// Parse document
	var projectData ProjectDoc
	err := doc.DataTo(&projectData)
	if err != nil {
		return err
	}

	// Look up short region code with fallback for migration
	regionCode, ok := REGION_LOOKUP_MAP[projectData.Region]
	if !ok {
		log.Info("Could not find project region", zap.String("region", projectData.Region))
		sentry.CaptureMessage("Could not find project region")
		regionCode = "ew"
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
	l.upstreamMap.Store("devx"+"-"+projectID, devxUrl)
	l.upstreamMap.Store("prodxx"+"-"+projectID, prodxUrl)

	log.Info("Done processing")

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

func (l *ProjectListener) RunUntilCanceled() error {
	ctx := l.ctx
	client := l.firestoreClient
	log := l.logger

	// TODO: Update this to also delete projects

	initial := true

	it := client.Collection(l.collection).Where("markedForDeletionAt", "==", nil).Snapshots(ctx)
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

		for {
			doc, err := snap.Documents.Next()

			if err == iterator.Done {
				// Notify the provisioner that we've done the first sync.
				if initial {
					log.Debug("First sync complete")
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
				// TODO: add "id":doc.Ref.Id in sentry scope
				sentry.CaptureException(err)
				log.Error("ProcessDoc error", zap.Error(err))
			}
		}
	}

	return nil
}

func (l *ProjectListener) RunWithRestarts() error {
	for {
		err := DontPanic(func() error { return l.RunUntilCanceled() })
		if err != nil {
			sentry.CaptureException(err)
			// FIXME: Do we really want to stay alive here?
			// return err
		}
		if err == nil {
			return nil
		}
	}
}
