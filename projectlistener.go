package dataappdnsservice

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"cloud.google.com/go/firestore"
	"github.com/caddyserver/caddy/v2"
	"github.com/getsentry/sentry-go"
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

type CloudRunService struct {
	name   string
	region string
	url    string
}

// Partial document to be parsed from firestore document
type ProjectDoc struct {
	Region string `firestore:"region"`

	// DevxURL  string `firestore:"devxUrl"`
	// ProdxURL string `firestore:"prodxUrl"`
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
	collection string
	projectMap sync.Map
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewProjectListener(collection string) *ProjectListener {
	ctx, cancel := context.WithCancel(context.Background())
	pl := &ProjectListener{
		projectMap: sync.Map{},
		collection: collection,
		ctx:        ctx,
		cancel:     cancel,
	}
	pl.wg.Add(1)
	return pl
}

// Destruct implements caddy.Destructor
func (pl *ProjectListener) Destruct() error {
	// Thsi should shut down the background goroutine
	defer pl.cancel()
	return nil
}

var _ caddy.Destructor = (*ProjectListener)(nil)

// Wait until first sync has happened
func (l *ProjectListener) WaitOnFirstSync(ctx context.Context) error {
	l.wg.Wait()
	return nil
}

// Notify that first sync has happened
func (l *ProjectListener) NotifyFirstSync(ctx context.Context) error {
	// FIXME: This can't happen more than once, wrong primitive?
	l.wg.Done()
	return nil
}

func (l *ProjectListener) ProcessDoc(ctx context.Context, doc *firestore.DocumentSnapshot) error {
	// Parse document
	var projectData ProjectDoc
	err := doc.DataTo(&projectData)
	if err != nil {
		return err
	}

	// Look up short region code with fallback for migration
	regionCode, ok := REGION_LOOKUP_MAP[projectData.Region]
	if !ok {
		sentry.CaptureMessage("Could not find project in region")
		regionCode = "ew"
	}

	projectID := doc.Ref.ID

	devxUrl := fmt.Sprintf("%s-%s-%s-%s.a.run.app:443", "devx", projectID, GCP_PROJECT_HASH, regionCode)
	prodxUrl := fmt.Sprintf("%s-%s-%s-%s.a.run.app:443", "prodx", projectID, GCP_PROJECT_HASH, regionCode)

	project := Project{
		ProjectDoc: projectData,
		ProjectID:  projectID,
		RegionCode: regionCode,
		Devx: CloudRunService{
			name:   "devx",
			region: regionCode,
			url:    devxUrl,
		},
		Prodx: CloudRunService{
			name:   "prodx",
			region: regionCode,
			url:    prodxUrl,
		},
	}

	// Write to threadsafe map
	l.projectMap.Store(projectID, project)

	return nil
}

var ProjectNotFoundError = errors.New("Project not found")

// Re-entrant project cache lookup
func (l *ProjectListener) LookupProject(projectID string) (Project, bool) {
	value, ok := l.projectMap.Load(projectID)
	if !ok {
		return Project{}, false
	}
	project, ok := value.(Project)
	return project, ok
}

func (l *ProjectListener) RunUntilCanceled() error {
	ctx := l.ctx

	// TODO: Get zap logger that's not associated with module _instance_?

	client, err := firestore.NewClient(ctx, GCP_PROJECT)
	if err != nil {
		sentry.CaptureException(err)
		return err
	}
	defer client.Close()

	// TODO: Update this to also delete projects

	var initial = true
	it := client.Collection(l.collection).Where("markedForDeletionAt", "==", nil).Snapshots(ctx)
	for {
		snap, err := it.Next()
		if err != nil {
			if status.Code(err) == codes.Canceled {
				// l.logger.Info("Shutting down gracefully, I've been cancelled.")
				return nil
			}

			//l.logger.Error("Snapshots.Next err", zap.Error(err))
			sentry.CaptureException(err)

		} else if snap != nil {
			for {
				doc, err := snap.Documents.Next()

				if err == iterator.Done {
					if initial {
						// Notify the provisioner that we've done the first sync.
						l.NotifyFirstSync(ctx)
						initial = false
						// l.logger.Info("Initial sync complete!")
					}
					break
				}

				if err != nil {
					// l.logger.Error("Documents.Next err", zap.Error(err))
					sentry.CaptureException(err)
					break // TODO: Break or fail all the way?
				}

				if err := l.ProcessDoc(ctx, doc); err != nil {
					sentry.CaptureException(err)
					break // TODO: Break or fail all the way?
				}
			}
		} else {
			// Shouldn't happen?
		}
	}
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
