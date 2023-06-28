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

const (
	collectionProjects   = "projects"
	collectionAppbutlers = "appbutlers"
)

// Partial Project document to be parsed from firestore document
type ProjectDoc struct {
	// Legacy projects will have the region here
	Region string `firestore:"region,omitempty"`

	// Legacy projects will have this set unless in broken state
	DevxUrl string `firestore:"devxUrl,omitempty"`

	// During a migration period, new projects will need to set
	// this on creation to use appbutlers for service creation
	EnableAppbutlers bool `firestore:"enableAppbutlers,omitempty"`
}

// Partial Appbutler document to be parsed from firestore document
type AppbutlerDoc struct {
	ProjectId         string `firestore:"projectId,omitempty"`
	ServiceIsReady    bool   `firestore:"serviceIsReady,omitempty"`
	ServiceType       string `firestore:"serviceType,omitempty"`
	RegionCode        string `firestore:"regionCode,omitempty"`
	CloudRunServiceId string `firestore:"cloudRunServiceId,omitempty"`
}

type ProjectListener struct {
	firestoreClient *firestore.Client

	ctx    context.Context
	cancel context.CancelFunc
	logger *zap.Logger

	upstreamMap sync.Map
}

func NewProjectListener(logger *zap.Logger) (*ProjectListener, error) {
	client, err := firestore.NewClient(context.Background(), GCP_PROJECT)
	if err != nil {
		return nil, err
	}

	// This use of context is a bit hacky, some refactoring can probably make the code cleaner
	ctx, cancel := context.WithCancel(context.Background())
	l := &ProjectListener{
		ctx:             ctx,
		cancel:          cancel,
		firestoreClient: client,
		logger:          logger,
	}

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

func (l *ProjectListener) ProcessDoc(ctx context.Context, doc *firestore.DocumentSnapshot) error {
	switch collection := doc.Ref.Parent.ID; collection {
	case collectionProjects:
		return l.ProcessProjectDoc(ctx, doc)
	case collectionAppbutlers:
		return l.ProcessAppbutlerDoc(ctx, doc)
	default:
		return fmt.Errorf("unexpected collection %s", collection)
	}
}

func (l *ProjectListener) ProcessAppbutlerDoc(ctx context.Context, doc *firestore.DocumentSnapshot) error {
	// Parse partial appbutler document
	var data AppbutlerDoc
	if err := doc.DataTo(&data); err != nil {
		return err
	}

	// Conditionally skip documents
	if data.ProjectId == "" {
		// Skip free pool appbutlers without assigned projects, there's nothing to route
		return nil
	} else if !data.ServiceIsReady {
		// Skip before service is ready to receive requests
		return nil
	}

	// Validate document
	if data.ServiceType == "" {
		return fmt.Errorf("missing serviceType")
	} else if data.RegionCode == "" {
		return fmt.Errorf("missing regionCode")
	} else if data.CloudRunServiceId == "" {
		return fmt.Errorf("missing cloudRunServiceId")
	}

	err := l.StoreUpstream(data.ServiceType, data.ProjectId, data.RegionCode, data.CloudRunServiceId)
	if err != nil {
		return err
	}
	return nil
}

func (l *ProjectListener) ProcessProjectDoc(ctx context.Context, doc *firestore.DocumentSnapshot) error {
	// Parse partial project document
	var data ProjectDoc
	if err := doc.DataTo(&data); err != nil {
		return err
	}
	projectID := doc.Ref.ID

	// If this project has or will have an associated appbutler, delegate to ProcessAppbutlerDoc.
	if data.EnableAppbutlers {
		// l.logger.Debug(
		// 	"Skipping project doc to use appbutler doc instead",
		// 	zap.String("projectId", doc.Ref.ID),
		// )
		return nil
	}

	if data.Region == "" {
		// I've migrated projects to always have region
		return nil
	}

	if data.DevxUrl == "" {
		// Only broken legacy projects that failed to create properly should have blank devxUrl,
		// and new projects should have enableAppbutlers and stop above
		return nil
	}

	// This should never happen now
	hub := sentry.GetHubFromContext(ctx)
	hub.WithScope(func(scope *sentry.Scope) {
		scope.SetTag("projectId", projectID)
		scope.SetTag("region", data.Region)
		scope.SetTag("devxUrl", data.DevxUrl)
		scope.SetTag("enableAppbutlers", fmt.Sprintf("%v", data.EnableAppbutlers))
		hub.CaptureMessage("ProcessProjectDoc is deprecated but still got here")
	})

	// Set fallback region if missing, for projects before we added multiregion
	region := data.Region
	if region == "" {
		region = "europe-west1"
	}

	// Look up short region code and fail if unknown
	regionCode, ok := REGION_LOOKUP_MAP[region]
	if !ok {
		l.logger.Error("Could not find project region", zap.String("region", region))
		err := fmt.Errorf("could not find project region %s", region)
		hub := sentry.GetHubFromContext(ctx).Clone()
		hub.CaptureException(err)
		return errors.Wrapf(ErrInvalidRegion, "Region=%s", region)
	}

	// Add both devx and prodx to mapping
	for _, serviceType := range []string{"devx", "prodx"} {
		serviceId := fmt.Sprintf("%s-%s", serviceType, projectID)
		err := l.StoreUpstream(serviceType, projectID, regionCode, serviceId)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *ProjectListener) StoreUpstream(serviceType, projectID, regionCode, serviceId string) error {
	// Write url to threadsafe map optimized for direct lookup of upstream url from (serviceType+projectID)
	// If there's already an entry, this will overwrite it.
	key := serviceType + projectID
	url := fmt.Sprintf("%s-%s-%s.a.run.app:443", serviceId, GCP_PROJECT_HASH, regionCode)
	l.upstreamMap.Store(key, url)

	// l.logger.Debug("Successfully stored upstream in map",
	// 	zap.String("serviceType", serviceType),
	// 	zap.String("projectId", projectID),
	// 	zap.String("region", regionCode),
	// 	zap.String("serviceId", serviceId),
	// )
	return nil
}

// Re-entrant optimized cache lookup for the upstream url only
func (l *ProjectListener) LookupUpUrl(projectID, serviceType string) string {
	value, ok := l.upstreamMap.Load(serviceType + projectID)
	if !ok {
		return ""
	}
	return value.(string)
}

// Count how many upstreams are in the cache
func (l *ProjectListener) Count() int {
	n := 0
	l.upstreamMap.Range(
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
func (l *ProjectListener) RunUntilCanceled(ctx context.Context, collection string, initWg *sync.WaitGroup) error {
	log := l.logger.With(zap.String("collection", collection))

	hub := sentry.GetHubFromContext(ctx)
	hub.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTag("collection", collection)
	})
	hub.CaptureMessage("Just checking that I'm getting the new release!")

	col := l.firestoreClient.Collection(collection)
	if col == nil {
		return fmt.Errorf("could not get collection %s", collection)
	}

	// TODO: To scale, iterate over collection ordered by update time
	//    and keep track of updatetime of last processed document?

	log.Info("Starting query")

	it := col.Snapshots(ctx)

	for {
		snap, err := it.Next()

		if status.Code(err) == codes.Canceled {
			log.Warn("Shutting down gracefully, I've been cancelled.", zap.Error(err))
			return nil
		} else if err != nil {
			// Once we get an error here the iterator won't recover
			log.Error("Snapshots.Next err", zap.Error(err))
			hub.CaptureException(err)
			return err
		}

		docCount := 0
		for {
			docCount++
			doc, err := snap.Documents.Next()

			if err == iterator.Done {
				// Notify the provisioner that we've done the first sync.
				if initWg != nil {
					initWg.Done()
					initWg = nil
				}
				break
			} else if err != nil {
				// Notify us and fail
				hub.WithScope(func(scope *sentry.Scope) {
					scope.SetTag("processedDocs", strconv.Itoa(docCount))
					hub.CaptureException(err)
				})
				log.Error("Documents.Next error", zap.Error(err), zap.String("collection", collection))
				break
			}

			// Process a single document
			if err := l.ProcessDoc(ctx, doc); err != nil {
				// Notify us of errors but keep going
				hub.WithScope(func(scope *sentry.Scope) {
					scope.SetTag("processedDocs", strconv.Itoa(docCount))
					scope.SetTag("id", doc.Ref.ID)
					hub.CaptureException(err)
				})
				log.Error("ProcessDoc error", zap.Error(err), zap.String("collection", collection))
			}
		}
		log.Debug("Processed documents in snapshot", zap.Int("documents", docCount))
	}
}

func (l *ProjectListener) RunWithoutCrashing(ctx context.Context, collection string, initWg *sync.WaitGroup) error {
	log := l.logger.With(zap.String("collection", collection))
	log.Info("Firestore listener starting")

	hub := sentry.GetHubFromContext(ctx).Clone()
	hub.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTag("collection", collection)
		scope.SetTag("startTime", time.Now().UTC().Format(time.RFC3339))
	})

	startTime := time.Now()
	err := DontPanic(func() error {
		return l.RunUntilCanceled(ctx, collection, initWg)
	})
	runTime := time.Since(startTime)

	log = log.With(zap.Duration("runTime", runTime))

	if err != nil {
		hub.WithScope(func(scope *sentry.Scope) {
			scope.SetLevel(sentry.LevelError)
			scope.SetTag("runTime", runTime.String())
			hub.CaptureException(err)
		})
		log.Error("Firestore listener returned error", zap.Error(err))
		return err
	}

	// Returns nil for graceful shutdown
	log.Info("Firestore listener graceful shutdown")
	return nil
}
