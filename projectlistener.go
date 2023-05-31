package dataappdnsservice

import (
	"context"
	"fmt"
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

	// During a migration period, new projects will need to set
	// this on creation to use appbutlers for service creation
	EnableAppbutlers bool `firestore:"enableAppbutlers,omitempty"`
}

// Partial Appbutler document to be parsed from firestore document
type AppbutlerDoc struct {
	ProjectId         string `firestore:"projectId,omitempty"`
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
		err := fmt.Errorf("unexpected collection %s", collection)
		sentry.CaptureException(err)
		return err
	}
}

func (l *ProjectListener) ProcessAppbutlerDoc(ctx context.Context, doc *firestore.DocumentSnapshot) error {
	appbutlerID := doc.Ref.ID

	log := l.logger.With(zap.String("appbutlerId", appbutlerID))

	log.Debug("Start of ProcessAppbutlerDoc")

	// Parse partial appbutler document
	var data AppbutlerDoc
	if err := doc.DataTo(&data); err != nil {
		log.Error("Error getting doc in ProcessAppbutlerDoc")
		return err
	}

	// Validate document
	if data.ProjectId == "" {
		err := fmt.Errorf("missing projectId")
		log.Error("Error in ProcessAppbutlerDoc", zap.Error(err))
		return err
	} else if data.ServiceType == "" {
		err := fmt.Errorf("missing serviceType")
		log.Error("Error in ProcessAppbutlerDoc", zap.Error(err))
		return err
	} else if data.RegionCode == "" {
		err := fmt.Errorf("missing regionCode")
		log.Error("Error in ProcessAppbutlerDoc", zap.Error(err))
		return err
	} else if data.CloudRunServiceId == "" {
		err := fmt.Errorf("missing cloudRunServiceId")
		log.Error("Error in ProcessAppbutlerDoc", zap.Error(err))
		return err
	}

	err := l.StoreUpstream(data.ServiceType, data.ProjectId, data.RegionCode, data.CloudRunServiceId)
	if err != nil {
		log.Error("Error in ProcessAppbutlerDoc.StoreUpstream", zap.Error(err))
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

	// If this project has or will have an associated appbutler, delegate to ProcessAppbutlerDoc.
	if data.EnableAppbutlers {
		l.logger.Debug(
			"Skipping project doc to use appbutler doc instead",
			zap.String("projectId", doc.Ref.ID),
		)
		return nil
	}

	projectID := doc.Ref.ID

	// Set fallback region if missing, for projects before we added multiregion
	region := data.Region
	if region == "" {
		region = "europe-west1"
	}

	// Look up short region code and fail if unknown
	regionCode, ok := REGION_LOOKUP_MAP[region]
	if !ok {
		l.logger.Error("Could not find project region", zap.String("region", region))
		sentry.CaptureMessage(fmt.Sprintf("Could not find project region %s", region))
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
	log := l.logger.With(
		zap.String("serviceType", serviceType),
		zap.String("projectId", projectID),
		zap.String("region", regionCode),
		zap.String("serviceId", serviceId),
	)

	// This should never happen here
	if regionCode == "" {
		log.Error("Missing region in StoreUpstream")
		sentry.CaptureMessage("Missing region in StoreUpstream")
		return errors.Wrapf(ErrInvalidRegion, "Region=%s", regionCode)
	}

	// Write to threadsafe map optimized for direct lookup of upstream url from (serviceType+projectID)
	key := serviceType + projectID
	url := fmt.Sprintf("%s-%s-%s.a.run.app:443", serviceId, GCP_PROJECT_HASH, regionCode)

	// Note: When we've migrated to appbutlers, we'll always have a region and can use this only once instead:
	// _, _ = l.upstreamMap.LoadOrStore(key, url)
	l.upstreamMap.Store(key, url)

	log.Info("Successfully stored upstream in map")
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
func (l *ProjectListener) RunUntilCanceled(collection string, initWg *sync.WaitGroup) error {
	ctx := l.ctx
	log := l.logger.With(zap.String("collection", collection))

	col := l.firestoreClient.Collection(collection)
	if col == nil {
		return fmt.Errorf("could not get collection %s", collection)
	}

	log.Info("Starting query")

	it := col.Snapshots(ctx)

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
				if initWg != nil {
					initWg.Done()
					initWg = nil
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
					scope.SetTag("id", doc.Ref.ID)
					sentry.CaptureException(err)
				})

				log.Error("ProcessDoc error", zap.Error(err))
			}
		}
		log.Debug("Processed documents in snapshot", zap.Int("documents", docCount))
	}
}

func (l *ProjectListener) RunWithRestarts(collection string, initWg *sync.WaitGroup) error {
	log := l.logger.With(zap.String("collection", collection))

	log.Info("Firestore listener starting")

	startTime := time.Now()
	err := DontPanic(func() error {
		return l.RunUntilCanceled(collection, initWg)
	})
	runTime := time.Since(startTime)

	log = log.With(zap.Duration("runTime", runTime))

	if err != nil {
		sentry.WithScope(func(scope *sentry.Scope) {
			scope.SetLevel(sentry.LevelError)
			scope.SetTag("collection", collection)
			scope.SetTag("runTime", runTime.String())
			sentry.CaptureException(err)
		})
		log.Error("Firestore listener returned error", zap.Error(err))
		return err
	}

	// Returns nil for graceful shutdown
	log.Info("Firestore listener graceful shutdown")
	return nil
}
