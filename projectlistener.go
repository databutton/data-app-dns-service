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
	collectionProjects = "projects"
	collectionFleets   = "fleets"
)

// Partial Project document to be parsed from firestore document
type ProjectDoc struct {
	// Legacy projects will have the region here
	Region string `firestore:"region,omitempty"`

	// New projects will have a fleetId
	FleetId string `firestore:"fleetId,omitempty"`
}

// Partial Fleet document to be parsed from firestore document
type FleetDoc struct {
	ProjectId string `firestore:"projectId"`
	Region    string `firestore:"region"`
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

// Cached fleet document
type Fleet struct {
	FleetDoc

	FleetID string

	RegionCode string
	Devx       CloudRunService
	Prodx      CloudRunService
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

func (l *ProjectListener) MakeServiceUrl(serviceType, fleetId, regionCode string) string {
	return fmt.Sprintf("%s-%s-%s-%s.a.run.app:443", serviceType, fleetId, GCP_PROJECT_HASH, regionCode)
}

func (l *ProjectListener) ProcessDoc(ctx context.Context, doc *firestore.DocumentSnapshot) error {
	switch collection := doc.Ref.Parent.ID; collection {
	case collectionProjects:
		return l.ProcessProjectDoc(ctx, doc)
	case collectionFleets:
		return l.ProcessFleetDoc(ctx, doc)
	default:
		err := fmt.Errorf("Unexpected collection %s", collection)
		sentry.CaptureException(err)
		return err
	}
}

func (l *ProjectListener) ProcessFleetDoc(ctx context.Context, doc *firestore.DocumentSnapshot) error {
	// Parse partial fleet document
	var data FleetDoc
	if err := doc.DataTo(&data); err != nil {
		return err
	}

	fleetID := doc.Ref.ID

	// Hacky workaround for temporary race condition:
	// Overwrite upstreams even though it's already there, such that we
	// overwrite a /projects version and this /fleets version wins eventually.
	overwrite := true

	return l.StoreUpstream(data.ProjectId, fleetID, data.Region, overwrite)
}

func (l *ProjectListener) ProcessProjectDoc(ctx context.Context, doc *firestore.DocumentSnapshot) error {
	// Parse partial project document
	var data ProjectDoc
	if err := doc.DataTo(&data); err != nil {
		return err
	}

	// If there's a fleet, let ProcessFleetDoc do the work
	// Note: Before we migrate everything to fleets, this is racy,
	// as the fleetId won't be set on initial project creation.
	// See the overwrite parameter for a quick and dirty workaround.
	// During the race period, we may route a project url request to
	// a cloud run service that doesn't exist, but only for a short period.
	if data.FleetId != "" {
		return nil
	}

	// For legacy projects without a fleet entry, the service name is
	// based on the projectId so we just define fleetId == projectId
	// (eventually we can migrate all projects to have a fleet entry using the same definition)
	projectID := doc.Ref.ID
	fleetID := projectID

	// Hacky workaround for temporary race condition:
	// Populate upstreams from /projects if it's not already there,
	// but avoid overwriting so we know the /fleets version wins eventually.
	overwrite := false

	return l.StoreUpstream(projectID, fleetID, data.Region, overwrite)
}

func (l *ProjectListener) StoreUpstream(projectID, fleetID, region string, overwrite bool) error {
	log := l.logger.With(
		zap.String("projectId", projectID),
		zap.String("fleetId", fleetID),
		zap.String("region", region),
	)

	// Set fallback region if missing, for projects before we added multiregion
	if region == "" {
		region = "europe-west1"
	}

	// Look up short region code and fail if unknown
	regionCode, ok := REGION_LOOKUP_MAP[region]
	if !ok {
		log.Error("Could not find project region", zap.String("region", region))
		sentry.CaptureMessage(fmt.Sprintf("Could not find project region %s", region))
		return errors.Wrapf(ErrInvalidRegion, "Region=%s", region)
	}

	// Write to threadsafe map optimized for direct lookup of upstream url from (serviceType+projectID)
	for _, serviceType := range []string{"devx", "prodx"} {
		key := serviceType + projectID
		url := l.MakeServiceUrl(serviceType, fleetID, regionCode)
		if overwrite {
			l.upstreamMap.Store(key, url)
		} else {
			_, _ = l.upstreamMap.LoadOrStore(key, url)
		}
	}

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
		return fmt.Errorf("Could not get collection %s", collection)
	}

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
