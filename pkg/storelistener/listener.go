package storelistener

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/caddyserver/caddy/v2"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/databutton/data-app-dns-service/pkg/safemap"
)

// Make key for upstreams map lookup, just to have it defined one place
func makeKey(serviceType, projectId string) string {
	return serviceType + projectId
}

type Projection interface {
	Collection() string
	Update(context.Context, *firestore.DocumentSnapshot) error
	Swap(*Listener)
}

type Listener struct {
	Ctx             context.Context
	cancel          context.CancelFunc
	firestoreClient *firestore.Client
	logger          *zap.Logger
	upstreams       *safemap.SafeStringMap
	usernames       *safemap.SafeStringMap
	domainProjects  *safemap.SafeStringMap
}

func NewFirestoreListener(cancel context.CancelFunc, logger *zap.Logger) (*Listener, error) {
	client, err := firestore.NewClient(context.Background(), GCP_PROJECT)
	if err != nil {
		return nil, err
	}

	// This use of context is a bit hacky, some refactoring can probably make the code cleaner
	ctx, cancel := context.WithCancel(context.Background())
	l := &Listener{
		Ctx:             ctx,
		cancel:          cancel,
		firestoreClient: client,
		logger:          logger,
		upstreams:       safemap.NewSafeStringMap(),
		usernames:       safemap.NewSafeStringMap(),
		domainProjects:  safemap.NewSafeStringMap(),
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

// LookupUpstreamHost does an optimized cache lookup to get the upstream url.
// The rest of this Listener code is basically written to support this fast lookup.
func (l *Listener) LookupUpstreamHost(projectID, serviceType string) string {
	upstreams := l.upstreams.GetMap()
	key := makeKey(serviceType, projectID)
	url := upstreams[key]
	return url
}

// LookupUsername does an optimized cache lookup to get the username for a project.
// The rest of this Listener code is basically written to support this fast lookup.
func (l *Listener) LookupUsername(projectID, serviceType string) string {
	usernames := l.usernames.GetMap()
	key := makeKey(serviceType, projectID)
	url := usernames[key]
	return url
}

// LookupUpProjectIdFromDomain does an optimized cache lookup to get the upstream url.
// The rest of this Listener code is basically written to support this fast lookup.
func (l *Listener) LookupUpProjectIdFromDomain(customBaseUrl string) string {
	domainProjects := l.domainProjects.GetMap()
	projectId := domainProjects[customBaseUrl]
	return projectId
}

func (l *Listener) RunListener(ctx context.Context, initWg *sync.WaitGroup, newProjection func() Projection) error {
	collection := newProjection().Collection()

	hub := sentry.GetHubFromContext(ctx)
	log := l.logger.With(zap.String("collection", collection))

	log.Info("Starting listener")

	col := l.firestoreClient.Collection(collection)
	if col == nil {
		return fmt.Errorf("could not get collection %s", collection)
	}

	it := col.Snapshots(ctx)
	for {
		snap, err := it.Next()

		if status.Code(err) == codes.Canceled {
			log.Info("Listener has been cancelled")
			return nil
		} else if err != nil {
			// Once we get an error here the iterator won't recover
			log.Error("Listener failed to get next snapshot", zap.Error(err))
			hub.CaptureException(err)
			return err
		}

		// In each iteration, build a new map, this needs no synchronization
		projection := newProjection()

		countDocs := 0
		countErrors := 0

		for {
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
					scope.SetTag("processedDocs", strconv.Itoa(countDocs))
					scope.SetTag("docsCount", strconv.Itoa(countDocs))
					scope.SetTag("docsFailed", strconv.Itoa(countErrors))
					hub.CaptureException(err)
				})
				log.Error("Listener failed to get next document", zap.Error(err))
				break
			}

			// Process a single document
			if err := projection.Update(ctx, doc); err != nil {
				// Notify us of errors but keep going
				hub.WithScope(func(scope *sentry.Scope) {
					scope.SetTag("id", doc.Ref.ID)
					scope.SetTag("docsCount", strconv.Itoa(countDocs))
					scope.SetTag("docsFailed", strconv.Itoa(countErrors))
					hub.CaptureException(err)
				})
				log.Error("Listener failed to process a single document, continuing", zap.Error(err))
				countErrors += 1
			}
			countDocs++
		}

		// Swap in the new projection
		projection.Swap(l)

		if countErrors > 0 {
			log.Error("Listener processed snapshot with errors",
				zap.Int("docsCount", countDocs),
				zap.Int("docsFailed", countErrors),
			)
		} else {
			log.Info("Listener processed snapshot successfully",
				zap.Int("docsCount", countDocs),
				zap.Int("docsFailed", countErrors),
			)
		}

		// Pause a little bit here to avoid overloading
		// the service when getting rapid updates
		time.Sleep(100 * time.Millisecond)
	}
}

func (l *Listener) CountUpstreams() int {
	return len(l.upstreams.GetMap())
}

func (l *Listener) CountDomains() int {
	return len(l.domainProjects.GetMap())
}

// Interface guards
var (
	_ caddy.Destructor = (*Listener)(nil)
)
