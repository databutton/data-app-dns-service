package dataappdnsservice

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/caddyserver/caddy/v2"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/databutton/data-app-dns-service/pkg/dontpanic"
	"github.com/databutton/data-app-dns-service/pkg/safemap"
)

const (
	collectionAppbutlers = "appbutlers"
	collectionDomains    = "domains"
)

// Partial Appbutler document to be parsed from firestore document
type AppbutlerDoc struct {
	UpdateTime        time.Time
	ProjectId         string `firestore:"projectId,omitempty"`
	ServiceType       string `firestore:"serviceType,omitempty"`
	RegionCode        string `firestore:"regionCode,omitempty"`
	CloudRunServiceId string `firestore:"cloudRunServiceId,omitempty"`
	ServiceIsReady    bool   `firestore:"serviceIsReady,omitempty"`
	OverrideURL       string `firestore:"overrideURL,omitempty"`
}

// Partial Domain document to be parsed from firestore document
type DomainDoc struct {
	ProjectId string `firestore:"projectId"`
}

type Listener struct {
	ctx             context.Context
	cancel          context.CancelFunc
	firestoreClient *firestore.Client
	logger          *zap.Logger
	upstreams       *safemap.SafeStringMap
	domainProjects  *safemap.SafeStringMap
}

func NewProjectListener(logger *zap.Logger) (*Listener, error) {
	client, err := firestore.NewClient(context.Background(), GCP_PROJECT)
	if err != nil {
		return nil, err
	}

	// This use of context is a bit hacky, some refactoring can probably make the code cleaner
	ctx, cancel := context.WithCancel(context.Background())
	l := &Listener{
		ctx:             ctx,
		cancel:          cancel,
		firestoreClient: client,
		logger:          logger,
		upstreams:       safemap.NewSafeStringMap(),
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

// Make key for upstreams map lookup, just to have it defined one place
func makeKey(serviceType, projectId string) string {
	return serviceType + projectId
}

// LookupUpUrl does an optimized cache lookup to get the upstream url.
// The rest of this Listener code is basically written to support this fast lookup.
func (l *Listener) LookupUpUrl(projectID, serviceType string) string {
	upstreams := l.upstreams.GetMap()
	key := makeKey(serviceType, projectID)
	url := upstreams[key]
	return url
}

// LookupUpProjectIdFromDomain does an optimized cache lookup to get the upstream url.
// The rest of this Listener code is basically written to support this fast lookup.
func (l *Listener) LookupUpProjectIdFromDomain(customBaseUrl string) string {
	domainProjects := l.domainProjects.GetMap()
	projectId := domainProjects[customBaseUrl]
	return projectId
}

// Look up project id for a custom domain
func (l *Listener) GetProjectIdForCustomDomain(ctx context.Context, customBaseUrl string) (string, error) {
	pathRef := strings.Join([]string{collectionDomains, customBaseUrl}, "/")

	doc := l.firestoreClient.Doc(pathRef)
	if doc == nil {
		return "", fmt.Errorf("invalid doc reference %s", pathRef)
	}
	ref, err := doc.Get(ctx)
	if err != nil {
		return "", err
	}

	var domain DomainDoc
	err = ref.DataTo(&domain)
	if err != nil {
		return "", err
	}

	return domain.ProjectId, nil
}

func (l *Listener) ShouldSkipAppbutler(data AppbutlerDoc) bool {
	if data.ProjectId == "" {
		// Skip free pool appbutlers without assigned projects, there's nothing to route
		return true
	} else if !data.ServiceIsReady {
		// Skip before service is ready to receive requests
		return true
	}
	return false
}

func (l *Listener) ProcessAppbutlerDoc(ctx context.Context, doc *firestore.DocumentSnapshot, upstreams map[string]string) error {
	var data AppbutlerDoc
	if err := doc.DataTo(&data); err != nil {
		return err
	}

	if l.ShouldSkipAppbutler(data) {
		return nil
	}

	if data.ProjectId == "" {
		return fmt.Errorf("missing projectId")
	} else if data.ServiceType == "" {
		return fmt.Errorf("missing serviceType")
	} else if data.RegionCode == "" {
		return fmt.Errorf("missing regionCode")
	} else if data.CloudRunServiceId == "" && data.OverrideURL == "" { // Updated this line to check for OverrideURL as well
		return fmt.Errorf("missing cloudRunServiceId")
	}

	key := makeKey(data.ServiceType, data.ProjectId)

	// Check if overrideURL is provided and non-empty
	if data.OverrideURL != "" {
		// Use the overrideURL instead of generating one
		upstreams[key] = data.OverrideURL
	} else {
		// Original URL generation logic
		url := fmt.Sprintf("%s-%s-%s.a.run.app:443", data.CloudRunServiceId, GCP_PROJECT_HASH, data.RegionCode)
		upstreams[key] = url
	}

	return nil
}

func (l *Listener) ProcessDomainDoc(ctx context.Context, doc *firestore.DocumentSnapshot, projection map[string]string) error {
	var data DomainDoc
	if err := doc.DataTo(&data); err != nil {
		return err
	}

	if data.ProjectId == "" {
		// Treating missing projectId as "do not route"
		return nil
	}

	customBaseUrl := doc.Ref.ID
	projection[customBaseUrl] = data.ProjectId

	return nil
}

func (l *Listener) RunListener(ctx context.Context, initWg *sync.WaitGroup, collection string) error {
	// Maybe there's an abstraction waiting to be created here
	var processDoc func(ctx context.Context, doc *firestore.DocumentSnapshot, upstreams map[string]string) error
	var stringMap *safemap.SafeStringMap
	switch collection {
	case collectionAppbutlers:
		processDoc = l.ProcessAppbutlerDoc
		stringMap = l.upstreams
	case collectionDomains:
		processDoc = l.ProcessDomainDoc
		stringMap = l.domainProjects
	default:
		return fmt.Errorf("invalid collection %s", collection)
	}

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
		projection := make(map[string]string)

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
			if err := processDoc(ctx, doc, projection); err != nil {
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
		stringMap.SetMap(projection)

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

// This is a paranoia and metrics wrapper
func (l *Listener) RunWithoutCrashing(ctx context.Context, initWg *sync.WaitGroup, collection string) error {
	log := l.logger.With(zap.String("collection", collection))

	hub := sentry.GetHubFromContext(ctx)
	hub.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTag("startTime", time.Now().UTC().Format(time.RFC3339))
	})

	startTime := time.Now()
	err := dontpanic.DontPanic(func() error {
		return l.RunListener(ctx, initWg, collection)
	})
	runTime := time.Since(startTime)

	log = log.With(zap.Duration("runTime", runTime))

	if err != nil {
		hub.WithScope(func(scope *sentry.Scope) {
			scope.SetLevel(sentry.LevelError)
			scope.SetTag("runTime", runTime.String())
			hub.CaptureException(err)
		})
		log.Error("Listener failed", zap.Error(err))
		return err
	}

	// Returns nil for graceful shutdown
	log.Info("Listener shutting down")
	return nil
}
