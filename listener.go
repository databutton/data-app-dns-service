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
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	collectionAppbutlers = "appbutlers"
)

// Partial Appbutler document to be parsed from firestore document
type AppbutlerDoc struct {
	UpdateTime        time.Time
	ProjectId         string `firestore:"projectId,omitempty"`
	ServiceType       string `firestore:"serviceType,omitempty"`
	RegionCode        string `firestore:"regionCode,omitempty"`
	CloudRunServiceId string `firestore:"cloudRunServiceId,omitempty"`
	ServiceIsReady    bool   `firestore:"serviceIsReady,omitempty"`
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
	upstreams       map[string]string
	upstreamsLock   sync.RWMutex
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
		// Create an empty map initially so it's never nil
		upstreams: make(map[string]string),
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
	upstreams := l.GetUpstreams()
	key := makeKey(serviceType, projectID)
	return upstreams[key]
}

// StoreToMap adds an entry to the upstreams map for the appbutler doc
func (l *Listener) StoreToMap(data AppbutlerDoc, upstreams map[string]string) {
	upstreams[makeKey(data.ServiceType, data.ProjectId)] = l.MakeUrl(data)
}

// Look up project id for a custom domain
func (l *Listener) GetProjectIdForCustomDomain(ctx context.Context, customBaseUrl string) (string, error) {
	// FIXME: Optimize this by adding a /domains listener, just getting proof of concept up and running

	doc := l.firestoreClient.Doc(fmt.Sprintf("/domains/%s", customBaseUrl))
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

// SkipDoc returns true if the document should be skipped over when building the upstreams map
func (l *Listener) SkipDoc(data AppbutlerDoc) bool {
	if data.ProjectId == "" {
		// Skip free pool appbutlers without assigned projects, there's nothing to route
		return true
	} else if !data.ServiceIsReady {
		// Skip before service is ready to receive requests
		return true
	}
	return false
}

// ValidateDoc return an error if some field we need is missing or invalid
func (l *Listener) ValidateDoc(data AppbutlerDoc) error {
	if data.ServiceType == "" {
		return fmt.Errorf("missing serviceType")
	} else if data.RegionCode == "" {
		return fmt.Errorf("missing regionCode")
	} else if data.CloudRunServiceId == "" {
		return fmt.Errorf("missing cloudRunServiceId")
	}
	return nil
}

// MakeUrl formats the upstreams url for an appbutler
func (l *Listener) MakeUrl(data AppbutlerDoc) string {
	return fmt.Sprintf("%s-%s-%s.a.run.app:443", data.CloudRunServiceId, GCP_PROJECT_HASH, data.RegionCode)
}

func (l *Listener) ProcessDoc(ctx context.Context, doc *firestore.DocumentSnapshot, upstreams map[string]string) error {
	var data AppbutlerDoc
	if err := doc.DataTo(&data); err != nil {
		return err
	}

	if l.SkipDoc(data) {
		return nil
	}

	if err := l.ValidateDoc(data); err != nil {
		return err
	}

	l.StoreToMap(data, upstreams)

	return nil
}

// Get current upstreams map, threadsafe.
func (l *Listener) GetUpstreams() map[string]string {
	l.upstreamsLock.RLock()
	defer l.upstreamsLock.RUnlock()
	return l.upstreams
}

func (l *Listener) RunUntilCanceled(ctx context.Context, initWg *sync.WaitGroup) error {
	log := l.logger

	hub := sentry.GetHubFromContext(ctx)

	collection := collectionAppbutlers
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
		} else if err != nil {
			// Once we get an error here the iterator won't recover
			log.Error("Snapshots.Next err", zap.Error(err))
			hub.CaptureException(err)
			return err
		}

		// In each iteration, build a new map, this needs no synchronization
		upstreams := make(map[string]string)

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
					hub.CaptureException(err)
				})
				log.Error("Documents.Next error", zap.Error(err))
				break
			}

			// Process a single document
			if err := l.ProcessDoc(ctx, doc, upstreams); err != nil {
				// Notify us of errors but keep going
				hub.WithScope(func(scope *sentry.Scope) {
					scope.SetTag("id", doc.Ref.ID)
					scope.SetTag("docsCount", strconv.Itoa(countDocs))
					scope.SetTag("docsFailed", strconv.Itoa(countErrors))
					hub.CaptureException(err)
				})
				log.Error("ProcessDoc error", zap.Error(err))
				countErrors += 1
			}
			countDocs++
		}

		// Swap in the new upstreams map
		func() {
			l.upstreamsLock.Lock()
			defer l.upstreamsLock.Unlock()
			l.upstreams = upstreams
		}()

		if countErrors > 0 {
			log.Error("Processed documents in snapshot with errors",
				zap.Int("docsCount", countDocs),
				zap.Int("docsFailed", countErrors),
			)
		} else {
			log.Info("Processed documents in snapshot",
				zap.Int("docsCount", countDocs),
				zap.Int("docsFailed", countErrors),
			)
		}

		// Pause a little bit here to avoid overloading
		// the service when getting rapid updates
		time.Sleep(100 * time.Millisecond)
	}
}

// This is a paranoia and metrics wrapper
func (l *Listener) RunWithoutCrashing(ctx context.Context, initWg *sync.WaitGroup) error {
	log := l.logger
	log.Info("Firestore listener starting")

	hub := sentry.GetHubFromContext(ctx).Clone()
	hub.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTag("startTime", time.Now().UTC().Format(time.RFC3339))
	})
	ctx = sentry.SetHubOnContext(ctx, hub)

	startTime := time.Now()
	err := DontPanic(func() error {
		return l.RunUntilCanceled(ctx, initWg)
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

// Count how many upstreams are in the cache
func (l *Listener) Count() int {
	return len(l.GetUpstreams())
}

// Debug dump of map
func (l *Listener) DebugDump() {
	upstreams := l.GetUpstreams()
	for key, value := range upstreams {
		l.logger.Debug(
			"DUMP",
			zap.String("key", key),
			zap.String("value", value),
		)
	}
	l.logger.Debug(
		"COUNT",
		zap.Int("count", len(upstreams)),
	)
}
