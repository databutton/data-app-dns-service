package dataappdnsservice

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"sync"

	"cloud.google.com/go/firestore"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"

	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var usagePool = caddy.NewUsagePool()

type SentryDestructor struct {
}

func (SentryDestructor) Destruct() error {
	// Flush buffered events before the program terminates.
	sentry.Flush(2 * time.Second)
	return nil
}

var _ caddy.Destructor = SentryDestructor{}

func initSentry() error {
	_, _, err := usagePool.LoadOrNew("sentryInit", func() (caddy.Destructor, error) {
		// Set up sentry
		err := sentry.Init(sentry.ClientOptions{
			Dsn: "https://aceadcbf56f14ef9a7fe76b0db5d7351@o1000232.ingest.sentry.io/4504735637176320",
			// Set TracesSampleRate to 1.0 to capture 100%
			// of transactions for performance monitoring.
			// We recommend adjusting this value in production,
			TracesSampleRate: 1.0,
		})
		if err != nil {
			return nil, err
		}
		return SentryDestructor{}, nil
	})
	return err
}

// TODO: Make these into config
const GCP_PROJECT = "databutton"
const GCP_PROJECT_HASH = "gvjcjtpafa"

var REGION_LOOKUP_MAP = map[string]string{
	"europe-west1":      "ew",
	"europe-north1":     "lz",
	"europe-southwest1": "no",
	"europe-west9":      "od",
	"europe-west4":      "ez",
	"europe-west8":      "oc",
}

type CloudRunService struct {
	name   string
	region string
	url    string
}

type Project struct {
	ProjectID string          `firestore:"projectId"`
	Region    string          `firestore:"region"`
	devx      CloudRunService `firestore:"devx"`
	prodx     CloudRunService `firestore:"prodx"`
	DevxURL   string          `firestore:"devxUrl"`
}

type FirestoreListener struct {
	client     *firestore.Client
	projectMap map[string]Project
}

func NewFirestoreListener(client *firestore.Client) *FirestoreListener {
	return &FirestoreListener{
		client:     client,
		projectMap: make(map[string]Project),
	}
}

// TODO: Make this a separate listener type reusable across modules, with lifetime longer than a provisioned module
func (fl *FirestoreListener) listenMultiple(ctx context.Context, collection string, wg *sync.WaitGroup, initial bool) error {

	// TODO: Update this to also delete projects from map when they are marked for deletion
	it := fl.client.Collection(collection).Where("markedForDeletionAt", "==", nil).Snapshots(ctx)

	for {
		snap, err := it.Next()

		if err != nil {
			if status.Code(err) == codes.Canceled {
				fl.logger.Info("Shutting down gracefully, I've been cancelled.")
				return nil
			}
			fl.logger.Error("Snapshots.Next err", zap.Error(err))
			sentry.CaptureException(err)
			// MSA: continue? Look up firestore collection iterator semantics in go.
		}

		for {
			doc, err := snap.Documents.Next()
			if err == iterator.Done {
				if initial {
					// Notify the provisioner that we've done the first sync.
					wg.Done()
					fl.logger.Info("Initial sync complete!")
					initial = false
				}
				break
			}

			if err != nil {
				fl.logger.Error("Documents.Next err", zap.Error(err))
				sentry.CaptureException(err)
			}

			var projectData Project
			doc.DataTo(&projectData)
			projectData.ProjectID = doc.Ref.ID
			fl.projectMap[projectData.ProjectID] = projectData
		}
	}
}

func initFirestore() (*firestore.Client, error) {
	// FIXME: Call LoadOrNew from Provision, and Delete from Cleanup
	value, _, err := usagePool.LoadOrNew("firestoreClient", func() (caddy.Destructor, error) {
		client, err := firestore.NewClient(context.Background(), GCP_PROJECT)
		if err != nil {
			return nil, err
		}
		return FirestoreDestructor{client: client}, nil
	})
	if err != nil {
		return nil, err
	}
	return value.client, err
}

func (fl *FirestoreListener) LookupProject(projectId string) (Project, error) {
	// TODO: Make sure this results in 404
	return "", errors.New("Could not find project")
}

type DevxUpstreams struct {
	// Global firestore listener thing
	fl *FirestoreListener

	// Logger in caddy module context
	logger *zap.Logger

	// keys to delete from global usage pool on closing
	usagePoolKeys []string
}

// CaddyModule returns the Caddy module information.
func (DevxUpstreams) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "http.reverse_proxy.upstreams.devx",
		New: func() caddy.Module {
			return new(DevxUpstreams)
		},
	}
}

func init() {
	caddy.RegisterModule(DevxUpstreams{})
}

func (du *DevxUpstreams) Provision(ctx caddy.Context) error {

	// Global initialization that will happen only once, no-op next times
	err := initSentry()
	if err != nil {
		return err
	}
	du.usagePoolKeys

	// FIXME: Refactor this block to cleanup properly
	{
		client, err := initFirestore()
		if err != nil {
			sentry.CaptureException(err)
			return err
		}
		fl, err := initFirestoreListener(client)
		if err != nil {
			sentry.CaptureException(err)
			return err
		}
		// Launch persistent goutine, wait for first loading to finish, goroutine keeps running in background
		var wg sync.WaitGroup
		wg.Add(1)
		go du.listenMultiple(ctx.Context, "projects", &wg, true)
		wg.Wait()

		// Initialize the firestore cache and listener
		du.fl = fl
	}

	// Store logger for use in this caddy module instance
	du.logger = ctx.Logger()

	return nil
}

func (du *DevxUpstreams) Validate() error {
	return nil
}

func (du *DevxUpstreams) Cleanup() error {
	var errs []error
	for _, key := range du.usagePoolKeys {
		_, err := usagePool.Delete(key)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if errs != nil {
		return errs[0]
	}
	return nil
}

func (d *DevxUpstreams) loggedError(msg string, tags map[string]string) error {
	sentry.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetLevel(sentry.LevelError)
		if tags != nil {
			scope.SetTags(tags)
		}
		sentry.CaptureMessage(msg)
	})
	return errors.New(msg)
}

func (du *DevxUpstreams) GetUpstreams(r *http.Request) ([]*reverseproxy.Upstream, error) {
	serviceTypes, ok := r.Header["X-Databutton-Service-Type"]
	if !ok {
		return nil, du.loggedError("Missing X-Databutton-Service-Type.", nil)
	}
	serviceType := serviceTypes[0]

	projectIds, ok := r.Header["X-Databutton-Project-Id"]
	if !ok {
		return nil, du.loggedError("Missing X-Databutton-Project-Id.", nil)
	}
	projectId := projectIds[0]

	project, err := du.fl.LookupProject(projectId)
	if err != nil {
		// TODO: Make sure this results in a 404
		return nil, du.loggedError("Could not find project", map[string]string{
			"project_id":   projectId,
			"service_type": serviceType,
		})
	}

	regionCode, ok := REGION_LOOKUP_MAP[project.Region]
	if !ok {
		sentry.CaptureMessage(fmt.Sprintf("Could not find project in region %s", project.Region))
		regionCode = "ew"
	}

	upstream := fmt.Sprintf("%s-%s-%s-%s.a.run.app:443", serviceType, projectId, GCP_PROJECT_HASH, regionCode)

	return []*reverseproxy.Upstream{
		{
			Dial: upstream,
		},
	}, nil
}

// UnmarshalCaddyfile implements caddyfile.Unmarshaler.
// TODO: Can we remove this?
func (devx *DevxUpstreams) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	return nil
}

// Interface guards
var (
	_ caddy.Provisioner           = (*DevxUpstreams)(nil)
	_ caddy.Validator             = (*DevxUpstreams)(nil)
	_ caddy.CleanerUpper          = (*DevxUpstreams)(nil)
	_ reverseproxy.UpstreamSource = (*DevxUpstreams)(nil)
	_ caddyfile.Unmarshaler       = (*DevxUpstreams)(nil)
)
