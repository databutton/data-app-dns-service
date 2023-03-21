package dataappdnsservice

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"

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

// Used to coordinate initialization of dependencies only once
var usagePool = caddy.NewUsagePool()

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

type DevxUpstreams struct {
	projectMap map[string]Project
	logger     *zap.Logger
}

func init() {
	caddy.RegisterModule(DevxUpstreams{})
}

// CaddyModule returns the Caddy module information.
func (DevxUpstreams) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.reverse_proxy.upstreams.devx",
		New: func() caddy.Module { return new(DevxUpstreams) },
	}
}

// UnmarshalCaddyfile implements caddyfile.Unmarshaler.
func (devx *DevxUpstreams) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	return nil
}

// TODO: Update this to also delete stuff projects.
func (d *DevxUpstreams) listenMultiple(ctx context.Context, collection string, wg *sync.WaitGroup, initial bool) error {
	bg := context.Background()

	// TODO: Put projectId (databutton) as an envvar or some other config.
	client, err := firestore.NewClient(bg, GCP_PROJECT)
	if err != nil {
		sentry.CaptureException(err)
		panic(err)
	}
	defer client.Close()

	it := client.Collection(collection).Where("markedForDeletionAt", "==", nil).Snapshots(ctx)
	for {
		snap, err := it.Next()
		if err != nil {
			if status.Code(err) == codes.Canceled {
				d.logger.Info("Shutting down gracefully, I've been cancelled.")
				return nil
			}
			d.logger.Error("Snapshots.Next err", zap.Error(err))
			sentry.CaptureException(err)
		}
		if snap != nil {
			for {
				doc, err := snap.Documents.Next()
				if err == iterator.Done {
					if initial {
						// Notify the provisioner that we've done the first sync.
						wg.Done()
						d.logger.Info("Initial sync complete!")
						initial = false
					}
					break
				}
				if err != nil {
					d.logger.Error("Documents.Next err", zap.Error(err))
					sentry.CaptureException(err)
				}
				var projectData Project
				doc.DataTo(&projectData)
				projectData.ProjectID = doc.Ref.ID
				d.projectMap[projectData.ProjectID] = projectData
			}
		}
	}

}

func (d *DevxUpstreams) Provision(ctx caddy.Context) error {
	// Set up sentry
	if err := initSentry(); err != nil {
		log.Fatalf("sentry.Init: %s", err)
	}

	// Initialize the firestore cache
	var wg sync.WaitGroup
	d.logger = ctx.Logger(d)
	d.projectMap = make(map[string]Project)
	collection := "projects"
	wg.Add(1)
	go d.listenMultiple(ctx.Context, collection, &wg, true)
	wg.Wait()

	return nil
}

// Validate implements caddy.Validator
func (*DevxUpstreams) Validate() error {
	return nil
}

// Cleanup implements caddy.CleanerUpper
func (*DevxUpstreams) Cleanup() error {
	return nil
}

func (d *DevxUpstreams) getUpstreamFromProjectId(projectId string, serviceName string, gcpProjectHash string) string {
	if project, ok := d.projectMap[projectId]; ok {
		if regionCode, ok := REGION_LOOKUP_MAP[project.Region]; ok {
			return fmt.Sprintf("%s-%s-%s-%s.a.run.app:443", serviceName, projectId, gcpProjectHash, regionCode)
		} else {
			sentry.CaptureMessage("Could not find project in region")
			return fmt.Sprintf("%s-%s-%s-%s.a.run.app:443", serviceName, projectId, gcpProjectHash, "ew")
		}
	}
	sentry.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetLevel(sentry.LevelError)
		scope.SetTags(map[string]string{
			"project_id":   projectId,
			"service_name": serviceName,
		})
		sentry.CaptureMessage("Could not find upstream")
	})
	return ""
}

func (d *DevxUpstreams) GetUpstreams(r *http.Request) ([]*reverseproxy.Upstream, error) {
	serviceType := r.Header["X-Databutton-Service-Type"]
	if projectId, ok := r.Header["X-Databutton-Project-Id"]; ok {
		upstream := d.getUpstreamFromProjectId(projectId[0], serviceType[0], GCP_PROJECT_HASH)
		return []*reverseproxy.Upstream{
			{
				Dial: upstream,
			},
		}, nil
	}
	sentry.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetLevel(sentry.LevelWarning)
		sentry.CaptureMessage("Missing X-Databutton-Service-Type or X-Databutton-Project-Id")
	})
	return nil, errors.New("missing x-databutton-project-id header")
}

// Interface guards
var (
	_ caddy.Provisioner           = (*DevxUpstreams)(nil)
	_ caddy.Validator             = (*DevxUpstreams)(nil)
	_ caddy.CleanerUpper          = (*DevxUpstreams)(nil)
	_ reverseproxy.UpstreamSource = (*DevxUpstreams)(nil)
	_ caddyfile.Unmarshaler       = (*DevxUpstreams)(nil)
)
