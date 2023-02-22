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
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DevxUpstreams struct {
	GCP_PROJECT      string `json:"gcp_project,omitempty"`
	GCP_PROJECT_HASH string `json:"gcp_project_hash,omitempty"`
}

// CaddyModule returns the Caddy module information.
func (DevxUpstreams) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.reverse_proxy.upstreams.devx",
		New: func() caddy.Module { return new(DevxUpstreams) },
	}
}

func init() {
	caddy.RegisterModule(DevxUpstreams{})
}

type Project struct {
	ProjectID string `firestore:"projectId"`
	Region    string `firestore:"region"`
	DevxURL   string `firestore:"devxUrl"`
}

// This is a project map that can hold projects. Looks up on projectId
var allProjects = make(map[string]Project, 10000)

// listenMultiple listens to a query, returning the names of all cities
// for a state.
func listenMultiple(ctx context.Context, gcpProject string, collection string, wg *sync.WaitGroup) error {
	// projectID := "project-id"
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// TODO: Put projectId (databutton) as an envvar or some other config.
	client, err := firestore.NewClient(ctx, gcpProject)
	if status.Code(err) == codes.DeadlineExceeded {
		return nil
	}
	if err != nil {
		return fmt.Errorf("firestore.NewClient: %v", err)
	}
	defer client.Close()
	initial := true

	it := client.Collection(collection).Where("markedForDeletionAt", "==", nil).Snapshots(ctx)
	for {
		snap, err := it.Next()
		// DeadlineExceeded will be returned when ctx is cancelled.
		if err != nil {
			return fmt.Errorf("Snapshots.Next: %v", err)
		}
		if snap != nil {
			for {
				doc, err := snap.Documents.Next()
				if err == iterator.Done {
					if initial {
						// Notify the provisioner that we've done the first sync.
						wg.Done()
						initial = false
					}
					break
				}
				if err != nil {
					return fmt.Errorf("Documents.Next: %v", err)
				}
				var projectData Project
				doc.DataTo(&projectData)
				allProjects[projectData.ProjectID] = projectData
			}
		}
	}
}

func (d *DevxUpstreams) Provision(ctx caddy.Context) error {
	var wg sync.WaitGroup
	collection := "projects"
	wg.Add(1)
	go listenMultiple(ctx.Context, collection, d.GCP_PROJECT_HASH, &wg)
	wg.Wait()
	return nil
}

var REGION_LOOKUP_MAP = map[string]string{
	"europe-west1":      "ew",
	"europe-north1":     "lz",
	"europe-southwest1": "no",
	"europe-west9":      "od",
	"europe-west4":      "ez",
	"europe-west8":      "oc",
}

func getUpstreamFromProjectId(projectId string, serviceName string, gcpProjectHash string) string {
	if project, ok := allProjects[projectId]; ok {
		if regionCode, ok := REGION_LOOKUP_MAP[project.Region]; ok {
			return fmt.Sprintf("%s-%s-%s-%s.a.run.app:443", serviceName, projectId, gcpProjectHash, regionCode)
		}
	}
	return ""
}

func (d *DevxUpstreams) GetUpstreams(r *http.Request) ([]*reverseproxy.Upstream, error) {
	serviceType := r.Header["X-Databutton-Service-Type"]
	if projectId, ok := r.Header["X-Databutton-Project-Id"]; ok {
		return []*reverseproxy.Upstream{
			{
				Dial: getUpstreamFromProjectId(projectId[0], serviceType[0], d.GCP_PROJECT_HASH),
			},
		}, nil
	}
	return nil, errors.New("missing x-databutton-project-id header")
}

// Interface guards
var (
	_ caddy.Provisioner           = (*DevxUpstreams)(nil)
	_ reverseproxy.UpstreamSource = (*DevxUpstreams)(nil)
)
