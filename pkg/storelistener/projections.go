package storelistener

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/getsentry/sentry-go"
)

// Partial Appbutler document to be parsed from firestore document
type AppbutlerDoc struct {
	UpdateTime          time.Time
	MarkedForDeletionAt *time.Time `firestore:"markedForDeletionAt"`
	ProjectId           string     `firestore:"projectId,omitempty"`
	ServiceType         string     `firestore:"serviceType,omitempty"`
	RegionCode          string     `firestore:"regionCode,omitempty"`
	CloudRunServiceId   string     `firestore:"cloudRunServiceId,omitempty"`
	ServiceIsReady      bool       `firestore:"serviceIsReady,omitempty"`
	OverrideURL         string     `firestore:"overrideURL,omitempty"`
	Username            string     `firestore:"username,omitempty"`
	CustomDomain        string     `firestore:"customDomain,omitempty"`
}

type AppbutlersProjection struct {
	upstreams map[string]string
	usernames map[string]string
}

func NewAppbutlersProjection() Projection {
	return &AppbutlersProjection{
		upstreams: make(map[string]string),
		usernames: make(map[string]string),
	}
}

func (p *AppbutlersProjection) Collection() string {
	return CollectionAppbutlers
}

func (p *AppbutlersProjection) shouldSkipAppbutler(data AppbutlerDoc) bool {
	if data.ProjectId == "" {
		// Skip free pool appbutlers without assigned projects, there's nothing to route
		return true
	}

	if !data.ServiceIsReady {
		// Skip before service is ready to receive requests
		return true
	}

	if data.MarkedForDeletionAt != nil {
		// Skip when marked for garbage collection
		return true
	}

	return false
}

func (p *AppbutlersProjection) makeCloudRunUrl(data AppbutlerDoc) string {
	if data.OverrideURL != "" {
		return data.OverrideURL
	}
	return fmt.Sprintf(
		"%s-%s-%s.a.run.app:443",
		data.CloudRunServiceId,
		GCP_PROJECT_HASH,
		data.RegionCode,
	)
}

func (p *AppbutlersProjection) Update(ctx context.Context, doc *firestore.DocumentSnapshot) error {
	var data AppbutlerDoc
	if err := doc.DataTo(&data); err != nil {
		return err
	}

	if p.shouldSkipAppbutler(data) {
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

	// Add to projection maps
	key := makeKey(data.ServiceType, data.ProjectId)
	p.upstreams[key] = p.makeCloudRunUrl(data)
	p.usernames[key] = data.Username

	return nil
}

func (p *AppbutlersProjection) LogSwapResults(oldMap, newMap map[string]string) {
	if len(oldMap) == 0 {
		sentry.WithScope(func(scope *sentry.Scope) {
			scope.SetExtra("oldUpstreams", len(oldMap))
			scope.SetExtra("newUpstreams", len(newMap))
			sentry.CaptureMessage("Set initial upstreams")
		})
	} else {
		added := make(map[string]struct{})
		removed := make(map[string]struct{})
		for k := range oldMap {
			if _, ok := newMap[k]; !ok {
				removed[k] = struct{}{}
			}
		}
		for k := range newMap {
			if _, ok := oldMap[k]; !ok {
				added[k] = struct{}{}
			}
		}
		sentry.WithScope(func(scope *sentry.Scope) {
			scope.SetExtra("oldUpstreams", len(oldMap))
			scope.SetExtra("newUpstreams", len(newMap))
			scope.SetExtra("added", len(added))
			scope.SetExtra("removed", len(removed))
			sentry.CaptureMessage("Swapped upstreams")
		})
	}
}

func (p *AppbutlersProjection) Swap(l *Listener) {
	p.LogSwapResults(l.upstreams.GetMap(), p.upstreams)
	l.upstreams.SetMap(p.upstreams)
	l.usernames.SetMap(p.usernames)
}

// Partial Domain document to be parsed from firestore document
type DomainDoc struct {
	Active     bool   `firestore:"active"`
	Validation string `firestore:"validation"`
	ProjectId  string `firestore:"projectId"`
}

type DomainsProjection struct {
	domainProjects map[string]string
}

func NewDomainsProjection() Projection {
	return &DomainsProjection{
		domainProjects: make(map[string]string),
	}
}

func (p *DomainsProjection) Collection() string {
	return CollectionDomains
}

func (p *DomainsProjection) Update(ctx context.Context, doc *firestore.DocumentSnapshot) error {
	var data DomainDoc
	if err := doc.DataTo(&data); err != nil {
		return err
	}

	// TODO: Active is not consistently set in the /domains collection, make sure
	// it is for new domains and then we can use it here when streamlit is dead:
	// if !data.Active {
	// 	return nil
	// }
	// if data.Validation != "validated" {
	// 	return nil
	// }
	if data.ProjectId == "" {
		// Treating missing projectId as "do not route"
		return nil
	}

	// Add to projection map
	customDomain := doc.Ref.ID
	p.domainProjects[customDomain] = data.ProjectId

	// l.logger.Warn(fmt.Sprintf("ADDED %s  %s", customDomain, data.ProjectId))

	return nil
}

func (p *DomainsProjection) Swap(l *Listener) {
	l.domainProjects.SetMap(p.domainProjects)
}
