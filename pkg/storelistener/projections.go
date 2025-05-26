package storelistener

import (
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/AlekSi/pointer"
)

const AppbutlerCursorTimestampField = "routingChangedAt"

// Partial Appbutler document to be parsed from firestore document
type AppbutlerDoc struct {
	// Updated when routing changes
	RoutingChangedAt time.Time `firestore:"routingChangedAt,omitempty"`

	// Which project is it owned by (should never be blank now)
	ProjectId string `firestore:"projectId,omitempty"`

	// What is the service for (devx or prodx)
	ServiceType string `firestore:"serviceType,omitempty"`

	// Is it not deleted
	MarkedForDeletionAt *time.Time `firestore:"markedForDeletionAt,omitempty"`

	// Is it ready to serve
	ServiceIsReady bool `firestore:"serviceIsReady,omitempty"`

	// What is the url of the target service
	// ... in google cloud run
	RegionCode        string `firestore:"regionCode,omitempty"`
	CloudRunServiceId string `firestore:"cloudRunServiceId,omitempty"`
	// ... or anywhere else
	OverrideURL string `firestore:"overrideURL,omitempty"`
	InternalURL string `firestore:"internalURL,omitempty"`

	// Where is it deployed (no CustomDomain means username.databutton.app)
	Username string `firestore:"username,omitempty"`
	// Appname     string `firestore:"appname,omitempty"` // TODO: Use this for _users/username/apps/appname URL?
	CustomDomain string `firestore:"customDomain,omitempty"`
}

func (data AppbutlerDoc) Fields() []string {
	return []string{
		AppbutlerCursorTimestampField,
		//
		"projectId",
		"serviceType",
		//
		"markedForDeletionAt",
		"serviceIsReady",
		//
		"regionCode",
		"cloudRunServiceId",
		"overrideURL",
		"internalURL",
		//
		"username",
		"customDomain",
	}
}

func (data *AppbutlerDoc) validate() error {
	if data.ProjectId == "" || data.ServiceType == "" {
		return ErrMissingServiceIdentityData
	}
	if data.InternalURL == "" && data.OverrideURL == "" && (data.RegionCode == "" || data.CloudRunServiceId == "") {
		return ErrMissingUpstreamData
	}
	return nil
}

func (data *AppbutlerDoc) makeTargetUrl() string {
	// Sometimes set for testing
	if data.OverrideURL != "" {
		return data.OverrideURL
	}

	// The new way
	if data.InternalURL != "" {
		return data.InternalURL
	}

	// Legacy construction of cloud run url
	if data.CloudRunServiceId != "" && data.RegionCode != "" {
		return fmt.Sprintf(
			"%s-%s-%s.a.run.app:443",
			data.CloudRunServiceId,
			GCP_PROJECT_HASH,
			data.RegionCode,
		)
	}

	return ""
}

type ServiceKey struct {
	ProjectId   string
	ServiceType string
}

// Make key for upstreams map lookup
func (sk ServiceKey) String() string {
	return sk.ProjectId + sk.ServiceType
}

type ServiceValues struct {
	Upstream string
	Username string
}

type DomainValues struct {
	ProjectId string
}

type AppbutlerValues struct {
	ProjectId    string
	ServiceType  string
	CustomDomain string
}

type InfraProjection struct {
	Services   *sync.Map
	Domains    *sync.Map
	Appbutlers *sync.Map
}

func NewInfraProjection() *InfraProjection {
	return &InfraProjection{
		Services:   &sync.Map{},
		Domains:    &sync.Map{},
		Appbutlers: &sync.Map{},
	}
}

func (p *InfraProjection) UpsertPolled(doc *firestore.DocumentSnapshot) error {
	var data AppbutlerDoc
	if err := doc.DataTo(&data); err != nil {
		return err
	}

	// Delete or skip if:
	// - not ready
	// - marked for deletion
	// - from pool
	if !data.ServiceIsReady || data.MarkedForDeletionAt != nil || data.ProjectId == "" {
		// Ignore if we have no record of the appbutler,
		// otherwise ignore new values in data
		// and clean up based on our cached appbutler doc
		appbutlerId := doc.Ref.ID
		if value, ok := p.Appbutlers.Load(appbutlerId); ok {
			appbutler := value.(AppbutlerValues)

			p.Services.Delete(
				ServiceKey{
					ProjectId:   appbutler.ProjectId,
					ServiceType: appbutler.ServiceType,
				}.String(),
			)

			if appbutler.CustomDomain != "" {
				p.Domains.Delete(appbutler.CustomDomain)
			}

			p.Appbutlers.Delete(appbutlerId)
		}
		return nil
	}

	if err := data.validate(); err != nil {
		return err
	}

	// Store for lookup on appbutlerId
	p.Appbutlers.Store(
		doc.Ref.ID,
		AppbutlerValues{
			ProjectId:    data.ProjectId,
			ServiceType:  data.ServiceType,
			CustomDomain: data.CustomDomain,
		},
	)

	// Store for lookup on customDomain
	if data.CustomDomain != "" {
		p.Domains.Store(data.CustomDomain, DomainValues{ProjectId: data.ProjectId})
	}

	// Store for lookup on projectId and serviceType
	p.Services.Store(
		ServiceKey{
			ProjectId:   data.ProjectId,
			ServiceType: data.ServiceType,
		}.String(),
		ServiceValues{
			Upstream: data.makeTargetUrl(),
			Username: data.Username,
		},
	)

	return nil
}

func (p *InfraProjection) Remove(doc *firestore.DocumentSnapshot) error {
	// TODO: TEST Will we get any data here when it has been removed?
	//        Or will we need to add a map to index from doc.ID to service key?
	// id := doc.Ref.ID

	var data AppbutlerDoc
	if err := doc.DataTo(&data); err != nil {
		return err
	}

	serviceKey := ServiceKey{ProjectId: data.ProjectId, ServiceType: data.ServiceType}.String()
	p.Services.Delete(serviceKey)

	if data.CustomDomain != "" {
		domain, ok := p.Domains.Load(data.CustomDomain)
		if ok {
			// Note: There's some potential for getting things wrong here if the source data
			// doesn't adhere to expected invariants such as projectid 1-1 domain
			if domain.(DomainValues).ProjectId == data.ProjectId {
				p.Domains.Delete(data.CustomDomain)
			}
		}
	}

	return nil
}

func (p *InfraProjection) Upsert(doc *firestore.DocumentSnapshot) error {
	var data AppbutlerDoc
	if err := doc.DataTo(&data); err != nil {
		return err
	}

	// Skip poolbutlers
	if data.ProjectId == "" {
		return nil
	}

	if err := data.validate(); err != nil {
		return err
	}

	// Add to projection maps
	serviceKey := ServiceKey{ProjectId: data.ProjectId, ServiceType: data.ServiceType}.String()
	service := ServiceValues{Upstream: data.makeTargetUrl(), Username: data.Username}
	p.Services.Store(serviceKey, service)

	if data.CustomDomain != "" {
		domain := DomainValues{ProjectId: data.ProjectId}
		p.Domains.Store(data.CustomDomain, domain)
	}

	return nil
}

func (p *InfraProjection) GetDomain(customDomain string) (DomainValues, bool) {
	value, found := p.Domains.Load(customDomain)
	if !found {
		return DomainValues{}, false
	}
	domain, ok := value.(DomainValues)
	return domain, ok
}

func (p *InfraProjection) GetService(projectId, serviceType string) (ServiceValues, bool) {
	serviceKey := ServiceKey{ProjectId: projectId, ServiceType: serviceType}.String()
	value, found := p.Services.Load(serviceKey)
	if !found {
		return ServiceValues{}, false
	}
	service, ok := value.(ServiceValues)
	return service, ok
}

func sizeOfSynMap(m *sync.Map) int {
	n := pointer.To(0)
	m.Range(func(k, v any) bool { *n += 1; return true })
	return *n
}

func (p *InfraProjection) CountServices() int {
	return sizeOfSynMap(p.Services)
}

func (p *InfraProjection) CountDomains() int {
	return sizeOfSynMap(p.Domains)
}

func (p *InfraProjection) DebugDumpDomains() {
	fmt.Printf("/////// BEGIN DEBUGGING DOMAINS DUMP\n")
	p.Domains.Range(func(k any, v any) bool {
		fmt.Printf("  %s : %+v\n", k, v)
		return true
	})
	fmt.Printf("/////// END DEBUGGING DOMAINS DUMP\n")
}

func (p *InfraProjection) DebugDumpServices() {
	fmt.Printf("/////// BEGIN DEBUGGING SERVICES DUMP\n")
	p.Services.Range(func(k any, v any) bool {
		fmt.Printf("  %s : %+v\n", k, v)
		return true
	})
	fmt.Printf("/////// END DEBUGGING SERVICES DUMP\n")
}
