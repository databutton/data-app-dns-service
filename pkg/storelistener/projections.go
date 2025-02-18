package storelistener

import (
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
)

// Partial Appbutler document to be parsed from firestore document
type AppbutlerDoc struct {
	UpdateTime time.Time

	// Which project is it owned by (should never be blank now)
	ProjectId string `firestore:"projectId,omitempty"`

	// What is the service for (devx or prodx)
	ServiceType string `firestore:"serviceType,omitempty"`

	// Is it ready to serve (should always be true now)
	// ServiceIsReady      bool       `firestore:"serviceIsReady,omitempty"`

	// Is it not deleted (should always be nil now)
	// MarkedForDeletionAt *time.Time  `firestore:"markedForDeletionAt,omitempty"`

	// What is the url of the target service
	// ... in google cloud run
	RegionCode        string `firestore:"regionCode,omitempty"`
	CloudRunServiceId string `firestore:"cloudRunServiceId,omitempty"`
	// ... or anywhere else
	OverrideURL string `firestore:"overrideURL,omitempty"`
	InternalURL string `firestore:"internalURL,omitempty"`

	// Where is it deployed (no CustomDomain means username.databutton.app)
	Username     string `firestore:"username,omitempty"`
	CustomDomain string `firestore:"customDomain,omitempty"`
}

func (data AppbutlerDoc) Fields() []string {
	return []string{
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

type InfraProjection struct {
	Services *sync.Map
	Domains  *sync.Map
}

func NewInfraProjection() *InfraProjection {
	return &InfraProjection{
		Services: &sync.Map{},
		Domains:  &sync.Map{},
	}
}

func (p *InfraProjection) Remove(doc *firestore.DocumentSnapshot) error {
	// FIXME: TEST Will we get any data here when it has been removed?
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
