package storelistener

import (
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/databutton/data-app-dns-service/pkg/threadsafemap"
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
	Services   *threadsafemap.Map[string, ServiceValues]
	Domains    *threadsafemap.Map[string, DomainValues]
	Appbutlers *threadsafemap.Map[string, AppbutlerValues]
}

func NewInfraProjection() *InfraProjection {
	return &InfraProjection{
		Services:   threadsafemap.New[string, ServiceValues](),
		Domains:    threadsafemap.New[string, DomainValues](),
		Appbutlers: threadsafemap.New[string, AppbutlerValues](),
	}
}

func (p *InfraProjection) GetService(projectId, serviceType string) (ServiceValues, bool) {
	return p.Services.Get(ServiceKey{ProjectId: projectId, ServiceType: serviceType}.String())
}

func (p *InfraProjection) GetDomain(customDomain string) (DomainValues, bool) {
	return p.Domains.Get(customDomain)
}

func (p *InfraProjection) GetAppbutler(appbutlerId string) (AppbutlerValues, bool) {
	return p.Appbutlers.Get(appbutlerId)
}

func (p *InfraProjection) CountServices() int {
	return p.Services.Len()
}

func (p *InfraProjection) CountDomains() int {
	return p.Domains.Len()
}

func (p *InfraProjection) CountAppbutlers() int {
	return p.Appbutlers.Len()
}

// UpsertPolled is the upsert-or-delete version used by polling implementation
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
		if appbutler, ok := p.Appbutlers.Get(appbutlerId); ok {
			p.Services.DeleteSync(
				ServiceKey{
					ProjectId:   appbutler.ProjectId,
					ServiceType: appbutler.ServiceType,
				}.String(),
			)

			if appbutler.CustomDomain != "" {
				p.Domains.DeleteSync(appbutler.CustomDomain)
			}

			p.Appbutlers.DeleteSync(appbutlerId)
		}
		return nil
	}

	if err := data.validate(); err != nil {
		return err
	}

	// Store for lookup on appbutlerId
	p.Appbutlers.SetSync(
		doc.Ref.ID,
		AppbutlerValues{
			ProjectId:    data.ProjectId,
			ServiceType:  data.ServiceType,
			CustomDomain: data.CustomDomain,
		},
	)

	// Store for lookup on customDomain
	if data.CustomDomain != "" {
		p.Domains.SetSync(data.CustomDomain, DomainValues{ProjectId: data.ProjectId})
	}

	// Store for lookup on projectId and serviceType
	p.Services.SetSync(
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

// RemoveForSnapshotListener is used together with Upsert by the snapshot listener implementation
func (p *InfraProjection) RemoveForSnapshotListener(doc *firestore.DocumentSnapshot) error {
	// TODO: TEST Will we get any data here when it has been removed?
	//        Or will we need to add a map to index from doc.ID to service key?
	// id := doc.Ref.ID

	var data AppbutlerDoc
	if err := doc.DataTo(&data); err != nil {
		return err
	}

	serviceKey := ServiceKey{ProjectId: data.ProjectId, ServiceType: data.ServiceType}.String()
	p.Services.DeleteSync(serviceKey)

	if data.CustomDomain != "" {
		domain, ok := p.Domains.Get(data.CustomDomain)
		if ok {
			// Note: There's some potential for getting things wrong here if the source data
			// doesn't adhere to expected invariants such as projectid 1-1 domain
			if domain.ProjectId == data.ProjectId {
				p.Domains.DeleteSync(data.CustomDomain)
			}
		}
	}

	return nil
}

// UpsertForSnapshotListener is used together with Remove by the snapshot listener implementation
func (p *InfraProjection) UpsertForSnapshotListener(doc *firestore.DocumentSnapshot) error {
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
	p.Services.SetSync(serviceKey, service)

	if data.CustomDomain != "" {
		domain := DomainValues{ProjectId: data.ProjectId}
		p.Domains.SetSync(data.CustomDomain, domain)
	}

	return nil
}

func (p *InfraProjection) DebugDumpDomains() {
	fmt.Printf("/////// BEGIN DEBUGGING DOMAINS DUMP\n")
	for _, kv := range p.Domains.Items() {
		fmt.Printf("  %s : %+v\n", kv.K, kv.V)
	}
	fmt.Printf("/////// END DEBUGGING DOMAINS DUMP\n")
}

func (p *InfraProjection) DebugDumpServices() {
	fmt.Printf("/////// BEGIN DEBUGGING SERVICES DUMP\n")
	for _, kv := range p.Services.Items() {
		fmt.Printf("  %s : %+v\n", kv.K, kv.V)
	}
	fmt.Printf("/////// END DEBUGGING SERVICES DUMP\n")
}

func (p *InfraProjection) DebugDumpAppbutlers() {
	fmt.Printf("/////// BEGIN DEBUGGING APPBUTLERS DUMP\n")
	for _, kv := range p.Appbutlers.Items() {
		fmt.Printf("  %s : %+v\n", kv.K, kv.V)
	}
	fmt.Printf("/////// END DEBUGGING APPBUTLERS DUMP\n")
}
