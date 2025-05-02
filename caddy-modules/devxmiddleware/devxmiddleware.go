// Middleware module to populate request context with data from listener and control some headers including CORS.
//
// https://caddyserver.com/docs/extending-caddy
package devxmiddleware

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/databutton/data-app-dns-service/caddy-modules/devxlistener"
	"github.com/databutton/data-app-dns-service/pkg/storelistener"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
)

// Set a project id here for extra debugging logs in prod for just this one app
const (
	// LOG_EXTRA_FOR_PROJECT_ID = "eadb6129-8fca-4866-b572-d2f9bcbc1146"
	LOG_EXTRA_FOR_PROJECT_ID = ""
)

type LookerUper interface {
	LookupUpProjectIdFromDomain(domain string) string
	LookupUpstreamHost(ctx context.Context, projectID, serviceType string) string
	LookupUsername(projectID, serviceType string) string
}

// For debugging
type GetInfraer interface {
	GetInfra() *storelistener.InfraProjection
}

type DevxMiddlewareModule struct {
	// Configuration fields, if any
	InstanceTag string `json:"instance_tag,omitempty"`

	// Internal state
	debug            bool
	logger           *zap.Logger
	listener         LookerUper
	mockUpstreamHost string
}

// Register module with caddy
func init() {
	caddy.RegisterModule(new(DevxMiddlewareModule))
	httpcaddyfile.RegisterHandlerDirective("devxmiddleware", parseCaddyfile)
}

// Implement caddy.Module interface
func (m *DevxMiddlewareModule) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.devxmiddleware",
		New: func() caddy.Module { return new(DevxMiddlewareModule) },
	}
}

func (m *DevxMiddlewareModule) Provision(ctx caddy.Context) error {
	m.logger = ctx.Logger().With(zap.String("instanceTag", m.InstanceTag))

	// Get initialized listener from devxlistener app module
	l, err := devxlistener.Get(ctx)
	if err != nil {
		m.logger.Error("Failed to get listener", zap.Error(err))
		return err
	}
	if l == nil {
		m.logger.Error("Listener is nil!")
	}
	m.listener = l

	// Set some mock state from env vars, for testing
	m.setMockStateFromEnv()

	return nil
}

// Mock setup for tests
func (m *DevxMiddlewareModule) setMockStateFromEnv() {
	mockUrl := os.Getenv("MOCK_DEVX_UPSTREAM_URL")
	if mockUrl != "" {
		u, err := url.Parse(mockUrl)
		if err != nil {
			m.logger.Error("failed to parse mock url", zap.String("mockurl", mockUrl))
		} else {
			m.mockUpstreamHost = u.Host
			m.logger.Warn("TEST MODE: MOCKING ALL UPSTREAM HOSTS!",
				zap.String("mockUpstreamHost", m.mockUpstreamHost),
			)
		}
	}
}

// Validate implements caddy.Validator
// Called after Provision.
func (m *DevxMiddlewareModule) Validate() error {
	m.logger.Info("devxmiddleware: Validate")
	return nil
}

// Cleanup implements caddy.CleanerUpper
func (m *DevxMiddlewareModule) Cleanup() error {
	m.logger.Info("devxmiddleware: Cleanup")
	return nil
}

// From https://caddyserver.com/docs/extending-caddy#complete-example
// UnmarshalCaddyfile implements caddyfile.Unmarshaler
func (m *DevxMiddlewareModule) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next() // consume directive name

	// require an argument
	if !d.NextArg() {
		return d.ArgErr()
	}

	// store the argument
	m.InstanceTag = d.Val()
	return nil
}

// parseCaddyfile unmarshals tokens from h into a new MiddlewareModule.
func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	m := new(DevxMiddlewareModule)
	err := m.UnmarshalCaddyfile(h.Dispenser)
	return m, err
}

var DevxKey caddy.CtxKey = "devx.data"

type DevxRequestData struct {
	ProjectID    string
	ServiceType  string
	UpstreamHost string
	CustomDomain string
	Hub          *sentry.Hub
}

func makeOriginalPath(projectID, serviceType, path string) string {
	if strings.HasPrefix(path, "/_projects") {
		return path
	}
	// Path is on the form /app/routes/<endpointpath>
	return fmt.Sprintf("/_projects/%s/dbtn/%s%s", projectID, serviceType, path)
}

func (m *DevxMiddlewareModule) DebugDumpDomains() {
	fmt.Printf("/////// BEGIN DEBUGGING DOMAINS DUMP\n")
	infra := m.listener.(GetInfraer).GetInfra()
	infra.Domains.Range(func(k any, v any) bool {
		fmt.Printf("  %s : %+v\n", k, v)
		return true
	})
	fmt.Printf("/////// END DEBUGGING DOMAINS DUMP\n")
}

func (m *DevxMiddlewareModule) DebugDumpServices() {
	fmt.Printf("/////// BEGIN DEBUGGING SERVICES DUMP\n")
	infra := m.listener.(GetInfraer).GetInfra()
	infra.Services.Range(func(k any, v any) bool {
		fmt.Printf("  %s : %+v\n", k, v)
		return true
	})
	fmt.Printf("/////// END DEBUGGING SERVICES DUMP\n")
}

// ServeHTTP implements caddyhttp.MiddlewareHandler
func (m *DevxMiddlewareModule) ServeHTTP(w http.ResponseWriter, r *http.Request, h caddyhttp.Handler) error {
	ctx := r.Context()

	// TODO: Maybe add zap and sentry middleware to entire caddy stack and not just devx parts
	// Clone a sentry hub for this request
	var hub *sentry.Hub
	if m.debug {
		hub = sentry.CurrentHub().Clone()
		hub.ConfigureScope(func(scope *sentry.Scope) {
			scope.SetRequest(r)
		})
	}

	// Origin should be one of
	//   https://databutton.com
	//   https://username.databutton.app
	//   https://customdomain.com
	// if a cross-origin request is made from a standard Databutton hosted app.
	// It can be something else if a cross-origin request is made from an external app.
	originHeader := r.Header.Get("Origin")

	// Referer should be e.g.
	// https://api.databutton.com/_projects/PID/dbtn/devx/ui/...restpath
	// https://username.databutton.app/appname/...restpath
	// https://customdomain.com/...restpath
	refererHeader := r.Header.Get("Referer")

	// Databutton app variables set by caddy from url if possible
	projectID := r.Header.Get("X-Databutton-Project-Id")
	serviceType := r.Header.Get("X-Databutton-Service-Type")

	// Enable extra debugging logs for a specific app while keeping a minimal overhead for all other requests
	enableExtraDebugLogs := projectID == LOG_EXTRA_FOR_PROJECT_ID && LOG_EXTRA_FOR_PROJECT_ID != "" && serviceType == "prodx"
	if enableExtraDebugLogs {
		m.logger.Warn("DEBUGGING: TOP OF DEVX MIDDLEWARE", zap.String("projectID", projectID),
			zap.String("serviceType", serviceType),
			zap.String("originHeader", originHeader),
			zap.String("refererHeader", refererHeader),
			zap.String("requestUrl", r.URL.String()),
			zap.Bool("isDebugCase", enableExtraDebugLogs),
		)
		// Example result from a prodx request:
		// originHeader: "https://martinal.databutton.app"
		// projectID: "eadb6129-8fca-4866-b572-d2f9bcbc1146"
		// refererHeader: "https://martinal.databutton.app/"
		// requestUrl: "/app/routes/chat/tools"
		// serviceType: "prodx"
	}

	// Validate origin _if present_
	var originHost string
	if originHeader != "" {
		originUrl, err := url.Parse(originHeader)
		if err != nil {
			m.logger.Error(
				"Invalid Origin",
				zap.String("Origin", originHeader),
				zap.Error(err),
				zap.Bool("isDebugCase", enableExtraDebugLogs),
			)
			w.WriteHeader(http.StatusBadGateway)
			return nil
		}
		if originUrl.Scheme != "https" && !strings.HasPrefix(originUrl.Host, "localhost") {
			m.logger.Error(
				"Insecure Origin",
				zap.String("Origin", originHeader),
				zap.Bool("isDebugCase", enableExtraDebugLogs),
			)
			w.WriteHeader(http.StatusBadGateway)
			return nil
		}
		originHost = originUrl.Host
	}

	// This will be set to originHeader in cases where cross-domain request is valid from origin
	var corsOrigin string

	var customDomain string

	// Origin shows where app is served from:
	// - devx           databutton.com            /_projects/{projectID}/dbtn/devx/ui/
	// - prodx          username.databutton.app   /appname/
	// - custom         custom.com                /

	// FIXME: Make the non-_projects cases work
	// API may be called on URL:
	// - appx             databutton.com            /_projects/{projectID}/dbtn/{serviceType}/
	// - appx             api.databutton.com        /_projects/{projectID}/dbtn/{serviceType}/
	// - databutton.app   username.databutton.app   /appname/api/* -> /_projects/{projectID}/dbtn/{serviceType}/app/routes/*
	// - custom           custom.com                /api/* -> /_projects/{projectID}/dbtn/{serviceType}/app/routes/*

	if projectID == "" {
		// This means the URL is not _projects/...
		// Get project id from domain lookup
		if strings.HasSuffix(originHost, ".databutton.app") {
			// FIXME: Case: username.databutton.app/appname/api/...
			// originHost = username.databutton.app
			// appnameHeader := r.Header.Get("X-Databutton-Appname")
			// projectID = m.listener.LookupUpProjectIdForDatabuttonAppDomain(
			// 	strings.TrimSuffix(originHost, ".databutton.app"),
			// 	appnameHeader,
			// )
		} else {
			// FIXME: Case: custom.com/api/...
			// originHost = custom.com
			projectID = m.listener.LookupUpProjectIdFromDomain(originHost)
		}

		if projectID == "" {
			// No project specified, can't find domain
			m.logger.Error(
				"No projectId and domain lookup failed",
				zap.String("originHost", originHost),
			)
			w.WriteHeader(http.StatusBadGateway)
			return nil
		}

		// Accept if project found for this domain and header and origin matches
		corsOrigin = originHeader
		r.Header.Set("X-Databutton-Project-Id", projectID)

		// Emulate what we already do in caddy for regular /_projects/.../prodx requests
		r.Header.Set(
			"X-Original-Path",
			makeOriginalPath(projectID, serviceType, r.URL.Path),
		)

	} else if projectID == "8de60c46-e25f-45fe-8df7-b5dd8b7f3b4d" || projectID == "f752666e-efd8-455d-b42f-6738931207e7" {
		// TODO: Add config for allowed origins to apps, special case for now
		// extraAllowedOrigins := m.listener.LookupExtraAllowedOrigins(projectID, serviceType)
		extraAllowedOrigins := []string{"*"}
		for _, allowedOrigin := range extraAllowedOrigins {
			if originHeader == allowedOrigin || allowedOrigin == "*" {
				r.Header.Set("X-Dbtn-Proxy-Case", "extra-allowed-origins")
				corsOrigin = originHeader
				customDomain = originHost
				break
			}
		}
	} else {
		isDevx := serviceType == "devx"
		if originHeader == "" {
			// Same-domain requests, requests from backends, etc, never set cors
			r.Header.Set("X-Dbtn-Proxy-Case", "no-origin")
		} else if originHeader == "https://databutton.com" { // isDevx &&
			// Devx is served from databutton.com
			r.Header.Set("X-Dbtn-Proxy-Case", "databutton-origin")
			corsOrigin = originHeader
		} else if !isDevx && strings.HasSuffix(originHost, ".databutton.app") {
			// New style hosting at per-user subdomains
			r.Header.Set("X-Dbtn-Proxy-Case", "user-subdomain")
			originUsername := strings.TrimSuffix(originHost, ".databutton.app")

			// Look up username of owner of project
			ownerUsername := m.listener.LookupUsername(projectID, serviceType)

			if ownerUsername == "" {
				// No owner username associated with project
				m.logger.Error("No owner username associated with project",
					zap.String("projectID", projectID),
					zap.String("serviceType", serviceType),
					zap.String("originHost", originHost),
					zap.String("originUsername", originUsername),
					zap.Bool("isDebugCase", enableExtraDebugLogs),
				)
				w.WriteHeader(http.StatusBadGateway)
				return nil
			} else if originUsername == ownerUsername {
				// Only set cors if username is owner of projectID
				corsOrigin = originHeader
			} else {
				// Log attempt at accessing another project
				m.logger.Error(
					"Attempt at accessing another users project",
					zap.String("originHost", originHost),
					zap.String("ownerUsername", ownerUsername),
					zap.String("username", originUsername),
					zap.String("projectID", projectID),
					zap.Bool("isDebugCase", enableExtraDebugLogs),
				)
				w.WriteHeader(http.StatusBadGateway)
				return nil
			}
		} else if isDevx && strings.HasPrefix(originHost, "localhost") {
			// Call comes from localhost, i.e. running webapp locally
			r.Header.Set("X-Dbtn-Proxy-Case", "localhost-for-dev")
			// TODO: This would not be necessary if vite proxies
			//       were set up to set Origin to https://databutton.com
			corsOrigin = originHeader
		} else {
			// Call comes from outside our infra
			r.Header.Set("X-Dbtn-Proxy-Case", "beyond-customdomain")

			// Get project id from domain lookup
			originDomainProjectID := m.listener.LookupUpProjectIdFromDomain(originHost)

			if originDomainProjectID == "" {
				// No project associated with domain
				if enableExtraDebugLogs {
					m.logger.Error(
						"No project associated with domain",
						zap.String("originHost", originHost),
						zap.String("projectID", projectID),
						zap.String("serviceType", serviceType),
						zap.Bool("isDebugCase", enableExtraDebugLogs),
					)
				}
				w.WriteHeader(http.StatusBadGateway)
				return nil
			} else if originDomainProjectID == projectID {
				// Set cors if project found for this domain and header and origin matches
				corsOrigin = originHeader
				customDomain = originHost
			} else {
				// Log attempt at accessing another project
				m.logger.Error(
					"Attempt at accessing project not associated with domain",
					zap.String("originHost", originHost),
					zap.String("originDomainProjectID", originDomainProjectID),
					zap.String("projectID", projectID),
					zap.Bool("isDebugCase", enableExtraDebugLogs),
				)
				w.WriteHeader(http.StatusBadGateway)
				return nil
			}
		}
	}

	// Set cors headers allowing this origin
	if corsOrigin != "" {
		setCorsHeaders(w, corsOrigin)
	}

	// Short-circuit options requests
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusNoContent)
		return nil
	}

	// Look up upstream URL
	upstream := m.listener.LookupUpstreamHost(ctx, projectID, serviceType)
	if upstream == "" {
		m.logger.Warn("devxmiddleware: upstream is blank",
			zap.String("projectID", projectID),
			zap.String("serviceType", serviceType),
			zap.String("originHost", originHost),
			zap.Bool("isDebugCase", enableExtraDebugLogs),
		)
		w.WriteHeader(http.StatusBadGateway)
		return nil
	}

	// For testing what gets passed on by caddy to upstream
	if m.mockUpstreamHost != "" {
		m.logger.Warn(
			"Mocking upstream",
			zap.String("listenerUpstream", upstream),
			zap.String("mockUpstream", m.mockUpstreamHost),
			zap.Bool("isDebugCase", enableExtraDebugLogs),
		)
		upstream = m.mockUpstreamHost
	}

	if hub != nil {
		hub.ConfigureScope(func(scope *sentry.Scope) {
			scope.SetTag("UpstreamHost", upstream)
		})
	}

	// Put properties on context for use in reverse proxy upstreams handler
	ctx = context.WithValue(ctx, DevxKey, DevxRequestData{
		ProjectID:    projectID,
		ServiceType:  serviceType,
		UpstreamHost: upstream,
		CustomDomain: customDomain,
		Hub:          hub,
	})

	// Keep going to reverse proxy which will call our
	// upstreams module to get the upstream address
	return h.ServeHTTP(w, r.WithContext(ctx))
}

const (
	ALLOWED_METHODS = "POST, GET, PATCH, PUT, OPTIONS, DELETE"
	ALLOWED_HEADERS = "Content-Type, Authorization, X-Authorization, X-Dbtn-Authorization, X-Dbtn-Webapp-Version, X-Request-Id"
	// STAINLESS_HEADERS = "X-Stainless-Arch, X-Stainless-Lang, X-Stainless-Os, X-Stainless-Package-Version, X-Stainless-Runtime, X-Stainless-Runtime-Version"
)

func setCorsHeaders(w http.ResponseWriter, origin string) {
	header := w.Header()
	header.Set("Access-Control-Allow-Origin", origin)
	header.Set("Access-Control-Allow-Methods", ALLOWED_METHODS)
	header.Set("Access-Control-Allow-Headers", ALLOWED_HEADERS)
	header.Set("Access-Control-Allow-Credentials", "true")

	foundVary := slices.Contains(header.Values("Vary"), "Origin")
	if !foundVary {
		header.Add("Vary", "Origin")
	}
}

// Interface guards
var (
	_ caddy.Module                = (*DevxMiddlewareModule)(nil)
	_ caddy.Provisioner           = (*DevxMiddlewareModule)(nil)
	_ caddy.Validator             = (*DevxMiddlewareModule)(nil)
	_ caddy.CleanerUpper          = (*DevxMiddlewareModule)(nil)
	_ caddyhttp.MiddlewareHandler = (*DevxMiddlewareModule)(nil)
	_ caddyfile.Unmarshaler       = (*DevxMiddlewareModule)(nil)
)
