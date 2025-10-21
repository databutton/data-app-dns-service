// Middleware module to populate request context with data from listener and control some headers including CORS.
//
// https://caddyserver.com/docs/extending-caddy
package devxmiddleware

import (
	"context"
	"encoding/json"
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
	// LOG_EXTRA_FOR_PROJECT_ID = "29b6d950-947f-44e3-a7b4-da91732bcbf2"
	LOG_EXTRA_FOR_PROJECT_ID = "tight-turbulent-carillon-sns4"
)

var EnableExtraDebugLogsForAllRequests = os.Getenv("ENABLE_EXTRA_DEBUG_LOGS_FOR_ALL_REQUESTS") == "true"

var prodxRootDomains = []string{
	".riff.works",
	".rifftools.com",
	".databutton.app",
}

var workspaceDomains = []string{
	"https://riff.new",
	"https://useriff.ai",
	"https://riff.hot",
	"https://databutton.com",
}

type LookerUper interface {
	ProjectIdForDomain(domain string) string
	UpstreamForProject(ctx context.Context, projectID, serviceType string) string
	UsernameForProject(projectID, serviceType string) string
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
	m.listener.(GetInfraer).GetInfra().DebugDumpDomains()
}

func (m *DevxMiddlewareModule) DebugDumpServices() {
	m.listener.(GetInfraer).GetInfra().DebugDumpServices()
}

type InfraResponse struct {
	Source  string            `json:"source"`
	Message string            `json:"message"`
	Context map[string]string `json:"context,omitempty"`
}

func (m *DevxMiddlewareModule) writeErrorResponse(
	w http.ResponseWriter,
	status int,
	message string,
	context map[string]string,
) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	err := json.NewEncoder(w).Encode(InfraResponse{
		Source:  "databutton-proxy",
		Message: message,
		Context: context,
	})
	if err != nil {
		m.logger.Error("Failed to write error response", zap.Error(err))
	}
	return nil
}

// ServeHTTP implements caddyhttp.MiddlewareHandler
func (m *DevxMiddlewareModule) ServeHTTP(w http.ResponseWriter, r *http.Request, h caddyhttp.Handler) error {
	ctx := r.Context()

	// TODO: Maybe add zap and sentry middleware to entire caddy stack and not just devx parts
	// Clone a sentry hub for this request
	var hub *sentry.Hub
	// if m.debug {
	hub = sentry.CurrentHub().Clone()
	hub.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetRequest(r)
	})
	ctx = sentry.SetHubOnContext(ctx, hub)
	//}

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
	enableExtraDebugLogs := (projectID == LOG_EXTRA_FOR_PROJECT_ID && LOG_EXTRA_FOR_PROJECT_ID != "") || EnableExtraDebugLogsForAllRequests
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
			return m.writeErrorResponse(w, http.StatusBadRequest, "Invalid Origin header", map[string]string{
				"Origin":      originHeader,
				"ProjectID":   projectID,
				"ServiceType": serviceType,
			})
		}
		// TODO: This exception is not really safe, localhost shouldn't be allowed, see comment below on localhost and vite
		if originUrl.Scheme != "https" && !strings.HasPrefix(originUrl.Host, "localhost") {
			m.logger.Error(
				"Insecure Origin",
				zap.String("Origin", originHeader),
				zap.Bool("isDebugCase", enableExtraDebugLogs),
			)
			return m.writeErrorResponse(w, http.StatusBadRequest, "Insecure Origin header", map[string]string{
				"Origin":      originHeader,
				"ProjectID":   projectID,
				"ServiceType": serviceType,
			})
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

	// TODO: Make the non-_projects cases work, add for example /_apps/users/{username}/apps/{appname}/
	//
	// API may be called on URL:
	// - appx             databutton.com            /_projects/{projectID}/dbtn/{serviceType}/
	// - appx             api.databutton.com        /_projects/{projectID}/dbtn/{serviceType}/
	// - databutton.app   username.databutton.app   /appname/api/* -> /_projects/{projectID}/dbtn/{serviceType}/app/routes/*
	// - custom           custom.com                /api/* -> /_projects/{projectID}/dbtn/{serviceType}/app/routes/*

	if projectID == "" {
		// Can this happen? When?

		// This means the URL is not _projects/...
		// Get project id from domain lookup
		if strings.HasSuffix(originHost, ".databutton.app") {
			// TODO: Case: username.databutton.app/appname/api/...
			// originHost = username.databutton.app
			// appnameHeader := r.Header.Get("X-Databutton-Appname")
			// projectID = m.listener.LookupUpProjectIdForDatabuttonAppDomain(
			// 	strings.TrimSuffix(originHost, ".databutton.app"),
			// 	appnameHeader,
			// )
		} else {
			// TODO: Case: custom.com/api/...
			// originHost = custom.com
			projectID = m.listener.ProjectIdForDomain(originHost)
		}

		if projectID == "" {
			// No project specified, can't find domain
			m.logger.Error(
				"No projectId and domain lookup failed",
				zap.String("originHost", originHost),
			)
			return m.writeErrorResponse(w, http.StatusServiceUnavailable, "No projectId to identify the app in URL, and domain lookup failed", map[string]string{
				"Origin": originHeader,
			})
		}

		// Accept if project found for this domain and header and origin matches
		corsOrigin = originHeader
		r.Header.Set("X-Databutton-Project-Id", projectID)

		// Emulate what we already do in caddy for regular /_projects/.../prodx requests
		r.Header.Set(
			"X-Original-Path",
			makeOriginalPath(projectID, serviceType, r.URL.Path),
		)

		r.Header.Set("X-Dbtn-Proxy-Case", "no-project-id")

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
		isProdx := serviceType == "prodx"

		matched := false
		if originHeader == "" {
			// Same-domain requests, requests from backends, etc, never set cors
			r.Header.Set("X-Dbtn-Proxy-Case", "no-origin")
			corsOrigin = ""
			matched = true
			if enableExtraDebugLogs {
				m.logger.Info("devxmiddleware: matched no origin")
			}
		} else if isProdx && originHeader == "https://databutton.com" {
			// Special case kept for backwards compatibility. May be irrelevant, but test before removing.
			// Don't remember what case this is, does it happen that non-devx apps are called from databutton.com origin?
			r.Header.Set("X-Dbtn-Proxy-Case", "databutton-com-prodx")
			corsOrigin = originHeader
			matched = true
		} else if isProdx {
			// TODO: Maybe a regex would be better here
			prodxRootDomainsIndex := slices.IndexFunc(
				prodxRootDomains,
				func(suffix string) bool {
					return strings.HasSuffix(originHost, suffix)
				})
			if prodxRootDomainsIndex >= 0 {
				prodxRootDomain := prodxRootDomains[prodxRootDomainsIndex]

				// This header is just a debugging tool, not currently used by anything
				// E.g. user-riff-works, user-rifftools-com, user-databutton-app
				r.Header.Set("X-Dbtn-Proxy-Case", "user"+strings.ReplaceAll(prodxRootDomain, ".", "-"))

				// Compare origin url username to app owner username
				originUsername := strings.TrimSuffix(originHost, prodxRootDomain)
				ownerUsername := m.listener.UsernameForProject(projectID, serviceType)
				if originUsername == ownerUsername {
					// Only set cors if username is owner of projectID
					corsOrigin = originHeader
					matched = true
				} else {
					// Error handling case
					msg := "The app being called is not owned by the username in the domain it's being called from"
					m.logger.Error(msg,
						zap.String("projectID", projectID),
						zap.String("serviceType", serviceType),
						zap.String("originHost", originHost),
						zap.String("originHostSuffix", prodxRootDomain),
						zap.String("originUsername", originUsername),
						zap.String("ownerUsername", ownerUsername),
						zap.Bool("isDebugCase", enableExtraDebugLogs),
					)
					if r.Method == "OPTIONS" {
						return m.writeErrorResponse(w, http.StatusNoContent, msg, map[string]string{
							"Origin":      originHeader,
							"ProjectID":   projectID,
							"ServiceType": serviceType,
						})
					}
				}
			}
		} else if isDevx {
			// The riff dev workspace behind misc domains
			for _, workspaceDomain := range workspaceDomains {
				if originHeader == workspaceDomain {
					r.Header.Set("X-Dbtn-Proxy-Case", "workspace")
					corsOrigin = originHeader
					matched = true
					if enableExtraDebugLogs {
						m.logger.Info("devxmiddleware: matched workspace")
					}
					break
				}
			}

			if !matched && strings.HasPrefix(originHost, "localhost") {
				// Call comes from localhost, i.e. running webapp locally
				r.Header.Set("X-Dbtn-Proxy-Case", "localhost-for-dev")
				// TODO: This would not be necessary if vite proxies
				//       were set up to set Origin to https://databutton.com
				corsOrigin = originHeader
				matched = true
				if enableExtraDebugLogs {
					m.logger.Info("devxmiddleware: matched localhost")
				}
			}
		}

		// This is checked for both devx and prodx, is that a bug or is there a corner case?
		// May be irrelevant for devx, but test before removing.
		if !matched {
			// Call comes from outside our infra
			r.Header.Set("X-Dbtn-Proxy-Case", "customdomain")

			// Get project id from domain lookup (for cors check only!)
			originDomainProjectID := m.listener.ProjectIdForDomain(originHost)
			if originDomainProjectID == projectID {
				// Set cors if project found for this domain and header and origin matches
				customDomain = originHost
				corsOrigin = originHeader
				matched = true

				if enableExtraDebugLogs {
					m.logger.Info("devxmiddleware: matched projectid for domain")
				}
			} else {
				// Don't set cors if different or no project found for this domain
				msg := "Origin domain is not associated with the app being called"
				if enableExtraDebugLogs {
					m.logger.Error(
						msg,
						zap.String("originHost", originHost),
						zap.String("projectID", projectID),
						zap.String("serviceType", serviceType),
						zap.Bool("isDebugCase", enableExtraDebugLogs),
					)
				}
				if r.Method == "OPTIONS" {
					return m.writeErrorResponse(w, http.StatusNoContent, msg, map[string]string{
						"Origin":      originHeader,
						"ProjectID":   projectID,
						"ServiceType": serviceType,
					})
				}
			}
		}

		if !matched && enableExtraDebugLogs {
			m.logger.Info("devxmiddleware: not matched")
		}
	}

	// Set cors headers allowing this origin
	if corsOrigin != "" {
		setCorsHeaders(w, corsOrigin)
	}

	// Short-circuit options requests
	if r.Method == "OPTIONS" {
		// Cases that get here should be regular no-cors-needed requests so not returning any message
		w.WriteHeader(http.StatusNoContent)
		return nil
	}

	// Look up upstream URL
	upstream := m.listener.UpstreamForProject(ctx, projectID, serviceType)
	if upstream == "" {
		m.logger.Warn("devxmiddleware: upstream is blank",
			zap.String("projectID", projectID),
			zap.String("serviceType", serviceType),
			zap.String("originHost", originHost),
			zap.Bool("isDebugCase", enableExtraDebugLogs),
		)
		return m.writeErrorResponse(
			w,
			http.StatusServiceUnavailable,
			"Found no backend service registered for this app",
			map[string]string{
				"Origin":      originHeader,
				"ProjectID":   projectID,
				"ServiceType": serviceType,
			},
		)
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
