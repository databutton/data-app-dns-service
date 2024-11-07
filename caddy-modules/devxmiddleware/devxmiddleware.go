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
	"strings"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/databutton/data-app-dns-service/caddy-modules/devxlistener"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
)

type LookerUper interface {
	LookupUpProjectIdFromDomain(domain string) string
	LookupUpstreamHost(projectID, serviceType string) string
	LookupUsername(projectID, serviceType string) string
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
			m.logger.Warn("MOCKING ALL UPSTREAM HOSTS!",
				zap.String("mockUpstreamHost", m.mockUpstreamHost),
			)
		}
	}
}

// Validate implements caddy.Validator
// Called after Provision.
func (m *DevxMiddlewareModule) Validate() error {
	m.logger.Info("MIDDLEWARE: Validate")
	return nil
}

// Cleanup implements caddy.CleanerUpper
func (m *DevxMiddlewareModule) Cleanup() error {
	m.logger.Info("MIDDLEWARE: Cleanup")
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
	// NB! This is for streamlit custom domains, where r.URL.Path
	// will not include the /_projects/.../prodx part, but may be
	// suitable in the future as well if we do API calls via custom domains
	return fmt.Sprintf("/_projects/%s/dbtn/%s%s", projectID, serviceType, path)
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

	// Origin should be e.g.
	// https://databutton.com
	// https://username.databutton.app
	// https://customdomain.com
	// but it is not always present!
	originHeader := r.Header.Get("Origin")

	// Validate origin _if present_
	var originHost string
	if originHeader != "" {
		originUrl, err := url.Parse(originHeader)
		if err != nil {
			m.logger.Error(fmt.Sprintf("Invalid origin header '%s'", originHeader))
			w.WriteHeader(http.StatusBadRequest)
			return nil
		}
		originHost = originUrl.Host
	}

	// This will be set to originHeader in cases where cross-domain request is valid from origin
	var corsOrigin string

	// Databutton app variables set by caddy from url if possible
	projectID := r.Header.Get("X-Databutton-Project-Id")
	serviceType := r.Header.Get("X-Databutton-Service-Type")
	var customDomain string

	// Origin shows where app is served from:
	// - devx streamlit       databutton.com/_projects/PID/dbtn/devx/*
	// - devx beyond          databutton.com/_projects/PID/dbtn/devx/*
	// - prodx streamlit /v/  databutton.com/v/*
	// - prodx beyond /u/     databutton.com/u/*
	// - prodx beyond         user.databutton.com|app
	// - custom streamlit     custom.com
	// - custom beyond        custom.com
	//
	// Temporary cases we need to keep working:
	// - devx streamlit       databutton.com/_projects/PID/dbtn/devx/*   same as new
	// - prodx streamlit /v/  databutton.com/v/*  soon deprecated
	// - prodx beyond /u/     databutton.com/u/*  soon deprecated
	// - custom streamlit     custom.com  has header
	//
	// Future cases we care about:
	// - devx beyond          databutton.com/_projects/PID/dbtn/devx/*
	// - prodx beyond         user.databutton.com|app
	// - custom beyond        custom.com

	if projectID == "" {
		// This means the URL is not _projects/... and is currently only happening for
		// streamlit apps on custom domains because then all of prodx is behind the domain.

		// Deprecated: Streamlit custom domain apps have this header set in cloudfront
		streamlitCustomDomain := r.Header.Get("X-Dbtn-Baseurl")
		if streamlitCustomDomain != "" && serviceType == "prodx" {
			r.Header.Set("X-Dbtn-Proxy-Case", "streamlit-customdomain")

			// This will happen
			// if streamlitCustomDomain != originHost {
			// 	m.logger.Warn("Origin and X-Dbtn-BaseUrl differs",
			// 		zap.String("OriginHost", originHost),
			// 		zap.String("DbtnBaseUrl", streamlitCustomDomain),
			// 	)
			// }

			// Get project id from domain lookup
			projectID = m.listener.LookupUpProjectIdFromDomain(streamlitCustomDomain)

			// Accept if project found for this domain and header and origin matches
			if projectID != "" {
				corsOrigin = originHeader
				customDomain = streamlitCustomDomain
				r.Header.Set("X-Databutton-Project-Id", projectID)

				// Emulate what we already do for regular prodx deploys in caddy
				// NB! This is for streamlit custom domains, where r.URL.Path
				// will not include the /_projects/.../prodx part
				r.Header.Set(
					"X-Original-Path",
					makeOriginalPath(projectID, serviceType, r.URL.Path),
				)
			}
		}

		if projectID == "" {
			// No project specified, can't find domain, just return 404 here right away
			m.logger.Error(
				"No projectId and domain lookup failed",
				zap.String("originHost", originHost),
				zap.String("streamlitCustomDomain", streamlitCustomDomain),
				zap.String("customDomain", customDomain),
			)
			w.WriteHeader(http.StatusBadRequest)
			return nil
		}

	} else if projectID == "8de60c46-e25f-45fe-8df7-b5dd8b7f3b4d" {
		// TODO: Add config for allowed origins to apps, special case for now
		// extraAllowedOrigins := m.listener.GetAllowedOriginsForProject(originProjectID)
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
		} else if originHeader == "https://databutton.com" {
			// App served from databutton.com, devx and legacy prodx cases
			r.Header.Set("X-Dbtn-Proxy-Case", "databutton-origin")
			corsOrigin = originHeader
		} else if strings.HasSuffix(originHost, ".databutton.app") {
			// New style hosting at per-user subdomains
			r.Header.Set("X-Dbtn-Proxy-Case", "user-subdomain")
			username := strings.TrimSuffix(originHost, ".databutton.app")

			// Look up username of owner of project
			ownerUsername := m.listener.LookupUsername(projectID, serviceType)

			// Only set cors if username is owner of projectID
			if username == ownerUsername {
				corsOrigin = originHeader
			} else {
				// Attempt at accessing another project, just return 401 right away
				m.logger.Error(
					"Attempt at accessing another users project",
					zap.String("originHost", originHost),
					zap.String("ownerUsername", ownerUsername),
					zap.String("username", username),
					zap.String("projectID", projectID),
				)
				w.WriteHeader(http.StatusUnauthorized)
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
			originProjectID := m.listener.LookupUpProjectIdFromDomain(originHost)

			// Accept if project found for this domain and header and origin matches
			if originProjectID == projectID {
				corsOrigin = originHeader
				customDomain = originHost
			} else {
				// Attempt at accessing another project, just return 401 right away
				m.logger.Error(
					"Attempt at accessing project not associated with domain",
					zap.String("originHost", originHost),
					zap.String("originProjectID", originProjectID),
					zap.String("projectID", projectID),
				)
				w.WriteHeader(http.StatusUnauthorized)
				return nil
			}
		}
	}

	// Set cors headers allowing this origin
	if corsOrigin != "" {
		setCorsHeaders(m.logger, w, corsOrigin)
	}

	// Short-circuit options requests
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusNoContent)
		return nil
	}

	// Look up upstream URL
	upstream := m.listener.LookupUpstreamHost(projectID, serviceType)
	if upstream == "" {
		m.logger.Warn("MIDDLEWARE: BLANK UPSTREAM")
	}

	// For testing what gets passed on by caddy to upstream
	if m.mockUpstreamHost != "" {
		m.logger.Warn(
			"Mocking upstream",
			zap.String("listenerUpstream", upstream),
			zap.String("mockUpstream", m.mockUpstreamHost),
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
	ALLOWED_HEADERS = "Content-Type, X-Request-Id, Authorization, X-Dbtn-Webapp-Version, X-Dbtn-Baseurl, X-Authorization"
)

func setCorsHeaders(logger *zap.Logger, w http.ResponseWriter, origin string) {
	header := w.Header()
	header.Set("Access-Control-Allow-Origin", origin)
	header.Set("Access-Control-Allow-Methods", ALLOWED_METHODS)
	header.Set("Access-Control-Allow-Headers", ALLOWED_HEADERS)
	header.Set("Access-Control-Allow-Credentials", "true") // TODO: "false"?

	foundVary := false
	for _, v := range header.Values("Vary") {
		if v == "Origin" {
			foundVary = true
			break
		}
	}
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
