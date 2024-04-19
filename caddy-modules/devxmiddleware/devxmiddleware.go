// Middleware module to populate request context with data from listener and control some headers including CORS.
//
// https://caddyserver.com/docs/extending-caddy
package devxmiddleware

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
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

type DevxMiddlewareModule struct {
	// Configuration fields, if any
	InstanceTag string `json:"instance_tag,omitempty"`

	// Internal state
	logger           *zap.Logger
	listener         *storelistener.Listener // TODO: Nicer to store as narrower interface
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
	m.logger.Info("MIDDLEWARE: Provision")

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
	ProjectID     string
	ServiceType   string
	UpstreamHost  string
	CustomBaseUrl string
	Hub           *sentry.Hub
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

	// Origin should be e.g. https://username.databutton.app
	originHeader := r.Header.Get("Origin")

	// Validate origin
	originUrl, err := url.Parse(originHeader)
	if err != nil || originUrl == nil {
		m.logger.Error(fmt.Sprintf("Invalid origin header '%s'", originHeader))
		w.WriteHeader(http.StatusBadRequest)
		io.Copy(io.Discard, r.Body)
		return nil
	}
	originHost := originUrl.Host

	// Databutton app variables set by caddy from url if possible
	projectID := r.Header.Get("X-Databutton-Project-Id")
	serviceType := r.Header.Get("X-Databutton-Service-Type")

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

	// This will be set to origin in cases where request is valid from origin
	var corsOrigin string

	// Deprecated: Streamlit custom domain apps have this header set in cloudfront
	streamlitCustomDomain := r.Header.Get("X-Dbtn-Baseurl")
	if streamlitCustomDomain != "" && serviceType == "prodx" {
		r.Header.Set("X-Dbtn-Proxy-Case", "streamlit-customdomain")

		if streamlitCustomDomain != originHost {
			m.logger.Warn("Origin and X-Dbtn-BaseUrl differs",
				zap.String("OriginHost", originHost),
				zap.String("DbtnBaseUrl", streamlitCustomDomain),
			)
		}

		// Get project id from domain lookup
		projectID = m.listener.LookupUpProjectIdFromDomain(streamlitCustomDomain)

		// Accept if project found for this domain and header and origin matches
		if projectID != "" {
			corsOrigin = originHeader
			r.Header.Set("X-Databutton-Project-Id", projectID)
		} else {
			m.logger.Error("Blank projectID after custom domain lookup", zap.String("originHost", originHost))
		}

		// Emulate what we already do for regular prodx deploys
		r.Header.Set(
			"X-Original-Path",
			// NB! This is for streamlit custom domains,
			// where r.URL.Path will not include the
			// /_projects/.../prodx part
			makeOriginalPath(projectID, serviceType, r.URL.Path),
		)

	} else if originHeader == "https://databutton.com" {
		r.Header.Set("X-Dbtn-Proxy-Case", "databutton-origin")
		// Accept when served from databutton, devx and legacy prodx cases
		corsOrigin = originHeader
	} else if strings.HasSuffix(originHost, ".databutton.app") || strings.HasSuffix(originHost, ".databutton.com") {
		r.Header.Set("X-Dbtn-Proxy-Case", "user-subdomain")
		// FIXME: Move to .app, drop .com case
		username := strings.TrimSuffix(strings.TrimSuffix(originHost, ".databutton.app"), ".databutton.com")
		// FIXME: Only set if username is owner of projectID
		if username != "" {
			corsOrigin = originHeader
		}
	} else {
		r.Header.Set("X-Dbtn-Proxy-Case", "beyond-customdomain")
		// Must be custom domain

		// Get project id from domain lookup
		projectID = m.listener.LookupUpProjectIdFromDomain(originHost)

		// Accept if project found for this domain and header and origin matches
		if projectID != "" {
			corsOrigin = originHeader
			r.Header.Set("X-Databutton-Project-Id", projectID)
		} else {
			m.logger.Error("Blank projectID after custom domain lookup", zap.String("originHost", originHost))
		}

		// Emulate what we already do for regular prodx deploys
		// (This is different from the streamlit case)
		r.Header.Set("X-Original-Path", makeOriginalPath(projectID, serviceType, r.URL.Path))
	}

	if projectID == "" {
		m.logger.Error("Missing projectID after app lookups")
	}

	// Set cors headers allowing this origin
	if projectID != "" && corsOrigin != "" {
		setCorsHeaders(m.logger, w, corsOrigin)
	}

	// Short-circuit options requests
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusNoContent)
		io.Copy(io.Discard, r.Body)
		return nil
	}

	// Look up upstream URL
	upstream := m.listener.LookupUpUrl(projectID, serviceType)
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

	// Clone a sentry hub for this request
	hub := sentry.CurrentHub().Clone()
	hub.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTag("ProjectID", projectID)
		scope.SetTag("ServiceType", serviceType)
		scope.SetTag("Origin", originHeader)
		scope.SetTag("UpstreamHost", upstream)
		scope.SetTag("StreamlitCustomDomain", streamlitCustomDomain)
	})

	// Put properties on context for use in reverse proxy upstreams handler
	ctx = context.WithValue(ctx, DevxKey, DevxRequestData{
		ProjectID:     projectID,
		ServiceType:   serviceType,
		UpstreamHost:  upstream,
		CustomBaseUrl: streamlitCustomDomain,
		Hub:           hub,
	})

	// Keep going to reverse proxy which will call our
	// upstreams module to get the upstream address
	return h.ServeHTTP(w, r.WithContext(ctx))
}

const (
	ALLOWED_METHODS = "POST, GET, PATCH, PUT, OPTIONS, DELETE"
	ALLOWED_HEADERS = "Content-Type, X-Request-Id, Authorization, X-Dbtn-Webapp-Version, X-Dbtn-Baseurl, X-Authorization, X-FIXME-TESTING" // TODO: Remove X-FIXME-TESTING when observed
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
