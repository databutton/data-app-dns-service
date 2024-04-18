// Middleware module to populate request context with data from listener and control some headers including CORS.
//
// https://caddyserver.com/docs/extending-caddy
package devxmiddleware

import (
	"context"
	"net/http"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/databutton/data-app-dns-service/caddy-modules/devxlistener"
	"go.uber.org/zap"
)

type MiddlewareModule struct {
	// Configuration fields, if any
	MyParam string `json:"myparam,omitempty"`

	// Internal state
	logger *zap.Logger
}

// Register module with caddy
func init() {
	caddy.RegisterModule(new(MiddlewareModule))
	httpcaddyfile.RegisterHandlerDirective("devxmiddleware", parseCaddyfile)
}

// Implement caddy.Module interface
func (m *MiddlewareModule) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.devxmiddleware",
		New: func() caddy.Module { return new(MiddlewareModule) },
	}
}

func (m *MiddlewareModule) Provision(ctx caddy.Context) error {
	m.logger = ctx.Logger().With(zap.String("myparam", m.MyParam))
	m.logger.Info("MIDDLEWARE: Provision")

	app, err := ctx.App("devxlistener")
	if err != nil {
		return err
	}
	listener := app.(*devxlistener.ListenerModule)
	m.logger.Info("MIDDLEWARE: Got listener", zap.String("param", listener.MyParam))

	return nil
}

// Validate implements caddy.Validator
// Called after Provision.
func (m *MiddlewareModule) Validate() error {
	m.logger.Info("MIDDLEWARE: Validate")
	return nil
}

// Cleanup implements caddy.CleanerUpper
func (m *MiddlewareModule) Cleanup() error {
	m.logger.Info("MIDDLEWARE: Cleanup")
	return nil
}

var (
	ProjectIdKey caddy.CtxKey = "devx.projectid"
	UpstreamKey  caddy.CtxKey = "devx.upstream"
)

// ServeHTTP implements caddyhttp.MiddlewareHandler
func (m *MiddlewareModule) ServeHTTP(w http.ResponseWriter, r *http.Request, h caddyhttp.Handler) error {
	m.logger.Info("MIDDLEWARE: ServeHTTP")
	ctx := r.Context()
	ctx = context.WithValue(ctx, ProjectIdKey, m.MyParam)
	ctx = context.WithValue(ctx, UpstreamKey, "fakeupstream")
	return h.ServeHTTP(w, r.WithContext(ctx))
}

// From https://caddyserver.com/docs/extending-caddy#complete-example
// UnmarshalCaddyfile implements caddyfile.Unmarshaler
func (m *MiddlewareModule) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next() // consume directive name

	// require an argument
	if !d.NextArg() {
		return d.ArgErr()
	}

	// store the argument
	m.MyParam = d.Val()
	return nil
}

// parseCaddyfile unmarshals tokens from h into a new MiddlewareModule.
func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	m := new(MiddlewareModule)
	err := m.UnmarshalCaddyfile(h.Dispenser)
	return m, err
}

// Interface guards
var (
	_ caddy.Module                = (*MiddlewareModule)(nil)
	_ caddy.Provisioner           = (*MiddlewareModule)(nil)
	_ caddy.Validator             = (*MiddlewareModule)(nil)
	_ caddy.CleanerUpper          = (*MiddlewareModule)(nil)
	_ caddyhttp.MiddlewareHandler = (*MiddlewareModule)(nil)
	_ caddyfile.Unmarshaler       = (*MiddlewareModule)(nil)
)
