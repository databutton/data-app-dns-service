package devxupstreamer

import (
	"net/http"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"

	"github.com/databutton/data-app-dns-service/caddy-modules/devxmiddleware"
	"github.com/databutton/data-app-dns-service/pkg/storelistener"
)

type DevxUpstreamsModule struct {
	hub      *sentry.Hub
	logger   *zap.Logger
	listener *storelistener.Listener
}

func init() {
	// Let Caddy know about this module
	caddy.RegisterModule(DevxUpstreamsModule{})
}

// CaddyModule returns the Caddy module information.
func (DevxUpstreamsModule) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "http.reverse_proxy.upstreams.devx",
		New: func() caddy.Module {
			return new(DevxUpstreamsModule)
		},
	}
}

func (d *DevxUpstreamsModule) Provision(ctx caddy.Context) error {
	// Get logger for the context of this caddy module instance
	d.logger = ctx.Logger(d)
	d.logger.Info("Provision called")
	return nil
}

// Cleanup implements caddy.CleanerUpper.
func (d *DevxUpstreamsModule) Cleanup() error {
	return nil
}

// GetUpstreams implements reverseproxy.UpstreamSource.
//
// This is what's called for every request.
// It needs to be threadsafe and fast!
func (d *DevxUpstreamsModule) GetUpstreams(r *http.Request) ([]*reverseproxy.Upstream, error) {
	ctx := r.Context()
	data, ok := ctx.Value(devxmiddleware.DevxKey).(devxmiddleware.DevxRequestData)
	if !ok {
		d.logger.Error("Middleware request data is missing!")
		return nil, ErrNoMiddlewareDataFound
	}

	// If no upstream was selected log and return error,
	// we should never get here anymore because devxmiddleware returns early
	if data.UpstreamHost == "" {
		err := upstreamMissingError(data)
		d.logger.Error(
			"Failed to get upstream",
			zap.String("projectID", data.ProjectID),
			zap.String("serviceType", data.ServiceType),
			zap.String("customDomain", data.CustomDomain),
			zap.Error(err),
		)
		return nil, err
	}

	// Always use https here
	return []*reverseproxy.Upstream{
		{
			Dial: data.UpstreamHost,
		},
	}, nil
}

// This should happen rarely, report as much as possible to track why
func upstreamMissingError(data devxmiddleware.DevxRequestData) error {
	switch {
	case data.ProjectID == "":
		// TODO: Maybe return badrequest for this in middleware
		return ErrProjectHeaderMissing
	case data.ServiceType == "":
		// TODO: Maybe return badrequest for this in middleware
		return ErrServiceHeaderMissing
	case data.CustomDomain != "":
		// TODO: Maybe return not found for this in middleware
		return ErrCustomDomainNotFound
	default:
		// TODO: Maybe return internal server error for this in middleware
		return ErrUpstreamNotFound
	}
}

// func dumpUpstreamMissingErrorToSentry(hub *sentry.Hub, r *http.Request, err error, projectID, serviceType, customDomain string) {
// 	// Add some request context for the error
// 	hub.ConfigureScope(func(scope *sentry.Scope) {
// 		scope.SetLevel(sentry.LevelError)
//
// 		// This seems to set the url to something missing the project part in the middle
// 		scope.SetRequest(r)
//
// 		// TODO: This doesn't seem to be available
// 		scope.SetTag("transaction_id", r.Header.Get("X-Request-Id"))
//
// 		scope.SetTag("hasBearer", strconv.FormatBool(strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ")))
// 		scope.SetTag("hasCookie", strconv.FormatBool(r.Header.Get("Cookie") != ""))
// 	})
// 	hub.CaptureException(err)
// }

// UnmarshalCaddyfile implements caddyfile.Unmarshaler
func (m *DevxUpstreamsModule) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	return nil
}

// Interface guards
var (
	_ caddy.Provisioner           = (*DevxUpstreamsModule)(nil)
	_ caddy.CleanerUpper          = (*DevxUpstreamsModule)(nil)
	_ reverseproxy.UpstreamSource = (*DevxUpstreamsModule)(nil)
	_ caddyfile.Unmarshaler       = (*DevxUpstreamsModule)(nil)
)
