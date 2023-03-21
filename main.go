package dataappdnsservice

import (
	"errors"
	"net/http"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
)

// Used to coordinate initialization of dependencies only once
var usagePool = caddy.NewUsagePool()

const projectsListenerUsageKey = "projectsListener"

// Initialize and launch projects listener only once, and
// return the running instance instantly if it's already running
func GetOrLaunchProjectsListener() (*ProjectListener, error) {
	listener, loaded, err := usagePool.LoadOrNew(projectsListenerUsageKey, func() (caddy.Destructor, error) {
		listener := NewProjectListener("projects")
		go listener.RunWithRestarts()
		return listener, nil
	})

	if err != nil {
		return nil, err
	}

	// TODO: Log whether this happened
	_ = loaded
	// logger.Info(loaded)

	return listener.(*ProjectListener), nil
}

type DevxUpstreams struct {
	logger    *zap.Logger
	listener  *ProjectListener
	usageKeys []string
}

func init() {
	caddy.RegisterModule(DevxUpstreams{})
}

// CaddyModule returns the Caddy module information.
func (DevxUpstreams) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.reverse_proxy.upstreams.devx",
		New: func() caddy.Module { return new(DevxUpstreams) },
	}
}

// UnmarshalCaddyfile implements caddyfile.Unmarshaler.
func (devx *DevxUpstreams) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	return nil
}

func (d *DevxUpstreams) Provision(ctx caddy.Context) error {
	// Get logger for the context of this caddy module instance
	d.logger = ctx.Logger(d)

	// Set up sentry (happens only once)
	err := initSentry()
	if err != nil {
		d.logger.Error("sentry.Init: %s", zap.Error(err))
		return err
	}
	d.usageKeys = append(d.usageKeys, sentryUsageKey)

	// Initialize the firestore cache (launches goroutine only once)
	d.listener, err = GetOrLaunchProjectsListener()
	if err != nil {
		d.logger.Error("GetOrLaunchProjectsListener: %s", zap.Error(err))
		return err
	}
	d.usageKeys = append(d.usageKeys, projectsListenerUsageKey)

	// Wait until the listener has fetched all projects at least once,
	// if this has been called before it should return instantly.
	err = d.listener.WaitOnFirstSync(ctx.Context)
	if err != nil {
		d.logger.Error("listener.WaitOnFirstSync: %s", zap.Error(err))
		return err
	}

	return nil
}

// Validate implements caddy.Validator
func (*DevxUpstreams) Validate() error {
	return nil
}

// Cleanup implements caddy.CleanerUpper
func (du *DevxUpstreams) Cleanup() error {
	// FIXME: Catch panic and log it in case we decrement too many times

	var allErrors []error
	for _, key := range du.usageKeys {
		deleted, err := usagePool.Delete(key)
		if err != nil {
			allErrors = append(allErrors, err)
		}
		if deleted {
			du.logger.Sugar().Infof("Destructor for %s was run", key)
		}
	}
	return errors.Join(allErrors...)
}

func (d *DevxUpstreams) GetUpstreams(r *http.Request) ([]*reverseproxy.Upstream, error) {
	projectID := r.Header.Get("X-Databutton-Project-Id")
	serviceType := r.Header.Get("X-Databutton-Service-Type")

	if projectID == "" {
		return nil, errors.New("X-Databutton-Project-Id header missing")
	}

	project, ok := d.listener.LookupProject(projectID)
	if !ok {
		err := errors.New("Could not find project")
		sentry.ConfigureScope(func(scope *sentry.Scope) {
			scope.SetLevel(sentry.LevelError)
			scope.SetTags(map[string]string{
				"project_id":   projectID,
				"service_type": serviceType,
			})
			sentry.CaptureException(err)
		})
		return nil, err
	}

	var upstream string
	switch serviceType {
	case "devx":
		upstream = project.Devx.url
	case "prodx":
		upstream = project.Prodx.url
	default:
		err := errors.New("X-Databutton-Service-Type header missing or invalid")
		sentry.ConfigureScope(func(scope *sentry.Scope) {
			scope.SetLevel(sentry.LevelWarning)
			scope.SetTags(map[string]string{
				"project_id":   projectID,
				"service_type": serviceType,
			})
			sentry.CaptureException(err)
		})
		return nil, err
	}

	return []*reverseproxy.Upstream{
		{
			Dial: upstream,
		},
	}, nil
}

// Interface guards
var (
	_ caddy.Provisioner           = (*DevxUpstreams)(nil)
	_ caddy.Validator             = (*DevxUpstreams)(nil)
	_ caddy.CleanerUpper          = (*DevxUpstreams)(nil)
	_ reverseproxy.UpstreamSource = (*DevxUpstreams)(nil)
	_ caddyfile.Unmarshaler       = (*DevxUpstreams)(nil)
)
