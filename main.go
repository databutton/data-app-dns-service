package dataappdnsservice

import (
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
)

// Used to coordinate initialization of dependencies only once
var usagePool = caddy.NewUsagePool()

const projectsListenerUsagePoolKey = "projectsListener"

type DevxUpstreams struct {
	logger    *zap.Logger
	listener  *ProjectListener
	usageKeys []string
	closers   []io.Closer
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
	d.usageKeys = append(d.usageKeys, sentryInitUsagePoolKey)

	// Initialize the firestore cache (launches goroutine only once)
	startTime := time.Now()
	const collection = "projects"
	listener, listenerLoaded, err := usagePool.LoadOrNew(projectsListenerUsagePoolKey, func() (caddy.Destructor, error) {
		listener := NewProjectListener(collection)
		go listener.RunWithRestarts()
		return listener, nil
	})
	if err != nil {
		d.logger.Error(
			"Error loading listener",
			zap.Bool("loaded", listenerLoaded),
			zap.Error(err),
		)
		return err
	}
	d.listener = listener.(*ProjectListener)
	d.usageKeys = append(d.usageKeys, projectsListenerUsagePoolKey)
	d.logger.Info("Launched projects listener",
		zap.Bool("loaded", listenerLoaded),
		zap.Duration("loadTime", time.Since(startTime)),
		zap.Int("projectCount", d.listener.Count()),
	)

	// Wait until the listener has fetched all projects at least once,
	// if this has been called before it should return instantly.
	err = d.listener.WaitOnFirstSync(ctx.Context)
	if err != nil {
		d.logger.Error("listener.WaitOnFirstSync: %s", zap.Error(err))
		return err
	}

	return nil
}

// Validate implements caddy.Validator.
//
// Honestly I'm not sure if there's any benefit
// to doing things here instead of in Provision.
func (*DevxUpstreams) Validate() error {
	return nil
}

// Cleanup implements caddy.CleanerUpper.
//
// Decrements usagePool counters which eventually
// leads to their destructors being called.
func (du *DevxUpstreams) Cleanup() error {
	var allErrors []error
	for _, key := range du.usageKeys {
		// Catch panic from Delete in case we have a bug and decrement too many times
		err := DontPanic(func() error {
			deleted, err := usagePool.Delete(key)
			du.logger.Info("Decremented usage counter",
				zap.String("key", key),
				zap.Bool("deleted", deleted),
				zap.Error(err),
			)
			return err
		})
		allErrors = append(allErrors, err)
	}

	// Capture shutdown errors to sentry, just so we know if it happens
	err := errors.Join(allErrors...)
	if err != nil {
		sentry.CaptureException(err)
	}
	return err
}

// GetUpstreams implements reverseproxy.UpstreamSource.
//
// This is what's called for every request.
// It needs to be threadsafe and fast!
func (d *DevxUpstreams) GetUpstreams(r *http.Request) ([]*reverseproxy.Upstream, error) {
	projectID := r.Header.Get("X-Databutton-Project-Id")
	serviceType := r.Header.Get("X-Databutton-Service-Type")
	upstream := d.listener.LookupUpUrl(projectID, serviceType)
	if upstream != "" {
		return []*reverseproxy.Upstream{
			{
				Dial: upstream,
			},
		}, nil
	}

	var err error
	switch {
	case projectID == "":
		err = errors.New("X-Databutton-Project-Id header missing")
	case serviceType == "":
		err = errors.New("X-Databutton-Service-Type header missing")
	default:
		err = errors.New("Could not find upstream url")
	}

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

// Interface guards
var (
	_ caddy.Provisioner           = (*DevxUpstreams)(nil)
	_ caddy.Validator             = (*DevxUpstreams)(nil)
	_ caddy.CleanerUpper          = (*DevxUpstreams)(nil)
	_ reverseproxy.UpstreamSource = (*DevxUpstreams)(nil)
	_ caddyfile.Unmarshaler       = (*DevxUpstreams)(nil)
)
