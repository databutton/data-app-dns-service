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

const (
	projectsListenerUsagePoolKey = "projectsListener"
	sentryInitUsagePoolKey       = "sentryInit"
)

type SentryDestructor struct {
}

func (SentryDestructor) Destruct() error {
	// Flush buffered events before the program terminates.
	sentry.Flush(2 * time.Second)
	return nil
}

var _ caddy.Destructor = SentryDestructor{}

// Configure Sentry once even if called multiple times,
// and flush when usagePool has been decremented.
func initSentry(logger *zap.Logger) error {
	_, loaded, err := usagePool.LoadOrNew(sentryInitUsagePoolKey, func() (caddy.Destructor, error) {
		err := sentry.Init(sentry.ClientOptions{
			Dsn:              SENTRY_DSN,
			TracesSampleRate: SENTRY_TRACES_SAMPLE_RATE,
		})
		if err != nil {
			return nil, err
		}
		return SentryDestructor{}, nil
	})
	logger.Info("initSentry completed",
		zap.Bool("loaded", loaded),
		zap.Error(err),
	)
	return err
}

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
	d.logger.Debug("Provision called")

	// Set up sentry (happens only once)
	err := initSentry(d.logger)
	if err != nil {
		d.logger.Error("initSentry returned error", zap.Error(err))
		return err
	}
	d.usageKeys = append(d.usageKeys, sentryInitUsagePoolKey)

	// Initialize the firestore cache (launches goroutine only once)
	startTime := time.Now()
	const collection = "projects"
	listener, listenerLoaded, err := usagePool.LoadOrNew(projectsListenerUsagePoolKey, func() (caddy.Destructor, error) {
		d.logger.Info("Creating project listener")

		// FIXME: Create a logger not associated with the caddy module instance
		logger := d.logger.With(zap.String("context", "projectsListener"))

		listener, err := NewProjectListener(collection, logger)
		if err != nil {
			return listener, err
		}

		d.logger.Info("Starting project listener goroutine")
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

	d.logger.Debug("Provision done")
	return nil
}

// Validate implements caddy.Validator.
//
// Honestly I'm not sure if there's any benefit
// to doing things here instead of in Provision.
func (d *DevxUpstreams) Validate() error {
	d.logger.Debug("Validate called")
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
		du.logger.Warn("Errors during shutdown", zap.Error(err))
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
		d.logger.Debug(
			"Got upstream",
			zap.String("upstream", upstream),
			zap.String("projectID", projectID),
			zap.String("serviceType", serviceType),
		)
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

	sentry.WithScope(func(scope *sentry.Scope) {
		scope.SetLevel(sentry.LevelError)
		scope.SetTags(map[string]string{
			"project_id":   projectID,
			"service_type": serviceType,
		})
		sentry.CaptureException(err)
	})
	d.logger.Error(
		"Failed to get upstream",
		zap.String("projectID", projectID),
		zap.String("serviceType", serviceType),
	)
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
