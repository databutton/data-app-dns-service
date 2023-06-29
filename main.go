package dataappdnsservice

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
)

// Errors
var (
	ErrProjectHeaderMissing = errors.New("missing header X-Databutton-Project-Id")
	ErrServiceHeaderMissing = errors.New("missing header X-Databutton-Service-Type")
	ErrUpstreamNotFound     = errors.New("could not find upstream url")
	ErrInvalidRegion        = errors.New("invalid region")
	ErrChaos                = errors.New("chaos test for testing")
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
			Release:          os.Getenv("DATA_APP_DNS_RELEASE"),
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
	hub       *sentry.Hub
	logger    *zap.Logger
	listener  *ProjectListener
	usageKeys []string
}

func init() {
	// This will allow Caddy to register signal handlers for graceful shutdown if possible,
	// although this doesn't cover SIGPIPE 13 seen in prod...
	caddy.TrapSignals()

	// Install our custom sigpipe handler
	trapSignal13()

	// Let Caddy know about this module
	caddy.RegisterModule(DevxUpstreams{})
}

// Send signal to the current process
func signalSelf(sig os.Signal) error {
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		return err
	}
	return p.Signal(sig)
}

// Signal 13 observed in prod because we're piping logs to jq in a bash script.
// Trying to just translate it to sigint here.
func trapSignal13() {
	go func() {
		shutdown := make(chan os.Signal, 1)
		signal.Notify(shutdown, syscall.SIGPIPE)
		for {
			<-shutdown
			_ = signalSelf(os.Interrupt)
		}
	}()
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
	// We must implement this function, but should we do something here?
	return nil
}

func (d *DevxUpstreams) Provision(ctx caddy.Context) error {
	// Get logger for the context of this caddy module instance
	d.logger = ctx.Logger(d)
	d.logger.Info("Provision called")

	// Set up sentry (happens only once)
	err := initSentry(d.logger)
	if err != nil {
		d.logger.Error("initSentry returned error", zap.Error(err))
		return err
	}
	d.usageKeys = append(d.usageKeys, sentryInitUsagePoolKey)

	// Clone a sentry hub for this module instance
	d.hub = sentry.CurrentHub().Clone()
	d.hub.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTag("provisioningStartedAt", time.Now().UTC().Format(time.RFC3339))
	})

	// Initialize the firestore cache (launches goroutine only once)
	startTime := time.Now()
	listener, listenerLoaded, err := usagePool.LoadOrNew(projectsListenerUsagePoolKey, func() (caddy.Destructor, error) {
		d.logger.Info("Creating project listener")

		// FIXME: Create a logger not associated with the caddy module instance
		logger := d.logger.With(zap.String("context", "projectsListener"))

		listener, err := NewProjectListener(logger)
		if err != nil {
			return listener, err
		}

		logger.Info("Starting appbutler listener goroutine")
		startTime := time.Now()
		appbutlersInitWg := new(sync.WaitGroup)
		appbutlersInitWg.Add(1)
		go func() {
			ctx, cancel := context.WithCancel(listener.ctx)
			defer cancel()

			ctx = sentry.SetHubOnContext(ctx, d.hub.Clone())
			hub := sentry.GetHubFromContext(ctx)

			// This should run forever
			err := listener.RunWithoutCrashing(ctx, collectionAppbutlers, appbutlersInitWg)
			if err != nil {
				hub.CaptureException(err)
			}

			// Panic in a goroutine kills the program abruptly, do that
			// unless we were canceled, such that the service restarts.
			// Perhaps there is a nicer way to shut down caddy, we'll see in prod...
			if !errors.Is(ctx.Err(), context.Canceled) {
				hub.CaptureException(ctx.Err())
				sentry.Flush(2 * time.Second)
				panic(fmt.Errorf("run failed with error: %v", err))
			}
		}()
		appbutlersInitWg.Wait()
		initialSyncTime := time.Since(startTime)
		logger.Info("Appbutlers listener first sync completed",
			zap.Duration("loadTime", initialSyncTime),
			zap.Int("upstreamsCount", listener.Count()),
		)

		logger.Info("Starting project listener goroutine")
		startTime = time.Now()
		projectsInitWg := new(sync.WaitGroup)
		projectsInitWg.Add(1)
		go func() {
			ctx, cancel := context.WithCancel(listener.ctx)
			defer cancel()

			ctx = sentry.SetHubOnContext(ctx, d.hub.Clone())
			hub := sentry.GetHubFromContext(ctx)

			// This should run forever
			err := listener.RunWithoutCrashing(ctx, collectionProjects, projectsInitWg)
			if err != nil {
				hub.CaptureException(err)
			}

			// Panic in a goroutine kills the program abruptly, do that
			// unless we were canceled, such that the service restarts.
			// Perhaps there is a nicer way to shut down caddy, we'll see in prod...
			if !errors.Is(ctx.Err(), context.Canceled) {
				hub.CaptureException(ctx.Err())
				panic(fmt.Errorf("run failed with error: %v", err))
			}
		}()
		projectsInitWg.Wait()
		initialSyncTime = time.Since(startTime)
		logger.Info("Launched projects listener",
			zap.Duration("loadTime", initialSyncTime),
			zap.Int("upstreamsCount", listener.Count()),
		)

		return listener, nil
	})
	if err != nil {
		d.logger.Error(
			"Error loading listener",
			zap.Bool("loaded", listenerLoaded),
			zap.Error(err),
		)
		d.hub.CaptureException(err)
		return err
	}
	d.listener = listener.(*ProjectListener)
	d.usageKeys = append(d.usageKeys, projectsListenerUsagePoolKey)

	d.logger.Info("Provision done",
		zap.Duration("loadTime", time.Since(startTime)),
		zap.Int("upstreamsCount", d.listener.Count()),
	)

	// Dump all map contents for inspection
	// d.listener.Dump()

	return nil
}

// Validate implements caddy.Validator.
//
// Honestly I'm not sure if there's any benefit
// to doing things here instead of in Provision.
func (d *DevxUpstreams) Validate() error {
	d.logger.Info("Validate called")
	return nil
}

// Cleanup implements caddy.CleanerUpper.
//
// Decrements usagePool counters which eventually
// leads to their destructors being called.
func (d *DevxUpstreams) Cleanup() error {

	var allErrors []error
	for _, key := range d.usageKeys {
		// Catch panic from Delete in case we have a bug and decrement too many times
		err := DontPanic(func() error {
			deleted, err := usagePool.Delete(key)
			d.logger.Info("Decremented usage counter",
				zap.String("key", key),
				zap.Bool("deleted", deleted),
				zap.Error(err),
			)
			return err
		})
		allErrors = append(allErrors, err)
	}

	// Capture shutdown errors to sentry, just so we know if it happens.
	if hub := d.hub; hub != nil {
		for _, err := range allErrors {
			hub.CaptureException(err)
		}
	}
	if len(allErrors) > 0 {
		d.logger.Warn("Errors during shutdown, see sentry")
		return fmt.Errorf("%v", allErrors)
	}
	return nil
}

// GetUpstreams implements reverseproxy.UpstreamSource.
//
// This is what's called for every request.
// It needs to be threadsafe and fast!
func (d *DevxUpstreams) GetUpstreams(r *http.Request) ([]*reverseproxy.Upstream, error) {
	projectID := r.Header.Get("X-Databutton-Project-Id")
	serviceType := r.Header.Get("X-Databutton-Service-Type")

	upstream := d.listener.LookupUpUrl(projectID, serviceType)

	if upstream == "" {
		// TODO: If this stays a little flaky we could try to fetch
		//   the appbutler document directly here, but lets delay that
		//   until we can remove the project listener to simplify here
		return nil, d.upstreamMissing(r, projectID, serviceType)
	}

	// Dropping this since it's so noisy
	// d.logger.Debug(
	// 	"Got upstream",
	// 	zap.String("upstream", upstream),
	// 	zap.String("projectID", projectID),
	// 	zap.String("serviceType", serviceType),
	// )

	return []*reverseproxy.Upstream{
		{
			Dial: upstream,
		},
	}, nil
}

// This should happen rarely, report as much as possible to track why
func (d *DevxUpstreams) upstreamMissing(r *http.Request, projectID, serviceType string) error {
	var err error
	switch {
	case projectID == "":
		err = ErrProjectHeaderMissing
	case serviceType == "":
		err = ErrServiceHeaderMissing
	default:
		err = ErrUpstreamNotFound
	}

	// Clone hub for thread safety, this is in the scope of a single request
	hub := d.hub.Clone()

	// Add some request context for the error
	hub.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetLevel(sentry.LevelError)

		// This seems to set the url to something missing the project part in the middle
		scope.SetRequest(r)

		// TODO: This doesn't seem to be available
		scope.SetTag("transaction_id", r.Header.Get("X-Request-ID"))
		scope.SetTag("hasBearer", strconv.FormatBool(strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ")))

		scope.SetTag("projectId", projectID)
		scope.SetTag("serviceType", serviceType)
	})
	hub.CaptureException(err)

	d.logger.Error(
		"Failed to get upstream",
		zap.String("projectID", projectID),
		zap.String("serviceType", serviceType),
	)

	return err
}

// Interface guards
var (
	_ caddy.Provisioner           = (*DevxUpstreams)(nil)
	_ caddy.Validator             = (*DevxUpstreams)(nil)
	_ caddy.CleanerUpper          = (*DevxUpstreams)(nil)
	_ reverseproxy.UpstreamSource = (*DevxUpstreams)(nil)
	_ caddyfile.Unmarshaler       = (*DevxUpstreams)(nil)
)
