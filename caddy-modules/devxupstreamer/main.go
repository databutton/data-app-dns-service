package devxupstreamer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
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

	"github.com/databutton/data-app-dns-service/pkg/dontpanic"
	"github.com/databutton/data-app-dns-service/pkg/storelistener"
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

type SentryDestructor struct{}

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
	hub              *sentry.Hub
	logger           *zap.Logger
	listener         *storelistener.Listener
	usageKeys        []string
	mockUpstreamHost string
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

	// Initialize the firestore cache (launches goroutines only once)
	startTime := time.Now()
	listener, listenerLoaded, err := usagePool.LoadOrNew(projectsListenerUsagePoolKey, func() (caddy.Destructor, error) {
		d.logger.Info("Initializing firestore listeners")

		// Should we create a logger not associated with the caddy module instance? Seems to work fine.
		logger := d.logger.With(zap.String("context", "projectsListener"))

		// This use of context is a bit hacky, some refactoring can probably make the code cleaner.
		// The listener will call cancel when Caddy Destructs it.
		// That will cancel the listenerCtx which the runListener goroutines are running with.
		listenerCtx, listenerCancel := context.WithCancel(context.Background())
		listener, err := storelistener.NewFirestoreListener(
			listenerCancel,
			logger,
			storelistener.Config{GcpProject: GCP_PROJECT},
		)
		if err != nil {
			return listener, err
		}

		runListener := func(collection string, initWg *sync.WaitGroup) {
			defer listenerCancel()

			hub := d.hub.Clone()
			ctx := sentry.SetHubOnContext(listenerCtx, hub)

			// This should run forever or until canceled...
			err := listener.RunListener(ctx, initWg, collection)

			// Graceful cancellation
			if errors.Is(ctx.Err(), context.Canceled) {
				return
			}

			// Panic in a goroutine kills the program abruptly, lets do that
			// unless we were canceled, such that the service restarts.
			hub.CaptureException(err)
			sentry.Flush(2 * time.Second)
			panic(fmt.Errorf("run failed with error: %v", err))
		}

		appbutlersInitWg := new(sync.WaitGroup)
		appbutlersInitWg.Add(1)
		go runListener(storelistener.CollectionAppbutlers, appbutlersInitWg)

		domainsInitWg := new(sync.WaitGroup)
		domainsInitWg.Add(1)
		go runListener(storelistener.CollectionDomains, domainsInitWg)

		domainsInitWg.Wait()
		appbutlersInitWg.Wait()

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
	d.listener = listener.(*storelistener.Listener)
	d.usageKeys = append(d.usageKeys, projectsListenerUsagePoolKey)

	mockUrl := os.Getenv("MOCK_DEVX_UPSTREAM_URL")
	if mockUrl != "" {
		u, err := url.Parse(mockUrl)
		if err != nil {
			d.logger.Error("failed to parse mock url", zap.String("mockurl", mockUrl))
			return err
		}
		d.mockUpstreamHost = u.Host
		d.logger.Warn("MOCKING ALL UPSTREAM HOSTS!",
			zap.String("mockUpstreamHost", d.mockUpstreamHost),
		)
	}

	d.logger.Info("Provision done",
		zap.Duration("loadTime", time.Since(startTime)),
		zap.Int("upstreamsCount", d.listener.CountUpstreams()),
		zap.Int("domainsCount", d.listener.CountDomains()),
	)

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
		err := dontpanic.DontPanic(func() error {
			deleted, err := usagePool.Delete(key)
			d.logger.Info("Decremented usage counter",
				zap.String("key", key),
				zap.Bool("deleted", deleted),
				zap.Error(err),
			)
			return err
		})
		if err != nil {
			allErrors = append(allErrors, err)
		}
	}

	// Capture shutdown errors to sentry, just so we know if it happens.
	if hub := d.hub; hub != nil {
		for _, err := range allErrors {
			hub.CaptureException(err)
		}
	}

	if len(allErrors) > 0 {
		d.logger.Warn("Errors during shutdown, see sentry", zap.Errors("errors", allErrors))
		return fmt.Errorf("%v", allErrors)
	}

	return nil
}

// GetUpstreams implements reverseproxy.UpstreamSource.
//
// This is what's called for every request.
// It needs to be threadsafe and fast!
func (d *DevxUpstreams) GetUpstreams(r *http.Request) ([]*reverseproxy.Upstream, error) {
	// Headers we'll add to the request on success
	headers := make([][2]string, 0, 10)

	// Databutton app variables set by caddy from url if possible
	projectID := r.Header.Get("X-Databutton-Project-Id")
	serviceType := r.Header.Get("X-Databutton-Service-Type")

	// Get origin (without scheme) and check if it has a custom domain
	// TODO: This is intended for CORS middleware
	// originProjectID := ""
	// origin := r.Header.Get("Origin")
	// originUrl, err := url.Parse(origin)
	// if err == nil && originUrl != nil {
	// 	originProjectID = d.listener.LookupUpProjectIdFromDomain(originUrl.Host)
	// }

	// Streamlit custom domain apps have this header set
	customBaseUrl := r.Header.Get("X-Dbtn-Baseurl")
	if customBaseUrl != "" && serviceType == "prodx" {
		// Use domain project, ignore header
		customDomainProjectID := d.listener.LookupUpProjectIdFromDomain(customBaseUrl)
		if customDomainProjectID != "" {
			// Use this id (should perhaps validate for mismatch)
			projectID = customDomainProjectID

			// Emulate what we already do for regular prodx deploys
			databuttonAppBasePath := fmt.Sprintf("/_projects/%s/dbtn/prodx", projectID)
			originalPath := fmt.Sprintf("%s%s", databuttonAppBasePath, r.URL.Path)

			// Set these headers before returning success
			headers = append(headers, [2]string{"X-Databutton-Project-Id", projectID})
			headers = append(headers, [2]string{"X-Original-Path", originalPath})

			// Continue to look up upstreams as normal
		}
	}

	// Find upstream appbutler url (this does not have the scheme included)
	upstream := d.listener.LookupUpUrl(projectID, serviceType)

	// For testing what gets passed on by caddy to upstream
	if d.mockUpstreamHost != "" {
		d.logger.Warn("Mocking upstream", zap.String("listenerUpstream", upstream), zap.String("mockUpstream", d.mockUpstreamHost))
		upstream = d.mockUpstreamHost
	}

	// If no upstream was selected log and return error
	if upstream == "" {
		err := upstreamMissingError(projectID, serviceType, customBaseUrl)
		d.logger.Warn(
			"Failed to get upstream",
			zap.String("projectID", projectID),
			zap.String("serviceType", serviceType),
			zap.String("customBaseUrl", customBaseUrl),
			zap.Error(err),
		)
		return nil, err

	}

	// Set headers and return
	for _, kv := range headers {
		r.Header.Set(kv[0], kv[1])
	}
	return []*reverseproxy.Upstream{
		{
			Dial: upstream,
		},
	}, nil
}

// FIXME: Refactor GetUpstreams into a middleware module that puts data on the context,
// so GetUpstreams can just fetch from context and return,
// and set cors headers on response in the middleware module
func setCorsHeaders(logger *zap.Logger, origin string, header http.Header) {
	defer func() {
		r := recover()
		if err, ok := r.(error); ok {
			logger.Error("EXPERIMENTAL setCorsHeaders FAILED", zap.String("origin", origin), zap.Error(err))
		} else {
			logger.Error("EXPERIMENTAL setCorsHeaders FAILED", zap.String("origin", origin), zap.Any("recovered", r))
		}
	}()
	const ALLOWED_METHODS = "POST, GET, PATCH, PUT, OPTIONS, DELETE"
	const ALLOWED_HEADERS = "Content-Type, X-Request-Id, Authorization, X-Dbtn-Webapp-Version, X-Dbtn-baseurl, X-Authorization"
	header.Set("Access-Control-Allow-Origin", origin)
	header.Set("Access-Control-Allow-Methods", ALLOWED_METHODS)
	header.Set("Access-Control-Allow-Credentials", "true") // TODO: "false"?
	header.Set("Access-Control-Allow-Headers", ALLOWED_HEADERS)
	header.Add("Vary", "Origin")
}

// This should happen rarely, report as much as possible to track why
func upstreamMissingError(projectID, serviceType, customBaseUrl string) error {
	switch {
	case projectID == "":
		return ErrProjectHeaderMissing
	case serviceType == "":
		return ErrServiceHeaderMissing
	default:
		return ErrUpstreamNotFound
	}
}

func dumpUpstreamMissingErrorToSentry(hub *sentry.Hub, r *http.Request, err error, projectID, serviceType, customBaseUrl string) {
	// Clone hub for thread safety, this is in the scope of a single request
	hub = hub.Clone()

	// Add some request context for the error
	hub.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetLevel(sentry.LevelError)

		// This seems to set the url to something missing the project part in the middle
		scope.SetRequest(r)

		// TODO: This doesn't seem to be available
		scope.SetTag("transaction_id", r.Header.Get("X-Request-ID"))

		scope.SetTag("hasBearer", strconv.FormatBool(strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ")))
		scope.SetTag("hasCookie", strconv.FormatBool(r.Header.Get("Cookie") != ""))

		scope.SetTag("projectId", projectID)
		scope.SetTag("serviceType", serviceType)
	})
	hub.CaptureException(err)
}

func dumpDebugInfoToSentry(hub *sentry.Hub, r *http.Request, err error) {
	// Clone hub for thread safety, this is in the scope of a single request
	hub = hub.Clone()

	// Doesn't matter if this is a bit slow
	defer hub.Flush(2 * time.Second)

	// Add some request context for the error
	hub.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetLevel(sentry.LevelDebug)

		// This seems to set the url to something missing the project part in the middle
		scope.SetRequest(r)
	})

	hub.CaptureException(err)
}

// Interface guards
var (
	_ caddy.Provisioner           = (*DevxUpstreams)(nil)
	_ caddy.Validator             = (*DevxUpstreams)(nil)
	_ caddy.CleanerUpper          = (*DevxUpstreams)(nil)
	_ reverseproxy.UpstreamSource = (*DevxUpstreams)(nil)
	_ caddyfile.Unmarshaler       = (*DevxUpstreams)(nil)
)
