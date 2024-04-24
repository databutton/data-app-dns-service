// A firestore listener process to run in the background.
//
// https://caddyserver.com/docs/extending-caddy
package devxlistener

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/databutton/data-app-dns-service/pkg/sentrytools"
	"github.com/databutton/data-app-dns-service/pkg/storelistener"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
)

type ListenerModule struct {
	// Internal state
	ctx                context.Context
	cancel             context.CancelFunc
	logger             *zap.Logger
	hub                *sentry.Hub
	listener           *storelistener.Listener
	waitForInitialLoad func()
}

func Get(ctx caddy.Context) (*storelistener.Listener, error) {
	app, err := ctx.App("devxlistener")
	if err != nil {
		return nil, err
	}
	m := app.(*ListenerModule)
	return m.listener, nil
}

// Register module with caddy
func init() {
	caddy.RegisterModule(new(ListenerModule))
}

// Implement caddy.Module interface
func (m *ListenerModule) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "devxlistener",
		New: func() caddy.Module { return new(ListenerModule) },
	}
}

func (m *ListenerModule) Provision(ctx caddy.Context) error {
	m.logger = ctx.Logger()
	m.logger.Info("LISTENER: Provision")
	m.ctx, m.cancel = context.WithCancel(ctx.Context)

	// Make sure sentry is initialized
	if err := sentrytools.InitSentry(); err != nil {
		return err
	}

	// Clone a sentry hub for this module instance
	m.hub = sentry.CurrentHub().Clone()
	m.hub.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTag("provisioningStartedAt", time.Now().UTC().Format(time.RFC3339))
	})

	// Create and start listener
	l, err := m.startListener()
	if err != nil {
		return err
	}
	m.listener = l
	return nil
}

func (m *ListenerModule) startListener() (*storelistener.Listener, error) {
	m.logger.Info("Initializing firestore listeners")

	// Should we create a logger not associated with the caddy module instance? Seems to work fine.
	logger := m.logger.With(zap.String("context", "projectsListener"))

	// This use of context is a bit hacky, some refactoring can probably make the code cleaner.
	// The listener will call cancel when Caddy Destructs it.
	// That will cancel the listenerCtx which the runListener goroutines are running with.
	listenerCtx, listenerCancel := context.WithCancel(context.Background())
	listener, err := storelistener.NewFirestoreListener(
		listenerCancel,
		logger,
	)
	if err != nil {
		return nil, err
	}

	runListener := func(initWg *sync.WaitGroup, newProjection func() storelistener.Projection) {
		defer listenerCancel()

		hub := m.hub.Clone()
		ctx := sentry.SetHubOnContext(listenerCtx, hub)

		// This should run forever or until canceled...
		err := listener.RunListener(ctx, initWg, newProjection)

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
	go runListener(appbutlersInitWg, storelistener.NewAppbutlersProjection)

	domainsInitWg := new(sync.WaitGroup)
	domainsInitWg.Add(1)
	go runListener(domainsInitWg, storelistener.NewDomainsProjection)

	m.waitForInitialLoad = func() {
		domainsInitWg.Wait()
		appbutlersInitWg.Wait()

		m.logger.Info("Initial listener data load complete",
			zap.Int("upstreamsCount", m.listener.CountUpstreams()),
			zap.Int("domainsCount", m.listener.CountDomains()),
		)
	}

	return listener, nil
}

// Start implements caddy.App
func (m *ListenerModule) Start() error {
	m.logger.Info("LISTENER: Start")
	if m.waitForInitialLoad == nil {
		m.logger.Error("LISTENER: Failed!")
		return fmt.Errorf("Module has not been provisioned")
	}

	// Block caddy startup until the listener has loaded data once
	m.waitForInitialLoad()

	m.logger.Info("LISTENER: Start exiting")
	return nil
}

// Stop implements caddy.App
func (m *ListenerModule) Stop() error {
	m.logger.Info("LISTENER: Stop")
	m.cancel()

	// Flush buffered events before the program terminates.
	sentry.Flush(2 * time.Second)

	return nil
}

// Validate implements caddy.Validator, called after Provision
func (m *ListenerModule) Validate() error {
	m.logger.Info("LISTENER: Validate")
	return nil
}

// Cleanup implements caddy.CleanerUpper
func (m *ListenerModule) Cleanup() error {
	m.logger.Info("LISTENER: Cleanup")
	m.cancel()

	// Flush buffered events before the program terminates.
	sentry.Flush(2 * time.Second)

	return nil
}

// Interface guards
var (
	_ caddy.App          = (*ListenerModule)(nil)
	_ caddy.Module       = (*ListenerModule)(nil)
	_ caddy.Provisioner  = (*ListenerModule)(nil)
	_ caddy.Validator    = (*ListenerModule)(nil)
	_ caddy.CleanerUpper = (*ListenerModule)(nil)
	// _ caddyfile.Unmarshaler       = (*ListenerModule)(nil)
)
