// A firestore listener process to run in the background.
//
// https://caddyserver.com/docs/extending-caddy
package devxlistener

import (
	"context"
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

// Implement caddy.Provisioner interface
// Provision is the caddy module entrypoint
func (m *ListenerModule) Provision(ctx caddy.Context) error {
	// Logger associated with this caddy module
	m.logger = ctx.Logger()
	m.logger.Info("devxlistener: Provision")

	// Store a context that is cancelled when the module cleans up
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

	// Create and start listener background process
	initWg := new(sync.WaitGroup)
	initWg.Add(1)
	firstSyncDone := sync.OnceFunc(func() { initWg.Done() })
	l, err := m.startListener(firstSyncDone)
	if err != nil {
		m.cancel()
		return err
	}
	// Block caddy startup until the listener has loaded data once
	initWg.Wait()

	m.logger.Info("devxlistener: Provision done after initial listener data loading",
		zap.Int("upstreamsCount", l.CountUpstreams()),
		zap.Int("domainsCount", l.CountDomains()),
	)

	// This will be accessed by devxmiddleware before Start runs
	m.listener = l

	return nil
}

func (m *ListenerModule) startListener(firstSyncDone func()) (*storelistener.Listener, error) {
	m.logger.Info("devxlistener: Starting firestore listener background process")

	// TODO: Should this use m.ctx as root context?
	ctx := sentry.SetHubOnContext(context.Background(), m.hub.Clone())

	listener, err := storelistener.NewFirestoreListener(
		ctx,
		m.logger,
	)
	if err != nil {
		return nil, err
	}

	// This launches a goroutine that should run forever or until canceled...
	listener.Start(firstSyncDone)

	return listener, nil
}

// Start implements caddy.App
func (m *ListenerModule) Start() error {
	m.logger.Info("devxlistener: Start")
	return nil
}

// Stop implements caddy.App
func (m *ListenerModule) Stop() error {
	m.logger.Info("devxlistener: Stop")
	m.cancel()
	// Flush buffered events before the program terminates.
	sentry.Flush(2 * time.Second)
	return nil
}

// Validate implements caddy.Validator, called after Provision
func (m *ListenerModule) Validate() error {
	m.logger.Info("devxlistener: Validate")
	return nil
}

// Cleanup implements caddy.CleanerUpper
func (m *ListenerModule) Cleanup() error {
	m.logger.Info("devxlistener: Cleanup")
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
