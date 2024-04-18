package devxlistener

import (
	"context"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"go.uber.org/zap"
)

type ListenerModule struct {
	// Configuration fields, if any
	// MyField string `json:"my_field,omitempty"`
	// Number  int    `json:"number,omitempty"`

	// Internal state
	ctx    context.Context
	cancel context.CancelFunc
	logger *zap.Logger
	// mutex  sync.Mutex
	// data   []string // example of some internal state
}

// Global variable for access by other modules
var (
	instance *ListenerModule
	once     sync.Once
)

// Implement caddy.Module interface
func (m *ListenerModule) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "devxlistener",
		New: func() caddy.Module { return new(ListenerModule) },
	}
}

func (m *ListenerModule) Provision(ctx caddy.Context) error {
	m.logger.Info("LISTENER: Provision")
	m.logger = ctx.Logger()
	m.ctx, m.cancel = context.WithCancel(ctx.Context)

	// Provision your module here
	instance = m // Set the global instance

	return nil
}

// Start implements caddy.App
func (m *ListenerModule) Start() error {
	m.logger.Info("LISTENER: Start")
	go m.backgroundProcess(m.ctx)
	return nil
}

// Stop implements caddy.App
func (m *ListenerModule) Stop() error {
	m.logger.Info("LISTENER: Stop")
	m.cancel()
	return nil
}

// Validate implements caddy.Validator
// Called after Provision.
func (m *ListenerModule) Validate() error {
	m.logger.Info("LISTENER: Validate")
	return nil
}

// Cleanup implements caddy.CleanerUpper
func (m *ListenerModule) Cleanup() error {
	m.logger.Info("LISTENER: Cleanup")
	m.cancel()
	return nil
}

func (m *ListenerModule) backgroundProcess(ctx context.Context) {
	// Example background process
	m.logger.Info("LISTENER: Running background process")
	time.Sleep(3 * time.Second)
	m.logger.Info("LISTENER: Done running background process")
	// for {
	// 	select {
	// 	// Implement your background logic
	// 	// Update m.data accordingly
	// 	}
	// }
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

// Notes:
// https://caddyserver.com/docs/extending-caddy
// "All modules in the http.handlers namespace implement the caddyhttp.MiddlewareHandler
