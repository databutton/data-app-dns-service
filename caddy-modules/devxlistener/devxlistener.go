// A firestore listener process to run in the background.
//
// https://caddyserver.com/docs/extending-caddy
package devxlistener

import (
	"context"
	"time"

	"github.com/caddyserver/caddy/v2"
	"go.uber.org/zap"
)

type ListenerModule struct {
	// Configuration fields, if any
	MyParam string `json:"myparam,omitempty"`
	// Number  int    `json:"number,omitempty"`

	// Internal state
	ctx    context.Context
	cancel context.CancelFunc
	logger *zap.Logger
	// mutex  sync.Mutex
	// data   []string // example of some internal state
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
	m.logger.Info("LISTENER: Provision", zap.String("myparam", m.MyParam))
	m.ctx, m.cancel = context.WithCancel(ctx.Context)
	return nil
}

// Start implements caddy.App
func (m *ListenerModule) Start() error {
	m.logger.Info("LISTENER: Start")
	go m.backgroundProcess(m.ctx)
	for i := range [10]int{} {
		m.logger.Info("LISTENER: Start", zap.Int("i", i))
		time.Sleep(time.Second)
	}
	m.logger.Info("LISTENER: Start exiting")
	return nil
}

// Stop implements caddy.App
func (m *ListenerModule) Stop() error {
	m.logger.Info("LISTENER: Stop")
	m.cancel()
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
	return nil
}

func (m *ListenerModule) backgroundProcess(ctx context.Context) {
	m.logger.Info("LISTENER: Running background process")

	// TODO: Firestore listener runs here!

	// Example background process
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.logger.Info("LISTENER: tick")
		}
	}
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
