package dataappdnsservice

import (
	"github.com/caddyserver/caddy/v2"
	"github.com/databutton/data-app-dns-service/pkg/sigtrap"

	// DO NOT REMOVE THESE IMPORTS!
	// They make sure the submodules are included when this parent module is compiled.
	_ "github.com/databutton/data-app-dns-service/caddy-modules/devxlistener"
	_ "github.com/databutton/data-app-dns-service/caddy-modules/devxmiddleware"
	_ "github.com/databutton/data-app-dns-service/caddy-modules/devxupstreamer"
)

func init() {
	// This will allow Caddy to register signal handlers for graceful shutdown if possible,
	// although this doesn't cover SIGPIPE 13 seen in prod...
	caddy.TrapSignals()

	// Install our custom sigpipe handler
	sigtrap.TrapSignal13()
}
