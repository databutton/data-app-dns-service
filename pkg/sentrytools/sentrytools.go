package sentrytools

import (
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
)

var initOnce sync.Once

// Configure Sentry once even if called multiple times
func InitSentry() error {
	var err error
	initOnce.Do(func() {
		err = sentry.Init(sentry.ClientOptions{
			Dsn:              SENTRY_DSN,
			Release:          os.Getenv("DATA_APP_DNS_RELEASE"),
			TracesSampleRate: SENTRY_TRACES_SAMPLE_RATE,
		})
	})
	return err
}

func DumpDebugInfoToSentry(hub *sentry.Hub, r *http.Request, err error) {
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
