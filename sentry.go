package dataappdnsservice

import (
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/getsentry/sentry-go"
)

type SentryDestructor struct {
}

func (SentryDestructor) Destruct() error {
	// Flush buffered events before the program terminates.
	sentry.Flush(2 * time.Second)
	return nil
}

var _ caddy.Destructor = SentryDestructor{}

func initSentry() error {
	_, _, err := usagePool.LoadOrNew("sentryInit", func() (caddy.Destructor, error) {
		// Set up sentry
		err := sentry.Init(sentry.ClientOptions{
			Dsn: SENTRY_DSN,
			// Set TracesSampleRate to 1.0 to capture 100%
			// of transactions for performance monitoring.
			// We recommend adjusting this value in production,
			TracesSampleRate: 1.0,
		})
		if err != nil {
			return nil, err
		}
		return SentryDestructor{}, nil
	})
	return err
}
