package dataappdnsservice

import (
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/getsentry/sentry-go"
)

const sentryUsageKey = "sentryInit"

type SentryDestructor struct {
}

func (SentryDestructor) Destruct() error {
	// Flush buffered events before the program terminates.
	sentry.Flush(2 * time.Second)
	return nil
}

var _ caddy.Destructor = SentryDestructor{}

func initSentry() error {
	_, _, err := usagePool.LoadOrNew(sentryUsageKey, func() (caddy.Destructor, error) {
		// Set up sentry
		err := sentry.Init(sentry.ClientOptions{
			Dsn:              SENTRY_DSN,
			TracesSampleRate: SENTRY_TRACES_SAMPLE_RATE,
		})
		if err != nil {
			return nil, err
		}
		return SentryDestructor{}, nil
	})
	return err
}
