package tracking

import (
	"context"
	"time"

	"github.com/mixpanel/mixpanel-go"
	segment "github.com/segmentio/analytics-go"
	"go.uber.org/zap"
)

// TrackMixpanelEvent sends a Mixpanel track event via HTTP API
func TrackMixpanelEvent(mixpanelClient *mixpanel.ApiClient, logger *zap.Logger, distinctID, eventName string, props map[string]any) {
	if mixpanelClient == nil {
		return
	}

	// Use independent short timeout to avoid cancellation when request scope ends
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := mixpanelClient.Track(ctx, []*mixpanel.Event{
		mixpanelClient.NewEvent(eventName, distinctID, props),
	})
	if err != nil {
		logger.Error("Mixpanel track error",
			zap.String("event", eventName),
			zap.String("distinctId", distinctID),
			zap.Error(err))
	}
}

func TrackSegmentEvent(segmentClient segment.Client, logger *zap.Logger, distinctID, eventName string, props map[string]any) {
	if segmentClient == nil {
		return
	}

	properties := segment.NewProperties()
	for k, v := range props {
		properties.Set(k, v)
	}
	err := segmentClient.Enqueue(segment.Track{
		UserId:     distinctID,
		Event:      eventName,
		Properties: properties,
	})
	if err != nil {
		logger.Error("Segment track error",
			zap.String("event", eventName),
			zap.String("distinctId", distinctID),
			zap.Error(err))
	}
}
