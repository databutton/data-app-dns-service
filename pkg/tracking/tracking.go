package tracking

import (
	"context"
	"os"
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

func InitSegment() segment.Client {
	// Create a segment client
	segmentWriteKey := os.Getenv("SEGMENT_WRITE_KEY")
	if segmentWriteKey == "" {
		return nil
	}
	segmentClient, err := segment.NewWithConfig(segmentWriteKey, segment.Config{
		BatchSize: 100,
		Interval:  5 * time.Second,
	})
	if err != nil {
		return nil
	}
	return segmentClient
}

func TrackSegmentEvent(segmentClient segment.Client, logger *zap.Logger, distinctID, eventName string, props map[string]any) {
	if segmentClient == nil {
		return
	}

	var properties segment.Properties
	if len(props) > 0 {
		properties = segment.Properties(props)
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
