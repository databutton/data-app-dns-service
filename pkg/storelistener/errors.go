package storelistener

import (
	"errors"
)

var (
	ErrCircuitBroken              = errors.New("Circuit broken")
	ErrSimulatedFailure           = errors.New("Simulated failure")
	ErrSnapshotListenerFailed     = errors.New("Snapshot listener failed too many times")
	ErrUnalivingService           = errors.New("Unaliving service")
	ErrMissingServiceIdentityData = errors.New("Insufficient data to identify service purpose")
	ErrMissingUpstreamData        = errors.New("Insufficient data to construct upstream url")
)
