package devxupstreamer

import "errors"

var (
	ErrNoMiddlewareDataFound = errors.New("missing middleware request data")
	ErrProjectHeaderMissing  = errors.New("missing header X-Databutton-Project-Id")
	ErrServiceHeaderMissing  = errors.New("missing header X-Databutton-Service-Type")
	ErrCustomDomainNotFound  = errors.New("could not find custom domain upstream")
	ErrUpstreamNotFound      = errors.New("could not find upstream url")
	ErrInvalidRegion         = errors.New("invalid region")
	ErrChaos                 = errors.New("chaos test for testing")
)
