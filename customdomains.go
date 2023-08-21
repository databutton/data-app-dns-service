package dataappdnsservice

import (
	"fmt"
)

// TODO: This should be a cached firestore lookup, for the moment we just hardcode it
func getProjectIdForCustomDomain(customBaseUrl string) (string, error) {
	switch customBaseUrl {
	case "custom.dbtn.app":
		return "9c089241-c851-4351-8f0c-7bfe994d8e87", nil
	}
	return "", fmt.Errorf("missing projectId for custom domain %s", customBaseUrl)
}
