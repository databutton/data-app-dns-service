package dataappdnsservice

const (
	GCP_PROJECT = "databutton"

	GCP_PROJECT_HASH = "gvjcjtpafa"

	SENTRY_DSN = "https://aceadcbf56f14ef9a7fe76b0db5d7351@o1000232.ingest.sentry.io/4504735637176320"

	SENTRY_TRACES_SAMPLE_RATE = 0.05
)

// TODO: After migrating to appbutlers we can get rid of this
var REGION_LOOKUP_MAP = map[string]string{
	"europe-west1":      "ew",
	"europe-north1":     "lz",
	"europe-southwest1": "no",
	"europe-west9":      "od",
	"europe-west4":      "ez",
	"europe-west8":      "oc",
	"europe-west12":     "og",
	"europe-west2":      "nw",
	"europe-west3":      "ey",
	"europe-west6":      "oa",
	"europe-central2":   "lm",
}
