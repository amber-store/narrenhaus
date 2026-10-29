package remote

import (
	"net/http"
	"testing"
	"time"
)

// SetStallLimit shortens the stall limit of the default HTTP client for
// the Remotes made during the test.
func SetStallLimit(t testing.TB, limit time.Duration) {
	old := stallLimit
	stallLimit = limit
	t.Cleanup(func() { stallLimit = old })
}

// NewHTTPClient is the default HTTP client with a stall limit of its own.
func NewHTTPClient(limit time.Duration) *http.Client {
	return newHTTPClient(limit)
}
