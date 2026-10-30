package remote

import (
	"net/http"
	"testing"
	"time"
)

// The command runs on the defaults, and every other test sets limits of
// its own: these are the numbers of the design.
func TestDefaultLimits(t *testing.T) {
	r := New(Config{})
	defer r.Close()
	for _, c := range []struct {
		name      string
		got, want time.Duration
	}{
		{"a lookup", r.lookupTimeout, 10 * time.Second},
		{"a lookup's wait on a connection that was up", r.repeatAfter, 3 * time.Second},
		{"a dial", r.dialTimeout, 30 * time.Second},
		{"failing fast after a failed dial", r.failFast, 30 * time.Second},
	} {
		if c.got != c.want {
			t.Errorf("the limit of %s is %v by default, want %v", c.name, c.got, c.want)
		}
	}
	stall, ok := r.http.Transport.(*stallTransport)
	if !ok {
		t.Fatalf("the default HTTP client's transport is a %T, want one with the stall limit", r.http.Transport)
	}
	if stall.limit != 30*time.Second {
		t.Errorf("a bucket transfer may stall for %v by default, want 30s", stall.limit)
	}
	// Connections to the bucket are kept for as many transfers as the
	// pushes of both kinds make at once by default, 64 and 64.
	base, ok := stall.base.(*http.Transport)
	if !ok {
		t.Fatalf("the transport under the stall limit is a %T, want an *http.Transport", stall.base)
	}
	if base.MaxIdleConnsPerHost != 128 || base.MaxIdleConns != 128 {
		t.Errorf("%d idle connections are kept to a host and %d in all, want 128 and 128",
			base.MaxIdleConnsPerHost, base.MaxIdleConns)
	}
}

func TestRepeatAfterFollowsTheLookupLimit(t *testing.T) {
	for _, c := range []struct {
		name                string
		lookup, given, want time.Duration
	}{
		{"three tenths of a short limit", 2 * time.Second, 0, 600 * time.Millisecond},
		{"3 s of a long limit", time.Minute, 0, 3 * time.Second},
		{"as it is given", 10 * time.Second, time.Second, time.Second},
		{"at most half of the limit", 10 * time.Second, 8 * time.Second, 5 * time.Second},
	} {
		r := New(Config{LookupTimeout: c.lookup, RepeatAfter: c.given})
		if r.repeatAfter != c.want {
			t.Errorf("%s: LookupTimeout %v and RepeatAfter %v give %v, want %v", c.name, c.lookup, c.given, r.repeatAfter, c.want)
		}
		r.Close()
	}
}
