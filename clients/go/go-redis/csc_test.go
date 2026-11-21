//go:build !goredis_old

// Features of recent go-redis releases only (client-side caching arrived in
// v9.18). Build with -tags goredis_old to test an older release; see
// test-version.ps1.

package goredis

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client-side caching: go-redis sends CLIENT TRACKING ON on each pooled
// connection, serves repeated GETs from its own cache, and drops an entry when
// the server pushes an "invalidate" message (RESP3).
func TestClientSideCache(t *testing.T) {
	skipOnRexyra(t, "CLIENT TRACKING is not implemented, so go-redis silently turns client-side caching off")
	defer guard(t)
	p := prefix()
	opt := options(3)
	opt.ClientSideCacheConfig = &redis.ClientSideCacheConfig{MaxEntries: 1024}
	csc := connect(t, opt)
	mutator := connect(t, options(0))

	key := p + "cached"
	must(mutator.Set(ctx(), key, "v1", 0).Result())
	deadline := time.Now().Add(3 * time.Second)
	for csc.CSCStats().Hits == 0 {
		equal(t, "cached GET", must(csc.Get(ctx(), key).Result()), "v1")
		if time.Now().After(deadline) {
			t.Fatalf("no cache hit after repeated GETs; stats %+v", csc.CSCStats())
		}
		time.Sleep(20 * time.Millisecond)
	}

	must(mutator.Set(ctx(), key, "v2", 0).Result())
	deadline = time.Now().Add(3 * time.Second)
	for {
		must(csc.Ping(ctx()).Result())
		v := must(csc.Get(ctx(), key).Result())
		if v == "v2" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("still reading %q from the cache after another client changed the key; stats %+v", v, csc.CSCStats())
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("cache stats: %+v", csc.CSCStats())
}
