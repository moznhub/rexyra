package goredis

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// clientInfo returns the CLIENT INFO line of the connection c uses, as a map.
func clientInfo(t *testing.T, conn *redis.Conn) map[string]string {
	t.Helper()
	cmd := redis.NewStringCmd(ctx(), "CLIENT", "INFO")
	_ = conn.Process(ctx(), cmd)
	line := must(cmd.Result())
	fields := map[string]string{}
	for _, f := range strings.Fields(line) {
		if k, v, ok := strings.Cut(f, "="); ok {
			fields[k] = v
		}
	}
	return fields
}

// The handshake go-redis does on every new connection: HELLO (with AUTH and
// SETNAME), CLIENT SETINFO lib-name / lib-ver, and in recent releases
// CLIENT MAINT_NOTIFICATIONS. Whatever the server rejects there go-redis
// mostly swallows, so check the state the handshake should have left behind.
func TestHandshake(t *testing.T) {
	for _, pr := range protocols {
		t.Run(pr.name, func(t *testing.T) {
			defer guard(t)
			proxy := newRecordingProxy(t, server.Addr)
			opt := options(pr.protocol)
			opt.Addr = proxy.Addr
			opt.ClientName = "go-handshake"
			c := connect(t, opt)

			conn := c.Conn()
			defer conn.Close()
			info := clientInfo(t, conn)
			for _, cmd := range proxy.Commands() {
				t.Logf("sent: %s", cmd)
			}
			t.Logf("CLIENT INFO: %v", info)

			wantResp := "3"
			if pr.protocol == 2 {
				wantResp = "2"
			}
			equal(t, "resp", info["resp"], wantResp)
			equal(t, "name", info["name"], "go-handshake")
			if !strings.HasPrefix(info["lib-name"], "go-redis") {
				t.Errorf("lib-name = %q, want go-redis(...) from CLIENT SETINFO", info["lib-name"])
			}
			if info["lib-ver"] == "" {
				t.Errorf("lib-ver is empty, want the version from CLIENT SETINFO")
			}
			equal(t, "CLIENT GETNAME", must(conn.ClientGetName(ctx()).Result()), "go-handshake")
		})
	}
}

func TestHelloReply(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprintf("resp%d", version), func(t *testing.T) {
			defer guard(t)
			c := connect(t, options(version))
			hello := must(c.Do(ctx(), "HELLO", version).Result())
			var fields map[string]any
			switch h := hello.(type) {
			case map[any]any:
				fields = map[string]any{}
				for k, v := range h {
					fields[fmt.Sprint(k)] = v
				}
			case []any:
				fields = map[string]any{}
				for i := 0; i+1 < len(h); i += 2 {
					fields[fmt.Sprint(h[i])] = h[i+1]
				}
			default:
				t.Fatalf("HELLO reply is %T", hello)
			}
			equal(t, "server", fmt.Sprint(fields["server"]), "redis")
			equal(t, "proto", fmt.Sprint(fields["proto"]), fmt.Sprint(version))
			equal(t, "mode", fmt.Sprint(fields["mode"]), "standalone")
			equal(t, "role", fmt.Sprint(fields["role"]), "master")
			if v := fmt.Sprint(fields["version"]); v < "7" {
				t.Errorf("version = %q, want >= 7", v)
			}
		})
	}
}

func TestWrongPassword(t *testing.T) {
	if server.Password == "" {
		t.Skip("the server has no password")
	}
	for _, pr := range protocols {
		t.Run(pr.name, func(t *testing.T) {
			opt := options(pr.protocol)
			opt.Password = "not-the-password"
			c := redis.NewClient(opt)
			defer c.Close()
			err := c.Ping(ctx()).Err()
			if err == nil {
				t.Fatal("PING with a wrong password succeeded")
			}
			if !strings.Contains(err.Error(), "WRONGPASS") {
				t.Errorf("error = %q, want WRONGPASS", err)
			}
		})
	}
}

func TestNoPassword(t *testing.T) {
	if server.Password == "" {
		t.Skip("the server has no password")
	}
	for _, pr := range protocols {
		t.Run(pr.name, func(t *testing.T) {
			opt := options(pr.protocol)
			opt.Password = ""
			c := redis.NewClient(opt)
			defer c.Close()
			err := c.Ping(ctx()).Err()
			if err == nil || !strings.Contains(err.Error(), "NOAUTH") {
				t.Errorf("PING without a password: %v, want NOAUTH", err)
			}
		})
	}
}

func TestSelectDatabase(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		opt := options(c.Options().Protocol)
		opt.DB = 5
		db5 := connect(t, opt)
		must(db5.Set(ctx(), p+"k", "in five", 0).Result())
		equal(t, "GET in db 5", must(db5.Get(ctx(), p+"k").Result()), "in five")
		if err := c.Get(ctx(), p+"k").Err(); !errors.Is(err, redis.Nil) {
			t.Fatalf("GET in db 0: %v, want redis.Nil", err)
		}
		// Every pooled connection is on db 5, not only the first one.
		var wg sync.WaitGroup
		var wrong atomic.Int32
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if v, err := db5.Get(ctx(), p+"k").Result(); err != nil || v != "in five" {
					wrong.Add(1)
				}
			}()
		}
		wg.Wait()
		equal(t, "wrong reads", wrong.Load(), 0)

		conn := c.Conn()
		defer conn.Close()
		must(conn.Select(ctx(), 5).Result())
		equal(t, "SELECT 5 then GET", must(conn.Get(ctx(), p+"k").Result()), "in five")
	})
}

func TestMove(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		opt := options(c.Options().Protocol)
		opt.DB = 5
		db5 := connect(t, opt)
		must(db5.Set(ctx(), p+"k", "in five", 0).Result())
		equal(t, "MOVE", must(db5.Move(ctx(), p+"k", 0).Result()), true)
		equal(t, "GET in db 0 after MOVE", must(c.Get(ctx(), p+"k").Result()), "in five")
		equal(t, "EXISTS in db 5 after MOVE", must(db5.Exists(ctx(), p+"k").Result()), 0)
		must(db5.Set(ctx(), p+"k", "other", 0).Result())
		equal(t, "MOVE onto an existing key", must(db5.Move(ctx(), p+"k", 0).Result()), false)
	})
}

func TestSwapDB(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		opt := options(c.Options().Protocol)
		opt.DB = 5
		db5 := connect(t, opt)
		must(c.Set(ctx(), p+"k", "in zero", 0).Result())
		equal(t, "SWAPDB", must(c.Do(ctx(), "SWAPDB", 0, 5).Text()), "OK")
		equal(t, "GET in db 5 after SWAPDB", must(db5.Get(ctx(), p+"k").Result()), "in zero")
		must(c.Do(ctx(), "SWAPDB", 0, 5).Result())
		equal(t, "GET in db 0 after swapping back", must(c.Get(ctx(), p+"k").Result()), "in zero")
	})
}

// Many goroutines sharing one client, as a web application does.
func TestConnectionPool(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		opt := options(c.Options().Protocol)
		opt.PoolSize = 8
		opt.MinIdleConns = 2
		pooled := connect(t, opt)

		const workers, each = 64, 200
		var wg sync.WaitGroup
		errs := make(chan error, workers)
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < each; i++ {
					if err := pooled.Incr(ctx(), p+"counter").Err(); err != nil {
						errs <- err
						return
					}
					key := fmt.Sprintf("%sw%d", p, w)
					if err := pooled.Set(ctx(), key, i, 0).Err(); err != nil {
						errs <- err
						return
					}
					if v, err := pooled.Get(ctx(), key).Int(); err != nil || v != i {
						errs <- fmt.Errorf("worker %d read %d, %v; want %d", w, v, err, i)
						return
					}
				}
			}(w)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		equal(t, "counter", must(pooled.Get(ctx(), p+"counter").Int()), workers*each)
		stats := pooled.PoolStats()
		t.Logf("pool stats: %+v", *stats)
		if stats.TotalConns > uint32(opt.PoolSize) {
			t.Errorf("pool has %d connections, more than PoolSize %d", stats.TotalConns, opt.PoolSize)
		}
		equal(t, "timeouts", stats.Timeouts, 0)
	})
}

func TestPipelined(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		cmds := must(c.Pipelined(ctx(), func(pipe redis.Pipeliner) error {
			for i := 0; i < 1000; i++ {
				pipe.Set(ctx(), fmt.Sprintf("%s%d", p, i), i, 0)
			}
			return nil
		}))
		equal(t, "commands", len(cmds), 1000)

		pipe := c.Pipeline()
		gets := make([]*redis.StringCmd, 1000)
		for i := range gets {
			gets[i] = pipe.Get(ctx(), fmt.Sprintf("%s%d", p, i))
		}
		must(pipe.Exec(ctx()))
		for i, g := range gets {
			equal(t, "pipelined GET", must(g.Int()), i)
		}

		// One failing command does not stop the others.
		var incr *redis.IntCmd
		var bad *redis.IntCmd
		var missing *redis.StringCmd
		_, err := c.Pipelined(ctx(), func(pipe redis.Pipeliner) error {
			pipe.Set(ctx(), p+"text", "not a number", 0)
			bad = pipe.Incr(ctx(), p+"text")
			missing = pipe.Get(ctx(), p+"missing")
			incr = pipe.Incr(ctx(), p+"n")
			return nil
		})
		if err == nil {
			t.Fatal("pipeline with a failing command returned no error")
		}
		if bad.Err() == nil || !strings.HasPrefix(bad.Err().Error(), "ERR") {
			t.Errorf("INCR of text: %v, want ERR ...", bad.Err())
		}
		if !errors.Is(missing.Err(), redis.Nil) {
			t.Errorf("GET of missing key in pipeline: %v, want redis.Nil", missing.Err())
		}
		equal(t, "INCR after the failure", must(incr.Result()), 1)
	})
}

func TestTxPipelined(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		var incr *redis.IntCmd
		var ttl *redis.DurationCmd
		cmds := must(c.TxPipelined(ctx(), func(pipe redis.Pipeliner) error {
			incr = pipe.Incr(ctx(), p+"n")
			pipe.Expire(ctx(), p+"n", time.Hour)
			ttl = pipe.TTL(ctx(), p+"n")
			pipe.HSet(ctx(), p+"h", "a", 1, "b", 2)
			return nil
		}))
		equal(t, "commands", len(cmds), 4)
		equal(t, "INCR in MULTI", must(incr.Result()), 1)
		equal(t, "TTL in MULTI", must(ttl.Result()), time.Hour)
		equal(t, "HLEN", must(c.HLen(ctx(), p+"h").Result()), 2)

		// A command that fails at EXEC time fails alone; the others still run.
		var bad *redis.IntCmd
		var after *redis.IntCmd
		_, err := c.TxPipelined(ctx(), func(pipe redis.Pipeliner) error {
			pipe.Set(ctx(), p+"text", "abc", 0)
			bad = pipe.Incr(ctx(), p+"text")
			after = pipe.Incr(ctx(), p+"n")
			return nil
		})
		if err == nil || bad.Err() == nil {
			t.Fatalf("INCR of text inside MULTI: %v / %v, want an error", err, bad.Err())
		}
		equal(t, "INCR after the failing command", must(after.Result()), 2)

		// A command rejected while queuing aborts the whole transaction (EXECABORT).
		_, err = c.TxPipelined(ctx(), func(pipe redis.Pipeliner) error {
			pipe.Incr(ctx(), p+"n")
			pipe.Do(ctx(), "NOT-A-COMMAND")
			return nil
		})
		if err == nil {
			t.Fatal("transaction with an unknown command succeeded")
		}
		t.Logf("transaction with an unknown command: %v", err)
		equal(t, "n after EXECABORT", must(c.Get(ctx(), p+"n").Int()), 2)
	})
}

// The optimistic-locking loop from the go-redis documentation: WATCH, read,
// MULTI/EXEC, and retry on redis.TxFailedErr.
func TestWatchWithRetry(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		key := p + "counter"
		increment := func() error {
			for retry := 0; retry < 1000; retry++ {
				err := c.Watch(ctx(), func(tx *redis.Tx) error {
					n, err := tx.Get(ctx(), key).Int()
					if err != nil && !errors.Is(err, redis.Nil) {
						return err
					}
					_, err = tx.TxPipelined(ctx(), func(pipe redis.Pipeliner) error {
						pipe.Set(ctx(), key, n+1, 0)
						return nil
					})
					return err
				}, key)
				if errors.Is(err, redis.TxFailedErr) {
					continue
				}
				return err
			}
			return errors.New("too many retries")
		}

		const workers, each = 10, 20
		var wg sync.WaitGroup
		var failed atomic.Value
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < each; i++ {
					if err := increment(); err != nil {
						failed.Store(err)
						return
					}
				}
			}()
		}
		wg.Wait()
		if err, _ := failed.Load().(error); err != nil {
			t.Fatal(err)
		}
		equal(t, "counter", must(c.Get(ctx(), key).Int()), workers*each)

		// A write from another client between WATCH and EXEC aborts EXEC.
		err := c.Watch(ctx(), func(tx *redis.Tx) error {
			if err := c.Set(ctx(), key, "changed", 0).Err(); err != nil {
				return err
			}
			_, err := tx.TxPipelined(ctx(), func(pipe redis.Pipeliner) error {
				pipe.Set(ctx(), key, "mine", 0)
				return nil
			})
			return err
		}, key)
		if !errors.Is(err, redis.TxFailedErr) {
			t.Fatalf("EXEC after a watched key changed: %v, want redis.TxFailedErr", err)
		}
		equal(t, "value", must(c.Get(ctx(), key).Result()), "changed")

		// An expiry of a watched key also aborts EXEC.
		must(c.Set(ctx(), p+"short", "v", 50*time.Millisecond).Result())
		err = c.Watch(ctx(), func(tx *redis.Tx) error {
			time.Sleep(150 * time.Millisecond)
			_, err := tx.TxPipelined(ctx(), func(pipe redis.Pipeliner) error {
				pipe.Set(ctx(), p+"short", "mine", 0)
				return nil
			})
			return err
		}, p+"short")
		if !errors.Is(err, redis.TxFailedErr) {
			t.Fatalf("EXEC after a watched key expired: %v, want redis.TxFailedErr", err)
		}

		// UNWATCH through Tx.Unwatch: the transaction then runs.
		err = c.Watch(ctx(), func(tx *redis.Tx) error {
			if err := tx.Unwatch(ctx()).Err(); err != nil {
				return err
			}
			if err := c.Set(ctx(), key, "changed again", 0).Err(); err != nil {
				return err
			}
			_, err := tx.TxPipelined(ctx(), func(pipe redis.Pipeliner) error {
				pipe.Set(ctx(), key, "mine", 0)
				return nil
			})
			return err
		}, key)
		if err != nil {
			t.Fatalf("EXEC after UNWATCH: %v", err)
		}
		equal(t, "value after UNWATCH", must(c.Get(ctx(), key).Result()), "mine")
	})
}

func TestLua(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		got := must(c.Eval(ctx(),
			"redis.call('SET', KEYS[1], ARGV[1]); return redis.call('INCRBY', KEYS[2], ARGV[2])",
			[]string{p + "s", p + "n"}, "hello", 41).Int())
		equal(t, "EVAL", got, 41)
		equal(t, "GET", must(c.Get(ctx(), p+"s").Result()), "hello")

		// Every Lua reply type.
		mixed := must(c.Eval(ctx(), "return {1, 'two', {3}, redis.status_reply('OK'), 3.99, true, false}", nil).Slice())
		equal(t, "len", len(mixed), 7)
		equal(t, "int", mixed[0], any(int64(1)))
		equal(t, "string", mixed[1], any("two"))
		equal(t, "nested", fmt.Sprint(mixed[2]), "[3]")
		equal(t, "status", mixed[3], any("OK"))
		equal(t, "float truncates", mixed[4], any(int64(3)))
		equal(t, "true", mixed[5], any(int64(1)))
		equal(t, "false", mixed[6], nil)
		if err := c.Eval(ctx(), "return nil", nil).Err(); !errors.Is(err, redis.Nil) {
			t.Errorf("return nil: %v, want redis.Nil", err)
		}
		err := c.Eval(ctx(), "return redis.error_reply('my error')", nil).Err()
		if err == nil || !strings.Contains(err.Error(), "my error") {
			t.Errorf("error_reply: %v", err)
		}
		err = c.Eval(ctx(), "return redis.call('INCR', KEYS[1])", []string{p + "s"}).Err()
		if err == nil || !strings.Contains(err.Error(), "not an integer") {
			t.Errorf("failing redis.call: %v", err)
		}
		equal(t, "pcall", must(c.Eval(ctx(),
			"local ok = redis.pcall('INCR', KEYS[1]); if type(ok) == 'table' and ok.err then return 'caught' end return 'no'",
			[]string{p + "s"}).Text()), "caught")

		// EVALSHA of a script the server does not know: NOSCRIPT.
		err = c.EvalSha(ctx(), "ffffffffffffffffffffffffffffffffffffffff", nil).Err()
		if err == nil || !strings.HasPrefix(err.Error(), "NOSCRIPT") {
			t.Errorf("EVALSHA of an unknown script: %v, want NOSCRIPT", err)
		}

		// SCRIPT LOAD, then EVALSHA.
		sha := must(c.ScriptLoad(ctx(), "return ARGV[1] .. '!'").Result())
		equal(t, "EVALSHA", must(c.EvalSha(ctx(), sha, nil, "hi").Text()), "hi!")
		equal(t, "SCRIPT EXISTS", fmt.Sprint(must(c.ScriptExists(ctx(), sha, "0000000000000000000000000000000000000000").Result())), "[true false]")

		// redis.NewScript(...).Run: EVALSHA, and EVAL when the server answers
		// NOSCRIPT. The script text is unique to this test so the server
		// cannot know it yet.
		limiter := redis.NewScript(fmt.Sprintf(`-- %s
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
if n > tonumber(ARGV[2]) then return 0 else return 1 end`, p))
		equal(t, "script known before Run", fmt.Sprint(must(limiter.Exists(ctx(), c).Result())), "[false]")
		allowed := 0
		for i := 0; i < 8; i++ {
			allowed += must(limiter.Run(ctx(), c, []string{p + "rate"}, 60000, 5).Int())
		}
		equal(t, "allowed", allowed, 5)
		equal(t, "script known after Run", fmt.Sprint(must(limiter.Exists(ctx(), c).Result())), "[true]")
		if ttl := must(c.PTTL(ctx(), p+"rate").Result()); ttl <= 0 || ttl > time.Minute {
			t.Errorf("PTTL = %v", ttl)
		}

		// Read-only variants (Redis 7): EVAL_RO / EVALSHA_RO.
		ro := redis.NewScript(fmt.Sprintf("-- ro %s\nreturn redis.call('GET', KEYS[1])", p))
		equal(t, "EVALSHA_RO / EVAL_RO", must(ro.RunRO(ctx(), c, []string{p + "s"}).Text()), "hello")
		err = c.EvalRO(ctx(), "return redis.call('SET', KEYS[1], 'x')", []string{p + "s"}).Err()
		if err == nil {
			t.Error("a write inside EVAL_RO succeeded")
		}

		// Scripts inside a pipeline and a transaction.
		var a, b *redis.Cmd
		must(c.TxPipelined(ctx(), func(pipe redis.Pipeliner) error {
			a = limiter.Run(ctx(), pipe, []string{p + "rate2"}, 60000, 5)
			b = pipe.Eval(ctx(), "return #KEYS + #ARGV", []string{p + "x", p + "y"}, 1, 2, 3)
			return nil
		}))
		equal(t, "script in MULTI", must(a.Int()), 1)
		equal(t, "EVAL in MULTI", must(b.Int()), 5)
	})
}

// Redis 7 functions: FUNCTION LOAD, FCALL, FCALL_RO.
func TestFunctions(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		lib := "golib" + p[3:11]
		code := fmt.Sprintf(`#!lua name=%s
redis.register_function('%s_incr', function(keys, args) return redis.call('INCRBY', keys[1], args[1]) end)
redis.register_function{function_name='%s_get', callback=function(keys) return redis.call('GET', keys[1]) end, flags={'no-writes'}}`, lib, lib, lib)
		equal(t, "FUNCTION LOAD", must(c.FunctionLoad(ctx(), code).Result()), lib)
		defer c.FunctionDelete(ctx(), lib)
		if err := c.FunctionLoad(ctx(), code).Err(); err == nil || !strings.Contains(err.Error(), "already exists") {
			t.Errorf("loading the library twice: %v, want ... already exists", err)
		}
		equal(t, "FUNCTION LOAD REPLACE", must(c.FunctionLoadReplace(ctx(), code).Result()), lib)
		equal(t, "FCALL", must(c.FCall(ctx(), lib+"_incr", []string{p + "n"}, 5).Int()), 5)
		equal(t, "FCALL_RO", must(c.FCallRO(ctx(), lib+"_get", []string{p + "n"}).Text()), "5")
		if err := c.FCallRO(ctx(), lib+"_incr", []string{p + "n"}, 1).Err(); err == nil {
			t.Error("FCALL_RO of a function that writes succeeded")
		}
		libs := must(c.FunctionList(ctx(), redis.FunctionListQuery{LibraryNamePattern: lib, WithCode: true}).Result())
		if len(libs) != 1 || libs[0].Name != lib || libs[0].Engine != "LUA" || len(libs[0].Functions) != 2 || libs[0].Code != code {
			t.Errorf("FUNCTION LIST = %+v", libs)
		}
		equal(t, "FUNCTION DELETE", must(c.FunctionDelete(ctx(), lib).Result()), "OK")
		if err := c.FCall(ctx(), lib+"_incr", []string{p + "n"}, 1).Err(); err == nil {
			t.Error("FCALL of a deleted function succeeded")
		}
	})
}

func TestBlockingCommands(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		// go-redis sends blocking timeouts in whole seconds (it rounds
		// anything shorter up to 1s).
		start := time.Now()
		err := c.BLPop(ctx(), time.Second, p+"empty").Err()
		if !errors.Is(err, redis.Nil) {
			t.Fatalf("BLPOP timeout: %v, want redis.Nil", err)
		}
		if elapsed := time.Since(start); elapsed < 900*time.Millisecond || elapsed > 3*time.Second {
			t.Errorf("BLPOP with a 1s timeout took %v", elapsed)
		}
		// A fractional timeout sent as is (Redis 6+ accepts it).
		start = time.Now()
		if err := c.Do(ctx(), "BLPOP", p+"empty", "0.2").Err(); !errors.Is(err, redis.Nil) {
			t.Fatalf("BLPOP with timeout 0.2: %v, want redis.Nil", err)
		}
		if elapsed := time.Since(start); elapsed < 150*time.Millisecond || elapsed > 900*time.Millisecond {
			t.Errorf("BLPOP with a 0.2s timeout took %v", elapsed)
		}

		go func() {
			time.Sleep(100 * time.Millisecond)
			_ = c.RPush(ctx(), p+"q2", "late").Err()
		}()
		got := must(c.BLPop(ctx(), 5*time.Second, p+"q1", p+"q2").Result())
		equal(t, "BLPOP", strings.Join(got, ","), p+"q2,late")

		must(c.RPush(ctx(), p+"src", "a", "b").Result())
		equal(t, "BRPOP", strings.Join(must(c.BRPop(ctx(), time.Second, p+"src").Result()), ","), p+"src,b")
		equal(t, "BLMOVE", must(c.BLMove(ctx(), p+"src", p+"dst", "LEFT", "RIGHT", time.Second).Result()), "a")
		equal(t, "BRPOPLPUSH timeout", errors.Is(c.BRPopLPush(ctx(), p+"src", p+"dst", time.Second).Err(), redis.Nil), true)

		must(c.ZAdd(ctx(), p+"z", redis.Z{Score: 1, Member: "one"}, redis.Z{Score: 2, Member: "two"}).Result())
		z := must(c.BZPopMin(ctx(), time.Second, p+"z").Result())
		equal(t, "BZPOPMIN", fmt.Sprintf("%s %v %v", z.Key, z.Member, z.Score), p+"z one 1")
		z = must(c.BZPopMax(ctx(), time.Second, p+"z").Result())
		equal(t, "BZPOPMAX", fmt.Sprintf("%v %v", z.Member, z.Score), "two 2")
		if err := c.BZPopMin(ctx(), time.Second, p+"z").Err(); !errors.Is(err, redis.Nil) {
			t.Errorf("BZPOPMIN timeout: %v, want redis.Nil", err)
		}

		must(c.RPush(ctx(), p+"m", "1", "2", "3").Result())
		key, vals := must2(c.BLMPop(ctx(), time.Second, "left", 2, p+"none", p+"m").Result())
		equal(t, "BLMPOP", key+" "+strings.Join(vals, ","), p+"m 1,2")

		// A blocked client is woken by a push inside a transaction.
		done := make(chan []string, 1)
		go func() {
			v, _ := c.BLPop(ctx(), 5*time.Second, p+"txq").Result()
			done <- v
		}()
		time.Sleep(100 * time.Millisecond)
		must(c.TxPipelined(ctx(), func(pipe redis.Pipeliner) error {
			pipe.RPush(ctx(), p+"txq", "x")
			return nil
		}))
		select {
		case v := <-done:
			equal(t, "BLPOP woken by MULTI", strings.Join(v, ","), p+"txq,x")
		case <-time.After(6 * time.Second):
			t.Fatal("BLPOP was not woken")
		}
	})
}

func TestServerAPI(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		info := must(c.Info(ctx(), "server").Result())
		if !strings.Contains(info, "redis_version:") {
			t.Errorf("INFO server has no redis_version:\n%s", info)
		}
		all := must(c.Info(ctx()).Result())
		for _, section := range []string{"# Server", "# Clients", "# Memory", "# Stats", "# Replication", "# Keyspace"} {
			if !strings.Contains(all, section) {
				t.Errorf("INFO has no %q section", section)
			}
		}

		cfg := must(c.ConfigGet(ctx(), "maxmemory").Result())
		if _, ok := cfg["maxmemory"]; !ok {
			t.Errorf("CONFIG GET maxmemory = %v", cfg)
		}
		cfg = must(c.ConfigGet(ctx(), "*max-*-entries*").Result())
		if len(cfg) == 0 {
			t.Errorf("CONFIG GET with a pattern returned nothing")
		}

		conn := c.Conn()
		defer conn.Close()
		must(conn.ClientSetName(ctx(), "go-server-api").Result())
		equal(t, "CLIENT GETNAME", must(conn.ClientGetName(ctx()).Result()), "go-server-api")
		id := must(conn.ClientID(ctx()).Result())
		list := must(c.ClientList(ctx()).Result())
		if !strings.Contains(list, fmt.Sprintf("id=%d ", id)) || !strings.Contains(list, "name=go-server-api") {
			t.Errorf("CLIENT LIST does not show this connection:\n%s", list)
		}
		ci := must(conn.ClientInfo(ctx()).Result())
		equal(t, "CLIENT INFO id", ci.ID, id)
		equal(t, "CLIENT INFO name", ci.Name, "go-server-api")

		equal(t, "ECHO", must(c.Echo(ctx(), "echo").Result()), "echo")
		if now := must(c.Time(ctx()).Result()); time.Since(now).Abs() > 5*time.Minute {
			t.Errorf("TIME = %v", now)
		}
		must(c.LastSave(ctx()).Result())
		must(c.SlowLogGet(ctx(), 10).Result())
		cmds := must(c.Command(ctx()).Result())
		if get, ok := cmds["get"]; !ok || get.Arity != 2 || get.FirstKeyPos != 1 {
			t.Errorf("COMMAND get = %+v", cmds["get"])
		}
		if n := must(c.Do(ctx(), "COMMAND", "COUNT").Int64()); n < 100 {
			t.Errorf("COMMAND COUNT = %d", n)
		}

		db5opt := options(c.Options().Protocol)
		db5opt.DB = 5
		db5 := connect(t, db5opt)
		must(db5.Set(ctx(), p+"x", 1, 0).Result())
		if n := must(db5.DBSize(ctx()).Result()); n < 1 {
			t.Errorf("DBSIZE = %d", n)
		}
		equal(t, "FLUSHDB", must(db5.FlushDB(ctx()).Result()), "OK")
		equal(t, "DBSIZE after FLUSHDB", must(db5.DBSize(ctx()).Result()), 0)
		must(db5.Set(ctx(), p+"y", 1, 0).Result())
		equal(t, "FLUSHDB ASYNC", must(db5.FlushDBAsync(ctx()).Result()), "OK")
		equal(t, "DBSIZE after FLUSHDB ASYNC", must(db5.DBSize(ctx()).Result()), 0)

		must(c.Set(ctx(), p+"mem", "value", 0).Result())
		if n := must(c.MemoryUsage(ctx(), p+"mem").Result()); n <= 0 {
			t.Errorf("MEMORY USAGE = %d", n)
		}
		if err := c.MemoryUsage(ctx(), p+"nothing").Err(); !errors.Is(err, redis.Nil) {
			t.Errorf("MEMORY USAGE of a missing key: %v, want redis.Nil", err)
		}
		equal(t, "OBJECT ENCODING", must(c.ObjectEncoding(ctx(), p+"mem").Result()), "embstr")
	})
}

func TestCommandGetKeys(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		keys := must(c.CommandGetKeys(ctx(), "MSET", "a", "1", "b", "2").Result())
		equal(t, "COMMAND GETKEYS", strings.Join(keys, ","), "a,b")
	})
}

func TestCommandGetKeysAndFlags(t *testing.T) {
	skipOnRexyra(t, "COMMAND GETKEYSANDFLAGS is not implemented: the command table has no per-key flags")
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		kf := must(c.CommandGetKeysAndFlags(ctx(), "LMOVE", "src", "dst", "LEFT", "RIGHT").Result())
		equal(t, "COMMAND GETKEYSANDFLAGS", fmt.Sprint(kf), "[{src [RW access delete]} {dst [RW insert]}]")
	})
}

// The SCAN iterator, which applications use instead of KEYS.
func TestScanIterator(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		for i := 0; i < 250; i++ {
			must(c.Set(ctx(), fmt.Sprintf("%sscan:%d", p, i), i, 0).Result())
		}
		must(c.HSet(ctx(), p+"scan:hash", "f", "v").Result())

		seen := map[string]bool{}
		iter := c.Scan(ctx(), 0, p+"scan:*", 20).Iterator()
		for iter.Next(ctx()) {
			seen[iter.Val()] = true
		}
		must(0, iter.Err())
		equal(t, "keys seen", len(seen), 251)

		var strs int
		iter = c.ScanType(ctx(), 0, p+"scan:*", 50, "string").Iterator()
		for iter.Next(ctx()) {
			strs++
		}
		must(0, iter.Err())
		equal(t, "SCAN TYPE string", strs, 250)

		equal(t, "KEYS", len(must(c.Keys(ctx(), p+"scan:1?").Result())), 10)

		deleted := must(c.Del(ctx(), func() []string {
			out := make([]string, 0, len(seen))
			for k := range seen {
				out = append(out, k)
			}
			return out
		}()...).Result())
		equal(t, "DEL", deleted, 251)
	})
}

// After CLIENT KILL drops the connection the client uses, the next command
// reconnects (go-redis retries on a broken connection).
func TestReconnectAfterClientKill(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		opt := options(c.Options().Protocol)
		opt.PoolSize = 1
		opt.ClientName = "go-reconnect-" + p[3:11]
		victim := connect(t, opt)
		must(victim.Set(ctx(), p+"k", "before", 0).Result())

		killed := 0
		for _, line := range strings.Split(must(c.ClientList(ctx()).Result()), "\n") {
			if !strings.Contains(line, "name="+opt.ClientName+" ") {
				continue
			}
			for _, f := range strings.Fields(line) {
				if id, ok := strings.CutPrefix(f, "id="); ok {
					n := must(c.Do(ctx(), "CLIENT", "KILL", "ID", id).Int())
					killed += n
				}
			}
		}
		if killed < 1 {
			t.Fatalf("CLIENT KILL ID killed %d connections", killed)
		}

		deadline := time.Now().Add(15 * time.Second)
		var value string
		var err error
		for time.Now().Before(deadline) {
			value, err = victim.Get(ctx(), p+"k").Result()
			if err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("GET after CLIENT KILL: %v", err)
		}
		equal(t, "value", value, "before")

		// The new connection went through the handshake again.
		conn := victim.Conn()
		defer conn.Close()
		equal(t, "name after reconnect", must(conn.ClientGetName(ctx()).Result()), opt.ClientName)

		// CLIENT KILL with the old address form, and SKIPME.
		other := connect(t, options(c.Options().Protocol))
		otherConn := other.Conn()
		defer otherConn.Close()
		addr := must(otherConn.ClientInfo(ctx()).Result()).Addr
		equal(t, "CLIENT KILL addr", must(c.ClientKill(ctx(), addr).Result()), "OK")
		if list := must(c.ClientList(ctx()).Result()); strings.Contains(list, "addr="+addr+" ") {
			t.Errorf("CLIENT LIST still shows %s after CLIENT KILL", addr)
		}
	})
}
