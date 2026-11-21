package goredis

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func sorted(s []string) string {
	s = append([]string(nil), s...)
	sort.Strings(s)
	return strings.Join(s, ",")
}

func TestStrings(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		equal(t, "SET", must(c.Set(ctx(), p+"a", "hello", 0).Result()), "OK")
		equal(t, "GET", must(c.Get(ctx(), p+"a").Result()), "hello")
		if err := c.Get(ctx(), p+"missing").Err(); !errors.Is(err, redis.Nil) {
			t.Fatalf("GET of a missing key: %v, want redis.Nil", err)
		}
		equal(t, "SETNX existing", must(c.SetNX(ctx(), p+"a", "other", 0).Result()), false)
		equal(t, "SETNX new", must(c.SetNX(ctx(), p+"b", "x", 0).Result()), true)
		equal(t, "SETXX missing", must(c.SetXX(ctx(), p+"none", "x", 0).Result()), false)
		equal(t, "SETXX existing", must(c.SetXX(ctx(), p+"b", "y", 0).Result()), true)

		// SET ... GET and NX/XX through SetArgs.
		old := must(c.SetArgs(ctx(), p+"b", "z", redis.SetArgs{Get: true}).Result())
		equal(t, "SET GET", old, "y")
		if err := c.SetArgs(ctx(), p+"b", "w", redis.SetArgs{Mode: "NX"}).Err(); !errors.Is(err, redis.Nil) {
			t.Errorf("SET NX on an existing key: %v, want redis.Nil", err)
		}

		equal(t, "APPEND", must(c.Append(ctx(), p+"a", " world").Result()), 11)
		equal(t, "GETRANGE", must(c.GetRange(ctx(), p+"a", 6, -1).Result()), "world")
		equal(t, "SETRANGE", must(c.SetRange(ctx(), p+"a", 6, "WORLD").Result()), 11)
		equal(t, "STRLEN", must(c.StrLen(ctx(), p+"a").Result()), 11)

		equal(t, "INCRBY", must(c.IncrBy(ctx(), p+"n", 5).Result()), 5)
		equal(t, "DECRBY", must(c.DecrBy(ctx(), p+"n", 2).Result()), 3)
		equal(t, "INCR", must(c.Incr(ctx(), p+"n").Result()), 4)
		equal(t, "DECR", must(c.Decr(ctx(), p+"n").Result()), 3)
		equal(t, "INCRBYFLOAT", must(c.IncrByFloat(ctx(), p+"n", 0.5).Result()), 3.5)
		err := c.Incr(ctx(), p+"a").Err()
		if err == nil || err.Error() != "ERR value is not an integer or out of range" {
			t.Errorf("INCR of text: %v", err)
		}

		equal(t, "MSET", must(c.MSet(ctx(), p+"m1", "1", p+"m2", "2").Result()), "OK")
		equal(t, "MSET map", must(c.MSet(ctx(), map[string]any{p + "m3": "3"}).Result()), "OK")
		vals := must(c.MGet(ctx(), p+"m1", p+"missing", p+"m2", p+"m3").Result())
		equal(t, "MGET", fmt.Sprint(vals), "[1 <nil> 2 3]")
		equal(t, "MSETNX", must(c.MSetNX(ctx(), p+"m1", "x", p+"m9", "9").Result()), false)

		equal(t, "GETSET", must(c.GetSet(ctx(), p+"a", "replaced").Result()), "hello WORLD")
		equal(t, "GETDEL", must(c.GetDel(ctx(), p+"a").Result()), "replaced")
		equal(t, "EXISTS", must(c.Exists(ctx(), p+"a").Result()), 0)

		must(c.Set(ctx(), p+"ex", "v", 0).Result())
		equal(t, "GETEX", must(c.GetEx(ctx(), p+"ex", time.Minute).Result()), "v")
		if ttl := must(c.TTL(ctx(), p+"ex").Result()); ttl <= 50*time.Second {
			t.Errorf("TTL after GETEX = %v", ttl)
		}

		bin := []byte{0, 1, 2, 255, 13, 10, 0}
		must(c.Set(ctx(), p+"bin", bin, 0).Result())
		if got := must(c.Get(ctx(), p+"bin").Bytes()); !bytes.Equal(got, bin) {
			t.Errorf("binary value = %v", got)
		}
		big := strings.Repeat("x", 1<<20)
		must(c.Set(ctx(), p+"big", big, 0).Result())
		equal(t, "1 MiB value", must(c.Get(ctx(), p+"big").Result()) == big, true)
		must(c.Set(ctx(), p+"utf8", "héllo wörld ✓", 0).Result())
		equal(t, "UTF-8", must(c.Get(ctx(), p+"utf8").Result()), "héllo wörld ✓")

		must(c.MSet(ctx(), p+"l1", "ohmytext", p+"l2", "mynewtext").Result())
		equal(t, "LCS", must(c.LCS(ctx(), &redis.LCSQuery{Key1: p + "l1", Key2: p + "l2"}).Result()).MatchString, "mytext")

		// Values that go-redis formats itself: numbers, bools, time.
		must(c.Set(ctx(), p+"f", 1.25, 0).Result())
		equal(t, "float value", must(c.Get(ctx(), p+"f").Float64()), 1.25)
		must(c.Set(ctx(), p+"bool", true, 0).Result())
		equal(t, "bool value", must(c.Get(ctx(), p+"bool").Bool()), true)
	})
}

func TestBitmaps(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		equal(t, "SETBIT", must(c.SetBit(ctx(), p+"b", 7, 1).Result()), 0)
		equal(t, "SETBIT again", must(c.SetBit(ctx(), p+"b", 7, 1).Result()), 1)
		must(c.SetBit(ctx(), p+"b", 100, 1).Result())
		equal(t, "GETBIT", must(c.GetBit(ctx(), p+"b", 7).Result()), 1)
		equal(t, "GETBIT unset", must(c.GetBit(ctx(), p+"b", 8).Result()), 0)
		equal(t, "BITCOUNT", must(c.BitCount(ctx(), p+"b", nil).Result()), 2)
		equal(t, "BITCOUNT range", must(c.BitCount(ctx(), p+"b", &redis.BitCount{Start: 0, End: 0}).Result()), 1)
		equal(t, "BITPOS", must(c.BitPos(ctx(), p+"b", 1).Result()), 7)
		equal(t, "BITPOS 0", must(c.BitPos(ctx(), p+"b", 0).Result()), 0)
		equal(t, "STRLEN", must(c.StrLen(ctx(), p+"b").Result()), 13)

		must(c.Set(ctx(), p+"x", "\xff\x0f", 0).Result())
		must(c.Set(ctx(), p+"y", "\x0f\xff", 0).Result())
		equal(t, "BITOP AND", must(c.BitOpAnd(ctx(), p+"and", p+"x", p+"y").Result()), 2)
		equal(t, "AND", must(c.Get(ctx(), p+"and").Result()), "\x0f\x0f")
		must(c.BitOpOr(ctx(), p+"or", p+"x", p+"y").Result())
		equal(t, "OR", must(c.Get(ctx(), p+"or").Result()), "\xff\xff")
		must(c.BitOpXor(ctx(), p+"xor", p+"x", p+"y").Result())
		equal(t, "XOR", must(c.Get(ctx(), p+"xor").Result()), "\xf0\xf0")
		must(c.BitOpNot(ctx(), p+"not", p+"x").Result())
		equal(t, "NOT", must(c.Get(ctx(), p+"not").Result()), "\x00\xf0")

		got := must(c.BitField(ctx(), p+"bf",
			"SET", "u8", 0, 200,
			"GET", "u8", 0,
			"INCRBY", "u8", 0, 100,
			"OVERFLOW", "SAT", "INCRBY", "u8", 0, 100,
			"GET", "i4", "#1").Result())
		equal(t, "BITFIELD", fmt.Sprint(got), "[0 200 44 144 0]")
		equal(t, "BITFIELD_RO", fmt.Sprint(must(c.BitFieldRO(ctx(), p+"bf", "u8", 0).Result())), "[144]")
	})
}

func TestHashes(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		h := p + "h"
		equal(t, "HSET", must(c.HSet(ctx(), h, "a", "1", "b", "2", "c", "3").Result()), 3)
		equal(t, "HSET map", must(c.HSet(ctx(), h, map[string]any{"d": "4"}).Result()), 1)
		equal(t, "HGET", must(c.HGet(ctx(), h, "b").Result()), "2")
		if err := c.HGet(ctx(), h, "zz").Err(); !errors.Is(err, redis.Nil) {
			t.Errorf("HGET of a missing field: %v, want redis.Nil", err)
		}
		equal(t, "HLEN", must(c.HLen(ctx(), h).Result()), 4)
		equal(t, "HINCRBY", must(c.HIncrBy(ctx(), h, "a", 10).Result()), 11)
		equal(t, "HINCRBYFLOAT", must(c.HIncrByFloat(ctx(), h, "f", 2.5).Result()), 2.5)
		equal(t, "HEXISTS", must(c.HExists(ctx(), h, "c").Result()), true)
		equal(t, "HDEL", must(c.HDel(ctx(), h, "c", "d", "nope").Result()), 2)
		equal(t, "HSETNX", must(c.HSetNX(ctx(), h, "a", "x").Result()), false)
		equal(t, "HSTRLEN", must(c.Do(ctx(), "HSTRLEN", h, "a").Int64()), 2)
		equal(t, "HMSET", must(c.HMSet(ctx(), h, "m", "1").Result()), true)
		must(c.HDel(ctx(), h, "m").Result())

		all := must(c.HGetAll(ctx(), h).Result())
		equal(t, "HGETALL", fmt.Sprint(all), "map[a:11 b:2 f:2.5]")
		equal(t, "HKEYS", sorted(must(c.HKeys(ctx(), h).Result())), "a,b,f")
		equal(t, "HVALS", sorted(must(c.HVals(ctx(), h).Result())), "11,2,2.5")
		equal(t, "HMGET", fmt.Sprint(must(c.HMGet(ctx(), h, "a", "zz").Result())), "[11 <nil>]")
		if n := len(must(c.HRandField(ctx(), h, 2).Result())); n != 2 {
			t.Errorf("HRANDFIELD 2 returned %d fields", n)
		}
		kv := must(c.HRandFieldWithValues(ctx(), h, -5).Result())
		equal(t, "HRANDFIELD WITHVALUES -5", len(kv), 5)
		for _, e := range kv {
			equal(t, "HRANDFIELD value of "+e.Key, e.Value, all[e.Key])
		}

		// Scanning a hash into a struct, as applications do.
		type user struct {
			Name string `redis:"name"`
			Age  int    `redis:"age"`
		}
		must(c.HSet(ctx(), p+"user", user{Name: "ada", Age: 36}).Result())
		var u user
		must(0, c.HGetAll(ctx(), p+"user").Scan(&u))
		equal(t, "struct", u, user{Name: "ada", Age: 36})

		for i := 0; i < 300; i++ {
			must(c.HSet(ctx(), p+"big", fmt.Sprintf("f%d", i), i).Result())
		}
		fields := map[string]bool{}
		iter := c.HScan(ctx(), p+"big", 0, "", 50).Iterator()
		for iter.Next(ctx()) {
			fields[iter.Val()] = true
		}
		must(0, iter.Err())
		equal(t, "HSCAN fields and values", len(fields), 600)
	})
}

// Hash field expiry, new in Redis 7.4.
func TestHashFieldExpiry(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		h := p + "h"
		must(c.HSet(ctx(), h, "a", "1", "b", "2").Result())
		equal(t, "HEXPIRE", fmt.Sprint(must(c.HExpire(ctx(), h, time.Minute, "a", "zz").Result())), "[1 -2]")
		ttl := must(c.HTTL(ctx(), h, "a", "b").Result())
		if len(ttl) != 2 || ttl[0] <= 50 || ttl[0] > 60 || ttl[1] != -1 {
			t.Errorf("HTTL = %v", ttl)
		}
		equal(t, "HPERSIST", fmt.Sprint(must(c.HPersist(ctx(), h, "a").Result())), "[1]")
		equal(t, "HPEXPIRE", fmt.Sprint(must(c.HPExpire(ctx(), h, 50*time.Millisecond, "b").Result())), "[1]")
		time.Sleep(200 * time.Millisecond)
		equal(t, "HGETALL after a field expired", fmt.Sprint(must(c.HGetAll(ctx(), h).Result())), "map[a:1]")
	})
}

func TestLists(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		l := p + "l"
		equal(t, "RPUSH", must(c.RPush(ctx(), l, "b", "c").Result()), 2)
		equal(t, "LPUSH", must(c.LPush(ctx(), l, "a").Result()), 3)
		equal(t, "RPUSHX missing", must(c.RPushX(ctx(), p+"none", "x").Result()), 0)
		equal(t, "LRANGE", strings.Join(must(c.LRange(ctx(), l, 0, -1).Result()), ","), "a,b,c")
		equal(t, "LLEN", must(c.LLen(ctx(), l).Result()), 3)
		equal(t, "LINDEX", must(c.LIndex(ctx(), l, -1).Result()), "c")
		equal(t, "LINSERT", must(c.LInsertBefore(ctx(), l, "c", "b2").Result()), 4)
		equal(t, "LSET", must(c.LSet(ctx(), l, 0, "A").Result()), "OK")
		equal(t, "LPOS", must(c.LPos(ctx(), l, "b2", redis.LPosArgs{}).Result()), 2)
		must(c.RPush(ctx(), l, "b", "b").Result())
		equal(t, "LPOSCOUNT", fmt.Sprint(must(c.LPosCount(ctx(), l, "b", 0, redis.LPosArgs{}).Result())), "[1 4 5]")
		equal(t, "LREM", must(c.LRem(ctx(), l, -2, "b").Result()), 2)
		equal(t, "after LREM", strings.Join(must(c.LRange(ctx(), l, 0, -1).Result()), ","), "A,b,b2,c")
		equal(t, "LTRIM", must(c.LTrim(ctx(), l, 1, -1).Result()), "OK")
		equal(t, "LPOP", must(c.LPop(ctx(), l).Result()), "b")
		equal(t, "RPOP", must(c.RPop(ctx(), l).Result()), "c")
		must(c.RPush(ctx(), l, "1", "2", "3").Result())
		equal(t, "LPOP count", strings.Join(must(c.LPopCount(ctx(), l, 2).Result()), ","), "b2,1")
		equal(t, "RPOP count", strings.Join(must(c.RPopCount(ctx(), l, 5).Result()), ","), "3,2")
		equal(t, "EXISTS after the last pop", must(c.Exists(ctx(), l).Result()), 0)
		if err := c.LPop(ctx(), l).Err(); !errors.Is(err, redis.Nil) {
			t.Errorf("LPOP of a missing list: %v, want redis.Nil", err)
		}

		must(c.RPush(ctx(), p+"src", "x", "y").Result())
		equal(t, "LMOVE", must(c.LMove(ctx(), p+"src", p+"dst", "RIGHT", "LEFT").Result()), "y")
		equal(t, "RPOPLPUSH", must(c.RPopLPush(ctx(), p+"src", p+"dst").Result()), "x")
		equal(t, "dst", strings.Join(must(c.LRange(ctx(), p+"dst", 0, -1).Result()), ","), "x,y")
		key, vals := must2(c.LMPop(ctx(), "right", 5, p+"none", p+"dst").Result())
		equal(t, "LMPOP", key+" "+strings.Join(vals, ","), p+"dst y,x")

		must(c.Set(ctx(), p+"str", "v", 0).Result())
		err := c.LPush(ctx(), p+"str", "x").Err()
		if err == nil || !strings.HasPrefix(err.Error(), "WRONGTYPE") {
			t.Errorf("LPUSH to a string: %v, want WRONGTYPE", err)
		}
	})
}

func TestSets(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		s := p + "s"
		equal(t, "SADD", must(c.SAdd(ctx(), s, "a", "b", "c", "a").Result()), 3)
		equal(t, "SMEMBERS", sorted(must(c.SMembers(ctx(), s).Result())), "a,b,c")
		equal(t, "SMEMBERS map", len(must(c.SMembersMap(ctx(), s).Result())), 3)
		equal(t, "SISMEMBER", must(c.SIsMember(ctx(), s, "b").Result()), true)
		equal(t, "SMISMEMBER", fmt.Sprint(must(c.SMIsMember(ctx(), s, "a", "z").Result())), "[true false]")
		equal(t, "SCARD", must(c.SCard(ctx(), s).Result()), 3)
		equal(t, "SREM", must(c.SRem(ctx(), s, "c", "z").Result()), 1)
		equal(t, "SRANDMEMBER", len(must(c.SRandMemberN(ctx(), s, -5).Result())), 5)
		must(c.SAdd(ctx(), p+"t", "b", "c", "d").Result())
		equal(t, "SINTER", sorted(must(c.SInter(ctx(), s, p+"t").Result())), "b")
		equal(t, "SUNION", sorted(must(c.SUnion(ctx(), s, p+"t").Result())), "a,b,c,d")
		equal(t, "SDIFF", sorted(must(c.SDiff(ctx(), s, p+"t").Result())), "a")
		equal(t, "SINTERCARD", must(c.SInterCard(ctx(), 0, s, p+"t").Result()), 1)
		equal(t, "SUNIONSTORE", must(c.SUnionStore(ctx(), p+"u", s, p+"t").Result()), 4)
		equal(t, "SINTERSTORE", must(c.SInterStore(ctx(), p+"i", s, p+"t").Result()), 1)
		equal(t, "SDIFFSTORE", must(c.SDiffStore(ctx(), p+"d", p+"t", s).Result()), 2)
		equal(t, "SMOVE", must(c.SMove(ctx(), p+"u", p+"moved", "d").Result()), true)
		popped := must(c.SPopN(ctx(), p+"u", 10).Result())
		equal(t, "SPOP count", sorted(popped), "a,b,c")
		if err := c.SPop(ctx(), p+"u").Err(); !errors.Is(err, redis.Nil) {
			t.Errorf("SPOP of an empty set: %v, want redis.Nil", err)
		}

		for i := 0; i < 300; i++ {
			must(c.SAdd(ctx(), p+"big", i).Result())
		}
		seen := map[string]bool{}
		iter := c.SScan(ctx(), p+"big", 0, "1*", 25).Iterator()
		for iter.Next(ctx()) {
			seen[iter.Val()] = true
		}
		must(0, iter.Err())
		equal(t, "SSCAN MATCH 1*", len(seen), 111)
	})
}

func TestSortedSets(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		z := p + "z"
		equal(t, "ZADD", must(c.ZAdd(ctx(), z,
			redis.Z{Score: 1, Member: "one"},
			redis.Z{Score: 2, Member: "two"},
			redis.Z{Score: 3, Member: "three"}).Result()), 3)
		equal(t, "ZADD NX", must(c.ZAddNX(ctx(), z, redis.Z{Score: 9, Member: "one"}).Result()), 0)
		equal(t, "ZADD GT CH", must(c.ZAddArgs(ctx(), z, redis.ZAddArgs{GT: true, Ch: true,
			Members: []redis.Z{{Score: 0, Member: "two"}, {Score: 4, Member: "three"}}}).Result()), 1)
		equal(t, "ZADD INCR", must(c.ZAddArgsIncr(ctx(), z, redis.ZAddArgs{
			Members: []redis.Z{{Score: 0.5, Member: "one"}}}).Result()), 1.5)
		equal(t, "ZSCORE", must(c.ZScore(ctx(), z, "three").Result()), 4.0)
		if err := c.ZScore(ctx(), z, "nope").Err(); !errors.Is(err, redis.Nil) {
			t.Errorf("ZSCORE of a missing member: %v, want redis.Nil", err)
		}
		equal(t, "ZMSCORE", fmt.Sprint(must(c.ZMScore(ctx(), z, "one", "nope", "two").Result())), "[1.5 0 2]")
		equal(t, "ZINCRBY", must(c.ZIncrBy(ctx(), z, 1, "two").Result()), 3.0)
		equal(t, "ZCARD", must(c.ZCard(ctx(), z).Result()), 3)
		equal(t, "ZRANK", must(c.ZRank(ctx(), z, "three").Result()), 2)
		equal(t, "ZREVRANK", must(c.ZRevRank(ctx(), z, "three").Result()), 0)
		rs := must(c.ZRankWithScore(ctx(), z, "two").Result())
		equal(t, "ZRANK WITHSCORE", fmt.Sprintf("%d %v", rs.Rank, rs.Score), "1 3")
		if err := c.ZRank(ctx(), z, "nope").Err(); !errors.Is(err, redis.Nil) {
			t.Errorf("ZRANK of a missing member: %v, want redis.Nil", err)
		}

		equal(t, "ZRANGE", strings.Join(must(c.ZRange(ctx(), z, 0, -1).Result()), ","), "one,two,three")
		equal(t, "ZRANGE WITHSCORES", fmt.Sprint(must(c.ZRangeWithScores(ctx(), z, 0, -1).Result())),
			"[{1.5 one} {3 two} {4 three}]")
		equal(t, "ZREVRANGE", strings.Join(must(c.ZRevRange(ctx(), z, 0, 0).Result()), ","), "three")
		equal(t, "ZRANGEBYSCORE", strings.Join(must(c.ZRangeByScore(ctx(), z,
			&redis.ZRangeBy{Min: "(1.5", Max: "+inf"}).Result()), ","), "two,three")
		// (go-redis releases disagree on the Start/Stop order for Rev with
		// ByScore, so REV goes through ZREVRANGEBYSCORE.)
		equal(t, "ZRANGE BYSCORE LIMIT", strings.Join(must(c.ZRangeArgs(ctx(), redis.ZRangeArgs{
			Key: z, Start: "-inf", Stop: "+inf", ByScore: true, Offset: 1, Count: 1}).Result()), ","), "two")
		equal(t, "ZREVRANGEBYSCORE LIMIT", strings.Join(must(c.ZRevRangeByScore(ctx(), z,
			&redis.ZRangeBy{Min: "-inf", Max: "+inf", Offset: 1, Count: 1}).Result()), ","), "two")
		equal(t, "ZRANGESTORE", must(c.ZRangeStore(ctx(), p+"copy", redis.ZRangeArgs{Key: z, Start: 0, Stop: 1}).Result()), 2)
		equal(t, "ZCOUNT", must(c.ZCount(ctx(), z, "2", "4").Result()), 2)

		must(c.ZAdd(ctx(), p+"lex", redis.Z{Member: "a"}, redis.Z{Member: "b"}, redis.Z{Member: "c"}).Result())
		equal(t, "ZRANGEBYLEX", strings.Join(must(c.ZRangeByLex(ctx(), p+"lex", &redis.ZRangeBy{Min: "[b", Max: "+"}).Result()), ","), "b,c")
		equal(t, "ZLEXCOUNT", must(c.ZLexCount(ctx(), p+"lex", "-", "(c").Result()), 2)
		equal(t, "ZREMRANGEBYLEX", must(c.ZRemRangeByLex(ctx(), p+"lex", "[a", "[a").Result()), 1)

		must(c.ZAdd(ctx(), p+"other", redis.Z{Score: 10, Member: "two"}, redis.Z{Score: 20, Member: "four"}).Result())
		equal(t, "ZUNIONSTORE", must(c.ZUnionStore(ctx(), p+"u", &redis.ZStore{Keys: []string{z, p + "other"}}).Result()), 4)
		equal(t, "ZSCORE in union", must(c.ZScore(ctx(), p+"u", "two").Result()), 13.0)
		equal(t, "ZINTERSTORE MAX", must(c.ZInterStore(ctx(), p+"i", &redis.ZStore{
			Keys: []string{z, p + "other"}, Weights: []float64{2, 1}, Aggregate: "MAX"}).Result()), 1)
		equal(t, "ZSCORE in inter", must(c.ZScore(ctx(), p+"i", "two").Result()), 10.0)
		equal(t, "ZINTER WITHSCORES", fmt.Sprint(must(c.ZInterWithScores(ctx(), &redis.ZStore{Keys: []string{z, p + "other"}}).Result())), "[{13 two}]")
		equal(t, "ZUNION", strings.Join(must(c.ZUnion(ctx(), redis.ZStore{Keys: []string{z, p + "other"}}).Result()), ","), "one,three,two,four")
		equal(t, "ZDIFF", strings.Join(must(c.ZDiff(ctx(), z, p+"other").Result()), ","), "one,three")
		equal(t, "ZDIFF WITHSCORES", fmt.Sprint(must(c.ZDiffWithScores(ctx(), z, p+"other").Result())), "[{1.5 one} {4 three}]")
		equal(t, "ZINTERCARD", must(c.ZInterCard(ctx(), 0, z, p+"other").Result()), 1)
		equal(t, "ZRANDMEMBER", len(must(c.ZRandMember(ctx(), z, -4).Result())), 4)

		equal(t, "ZPOPMIN", fmt.Sprint(must(c.ZPopMin(ctx(), z).Result())), "[{1.5 one}]")
		equal(t, "ZPOPMAX 2", fmt.Sprint(must(c.ZPopMax(ctx(), p+"u", 2).Result())), "[{20 four} {13 two}]")
		key, popped := must2(c.ZMPop(ctx(), "min", 1, p+"none", z).Result())
		equal(t, "ZMPOP", fmt.Sprint(key, popped), fmt.Sprint(z, []redis.Z{{Score: 3, Member: "two"}}))
		equal(t, "ZREM", must(c.ZRem(ctx(), z, "three", "nope").Result()), 1)
		equal(t, "EXISTS after the last ZREM", must(c.Exists(ctx(), z).Result()), 0)

		// Infinite and very small scores.
		must(c.ZAdd(ctx(), p+"inf", redis.Z{Score: math.Inf(1), Member: "top"}, redis.Z{Score: math.Inf(-1), Member: "bottom"},
			redis.Z{Score: 1e-300, Member: "tiny"}).Result())
		equal(t, "+inf score", must(c.ZScore(ctx(), p+"inf", "top").Result()), math.Inf(1))
		equal(t, "-inf score", must(c.ZScore(ctx(), p+"inf", "bottom").Result()), math.Inf(-1))
		equal(t, "tiny score", must(c.ZScore(ctx(), p+"inf", "tiny").Result()), 1e-300)
		equal(t, "scores with inf", fmt.Sprint(must(c.ZRangeWithScores(ctx(), p+"inf", 0, -1).Result())),
			"[{-Inf bottom} {1e-300 tiny} {+Inf top}]")
		equal(t, "ZREMRANGEBYSCORE", must(c.ZRemRangeByScore(ctx(), p+"inf", "-inf", "(1").Result()), 2)
		equal(t, "ZREMRANGEBYRANK", must(c.ZRemRangeByRank(ctx(), p+"inf", 0, -1).Result()), 1)

		for i := 0; i < 300; i++ {
			must(c.ZAdd(ctx(), p+"big", redis.Z{Score: float64(i), Member: fmt.Sprint(i)}).Result())
		}
		var n int
		iter := c.ZScan(ctx(), p+"big", 0, "", 40).Iterator()
		for iter.Next(ctx()) {
			n++
		}
		must(0, iter.Err())
		equal(t, "ZSCAN members and scores", n, 600)
	})
}

func TestKeysAndExpiry(t *testing.T) {
	eachProtocol(t, func(t *testing.T, c *redis.Client, p string) {
		must(c.Set(ctx(), p+"k", "v", 100*time.Second).Result())
		if ttl := must(c.TTL(ctx(), p+"k").Result()); ttl < 90*time.Second || ttl > 100*time.Second {
			t.Errorf("TTL = %v", ttl)
		}
		if pttl := must(c.PTTL(ctx(), p+"k").Result()); pttl < 90*time.Second || pttl > 100*time.Second {
			t.Errorf("PTTL = %v", pttl)
		}
		if at := must(c.ExpireTime(ctx(), p+"k").Result()); at < time.Duration(time.Now().Unix())*time.Second {
			t.Errorf("EXPIRETIME = %v", at)
		}
		equal(t, "PERSIST", must(c.Persist(ctx(), p+"k").Result()), true)
		equal(t, "TTL without expiry", must(c.TTL(ctx(), p+"k").Result()), time.Duration(-1))
		equal(t, "TTL of a missing key", must(c.TTL(ctx(), p+"none").Result()), time.Duration(-2))
		equal(t, "EXPIRE NX", must(c.ExpireNX(ctx(), p+"k", time.Hour).Result()), true)
		equal(t, "EXPIRE NX again", must(c.ExpireNX(ctx(), p+"k", time.Hour).Result()), false)
		equal(t, "EXPIRE GT", must(c.ExpireGT(ctx(), p+"k", time.Minute).Result()), false)
		equal(t, "EXPIREAT", must(c.ExpireAt(ctx(), p+"k", time.Now().Add(time.Hour)).Result()), true)
		equal(t, "PEXPIREAT", must(c.PExpireAt(ctx(), p+"k", time.Now().Add(time.Hour)).Result()), true)
		equal(t, "SET KEEPTTL", must(c.Set(ctx(), p+"k", "v2", redis.KeepTTL).Result()), "OK")
		if ttl := must(c.TTL(ctx(), p+"k").Result()); ttl <= 0 {
			t.Errorf("TTL after SET KEEPTTL = %v", ttl)
		}

		equal(t, "PEXPIRE", must(c.PExpire(ctx(), p+"k", 100*time.Millisecond).Result()), true)
		must(c.Set(ctx(), p+"short", "v", 50*time.Millisecond).Result())
		must(c.SetEx(ctx(), p+"setex", "v", time.Second).Result())
		time.Sleep(300 * time.Millisecond)
		equal(t, "EXISTS after expiry", must(c.Exists(ctx(), p+"k", p+"short").Result()), 0)
		equal(t, "EXISTS setex", must(c.Exists(ctx(), p+"setex").Result()), 1)

		must(c.Set(ctx(), p+"r", "v", 0).Result())
		equal(t, "RENAME", must(c.Rename(ctx(), p+"r", p+"r2").Result()), "OK")
		equal(t, "RENAMENX", must(c.RenameNX(ctx(), p+"r2", p+"setex").Result()), false)
		equal(t, "TYPE", must(c.Type(ctx(), p+"r2").Result()), "string")
		equal(t, "TYPE missing", must(c.Type(ctx(), p+"r").Result()), "none")
		must(c.RPush(ctx(), p+"list", "x").Result())
		equal(t, "TYPE list", must(c.Type(ctx(), p+"list").Result()), "list")
		err := c.Rename(ctx(), p+"none", p+"x").Err()
		if err == nil || err.Error() != "ERR no such key" {
			t.Errorf("RENAME of a missing key: %v", err)
		}

		dump := must(c.Dump(ctx(), p+"r2").Result())
		equal(t, "RESTORE", must(c.Restore(ctx(), p+"restored", 0, dump).Result()), "OK")
		equal(t, "restored", must(c.Get(ctx(), p+"restored").Result()), "v")
		err = c.Restore(ctx(), p+"restored", 0, dump).Err()
		if err == nil || !strings.HasPrefix(err.Error(), "BUSYKEY") {
			t.Errorf("RESTORE over an existing key: %v, want BUSYKEY", err)
		}
		equal(t, "RESTORE REPLACE", must(c.RestoreReplace(ctx(), p+"restored", time.Minute, dump).Result()), "OK")

		equal(t, "COPY", must(c.Copy(ctx(), p+"r2", p+"copy", 0, false).Result()), 1)
		equal(t, "COPY existing", must(c.Copy(ctx(), p+"r2", p+"copy", 0, false).Result()), 0)
		equal(t, "TOUCH", must(c.Touch(ctx(), p+"r2", p+"copy", p+"none").Result()), 2)
		equal(t, "UNLINK", must(c.Unlink(ctx(), p+"copy").Result()), 1)
		equal(t, "DEL", must(c.Del(ctx(), p+"r2", p+"restored", p+"none").Result()), 2)
		if _, err := c.RandomKey(ctx()).Result(); err != nil {
			t.Errorf("RANDOMKEY: %v", err)
		}
		if n := must(c.ObjectIdleTime(ctx(), p+"list").Result()); n < 0 {
			t.Errorf("OBJECT IDLETIME = %v", n)
		}
	})
}
