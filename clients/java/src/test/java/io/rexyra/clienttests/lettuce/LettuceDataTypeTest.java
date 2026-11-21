package io.rexyra.clienttests.lettuce;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lettuce.core.Consumer;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.GetExArgs;
import io.lettuce.core.KeyValue;
import io.lettuce.core.LMPopArgs;
import io.lettuce.core.LMoveArgs;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.SetArgs;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.protocol.ProtocolVersion;
import io.rexyra.clienttests.RexyraServer;
import io.rexyra.clienttests.lettuce.LettuceSupport.Connected;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The basics of every data type through Lettuce's synchronous API, in RESP3
 * (Lettuce's default) and RESP2.
 */
class LettuceDataTypeTest {
    private static String prefix() {
        return RexyraServer.prefix("lettuce");
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void strings(ProtocolVersion protocol) {
        try (Connected c = new Connected(protocol)) {
            RedisCommands<String, String> redis = c.redis;
            String p = prefix();

            assertEquals("OK", redis.set(p + "a", "hello"));
            assertEquals("hello", redis.get(p + "a"));
            assertNull(redis.set(p + "a", "other", SetArgs.Builder.nx()));
            assertTrue(redis.setnx(p + "b", "x"));
            assertFalse(redis.setnx(p + "b", "y"));
            assertEquals(11, redis.append(p + "a", " world"));
            assertEquals("world", redis.getrange(p + "a", 6, -1));
            assertEquals(11, redis.strlen(p + "a"));

            assertEquals(5, redis.incrby(p + "n", 5));
            assertEquals(3, redis.decrby(p + "n", 2));
            assertEquals(4, redis.incr(p + "n"));
            assertEquals(4.5, redis.incrbyfloat(p + "n", 0.5));

            assertEquals("OK", redis.mset(Map.of(p + "m1", "1", p + "m2", "2")));
            List<KeyValue<String, String>> values = redis.mget(p + "m1", p + "missing", p + "m2");
            assertEquals(Arrays.asList("1", null, "2"),
                    values.stream().map(kv -> kv.getValueOrElse(null)).collect(Collectors.toList()));
            assertFalse(redis.msetnx(Map.of(p + "m1", "x", p + "m3", "3")));

            assertEquals("hello world", redis.setGet(p + "a", "replaced"));
            assertEquals("replaced", redis.getex(p + "a", GetExArgs.Builder.ex(100)));
            long ttl = redis.ttl(p + "a");
            assertTrue(ttl > 90 && ttl <= 100, "ttl " + ttl);
            assertEquals("replaced", redis.getdel(p + "a"));
            assertEquals(0, redis.exists(p + "a"));

            assertEquals("OK", redis.setex(p + "ex", 100, "v"));
            assertEquals("OK", redis.set(p + "px", "v", SetArgs.Builder.px(100_000).nx()));
            assertTrue(redis.pttl(p + "px") > 90_000);

            String big = "x".repeat(1 << 20);
            redis.set(p + "big", big);
            assertEquals(big, redis.get(p + "big"));

            // Binary values through the byte-array codec.
            try (StatefulRedisConnection<byte[], byte[]> binary = c.client.connect(ByteArrayCodec.INSTANCE)) {
                byte[] key = (p + "bin").getBytes(StandardCharsets.UTF_8);
                byte[] bytes = {0, 1, 2, (byte) 255, 13, 10};
                binary.sync().set(key, bytes);
                assertArrayEquals(bytes, binary.sync().get(key));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void keysAndExpiry(ProtocolVersion protocol) throws InterruptedException {
        try (Connected c = new Connected(protocol)) {
            RedisCommands<String, String> redis = c.redis;
            String p = prefix();

            redis.setex(p + "k", 100, "v");
            long ttl = redis.ttl(p + "k");
            assertTrue(ttl > 90 && ttl <= 100, "ttl " + ttl);
            assertTrue(redis.expiretime(p + "k") > System.currentTimeMillis() / 1000);
            assertTrue(redis.persist(p + "k"));
            assertEquals(-1, redis.ttl(p + "k"));
            assertEquals(-2, redis.ttl(p + "nothing"));

            assertTrue(redis.pexpire(p + "k", 100));
            Thread.sleep(300);
            // Docker's clock on Windows can lag a little: allow up to two seconds.
            for (int i = 0; i < 20 && redis.exists(p + "k") > 0; i++) {
                Thread.sleep(100);
            }
            assertEquals(0, redis.exists(p + "k"));

            redis.set(p + "short", "v", SetArgs.Builder.px(50));
            Thread.sleep(200);
            for (int i = 0; i < 20 && redis.get(p + "short") != null; i++) {
                Thread.sleep(100);
            }
            assertNull(redis.get(p + "short"));

            redis.set(p + "r", "v");
            assertEquals("OK", redis.rename(p + "r", p + "r2"));
            assertEquals("string", redis.type(p + "r2"));
            assertEquals("none", redis.type(p + "r"));
            assertFalse(redis.renamenx(p + "r2", p + "r2"));
            assertTrue(redis.copy(p + "r2", p + "copy"));
            assertEquals("v", redis.get(p + "copy"));
            assertEquals(2, redis.exists(p + "r2", p + "copy"));
            assertEquals(2, redis.touch(p + "r2", p + "copy"));
            assertEquals(1, redis.unlink(p + "copy"));

            byte[] dump = redis.dump(p + "r2");
            assertNotNull(dump);
            assertEquals("OK", redis.restore(p + "restored", 0, dump));
            assertEquals("v", redis.get(p + "restored"));

            for (int i = 0; i < 25; i++) {
                redis.set(p + "scan:" + i, Integer.toString(i));
            }
            Set<String> found = new HashSet<>();
            ScanIterator.scan(redis, ScanArgs.Builder.matches(p + "scan:*").limit(7)).forEachRemaining(found::add);
            assertEquals(25, found.size());
            assertEquals(25, redis.keys(p + "scan:*").size());
            assertEquals(25, redis.del(found.toArray(new String[0])));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void hashes(ProtocolVersion protocol) {
        try (Connected c = new Connected(protocol)) {
            RedisCommands<String, String> redis = c.redis;
            String p = prefix();

            assertEquals(3, redis.hset(p + "h", Map.of("a", "1", "b", "2", "c", "3")));
            assertEquals("2", redis.hget(p + "h", "b"));
            assertEquals(3, redis.hlen(p + "h"));
            assertEquals(11, redis.hincrby(p + "h", "a", 10));
            assertEquals(2.5, redis.hincrbyfloat(p + "h", "f", 2.5));
            assertTrue(redis.hexists(p + "h", "c"));
            assertEquals(1, redis.hdel(p + "h", "c"));
            assertFalse(redis.hsetnx(p + "h", "a", "x"));
            assertEquals(2, redis.hstrlen(p + "h", "a"));

            assertEquals(Map.of("a", "11", "b", "2", "f", "2.5"), redis.hgetall(p + "h"));
            assertEquals(Set.of("a", "b", "f"), new HashSet<>(redis.hkeys(p + "h")));
            assertEquals(Set.of("11", "2", "2.5"), new HashSet<>(redis.hvals(p + "h")));
            List<KeyValue<String, String>> fields = redis.hmget(p + "h", "a", "zz");
            assertEquals("11", fields.get(0).getValue());
            assertFalse(fields.get(1).hasValue());
            assertTrue(Set.of("a", "b", "f").contains(redis.hrandfield(p + "h")));
            assertEquals(2, redis.hrandfieldWithvalues(p + "h", 2).size());

            Map<String, String> big = new HashMap<>();
            for (int i = 0; i < 300; i++) {
                big.put("f" + i, Integer.toString(i));
            }
            redis.hset(p + "big", big);
            Map<String, String> scanned = new HashMap<>();
            ScanIterator.hscan(redis, p + "big", ScanArgs.Builder.limit(50))
                    .forEachRemaining(kv -> scanned.put(kv.getKey(), kv.getValue()));
            assertEquals(big, scanned);
        }
    }

    /** Field expiry, new in Redis 7.4. */
    @Disabled("Rexyra: hash field expiry is not implemented (HEXPIRE: unknown command)")
    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void hashFieldExpiry(ProtocolVersion protocol) {
        try (Connected c = new Connected(protocol)) {
            RedisCommands<String, String> redis = c.redis;
            String p = prefix();
            redis.hset(p + "h", Map.of("a", "1", "b", "2"));
            assertEquals(List.of(1L, -2L), redis.hexpire(p + "h", 100, "a", "nope"));
            List<Long> ttl = redis.httl(p + "h", "a", "b");
            assertTrue(ttl.get(0) > 90 && ttl.get(0) <= 100, "ttl " + ttl);
            assertEquals(-1L, ttl.get(1));
            assertEquals(List.of(1L), redis.hpersist(p + "h", "a"));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void lists(ProtocolVersion protocol) throws Exception {
        try (Connected c = new Connected(protocol); Connected other = new Connected(protocol)) {
            RedisCommands<String, String> redis = c.redis;
            String p = prefix();

            assertEquals(3, redis.rpush(p + "l", "a", "b", "c"));
            assertEquals(4, redis.lpush(p + "l", "z"));
            assertEquals(List.of("z", "a", "b", "c"), redis.lrange(p + "l", 0, -1));
            assertEquals("b", redis.lindex(p + "l", 2));
            assertEquals(2L, redis.lpos(p + "l", "b"));
            assertEquals("z", redis.lpop(p + "l"));
            assertEquals("c", redis.rpop(p + "l"));
            assertEquals("b", redis.lmove(p + "l", p + "l2", LMoveArgs.Builder.rightLeft()));
            assertEquals(2, redis.linsert(p + "l", false, "a", "after"));
            assertEquals("OK", redis.lset(p + "l", 1, "set"));
            assertEquals("OK", redis.ltrim(p + "l", 0, 0));
            assertEquals(List.of("a"), redis.lrange(p + "l", 0, -1));
            assertEquals(1, redis.llen(p + "l2"));
            assertEquals(1, redis.lrem(p + "l2", 0, "b"));

            redis.rpush(p + "m", "1", "2", "3");
            KeyValue<String, List<String>> popped = redis.lmpop(LMPopArgs.Builder.left().count(2), p + "empty", p + "m");
            assertEquals(p + "m", popped.getKey());
            assertEquals(List.of("1", "2"), popped.getValue());
            assertEquals(List.of("3"), redis.lpop(p + "m", 5));

            // A blocking pop on one connection that a push on another wakes up.
            CompletableFuture<KeyValue<String, String>> waiting =
                    c.connection.async().brpop(5.0, p + "q").toCompletableFuture();
            Thread.sleep(200);
            other.redis.lpush(p + "q", "job");
            KeyValue<String, String> job = waiting.get(10, TimeUnit.SECONDS);
            assertEquals(p + "q", job.getKey());
            assertEquals("job", job.getValue());
            assertNull(redis.blpop(0.1, p + "nothing"));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void sets(ProtocolVersion protocol) {
        try (Connected c = new Connected(protocol)) {
            RedisCommands<String, String> redis = c.redis;
            String p = prefix();

            assertEquals(3, redis.sadd(p + "a", "1", "2", "3"));
            assertEquals(2, redis.sadd(p + "b", "3", "4"));
            assertTrue(redis.sismember(p + "a", "2"));
            assertEquals(List.of(true, false), redis.smismember(p + "a", "1", "9"));
            assertEquals(3, redis.scard(p + "a"));
            assertEquals(Set.of("1", "2", "3"), redis.smembers(p + "a"));

            assertEquals(Set.of("1", "2", "3", "4"), redis.sunion(p + "a", p + "b"));
            assertEquals(Set.of("3"), redis.sinter(p + "a", p + "b"));
            assertEquals(Set.of("1", "2"), redis.sdiff(p + "a", p + "b"));
            assertEquals(2, redis.sdiffstore(p + "d", p + "a", p + "b"));
            assertEquals(1, redis.sintercard(p + "a", p + "b"));

            assertTrue(redis.smove(p + "a", p + "b", "1"));
            assertEquals(1, redis.srem(p + "b", "1"));
            assertNotNull(redis.spop(p + "a"));
            assertEquals(1, redis.scard(p + "a"));
            assertNotNull(redis.srandmember(p + "b"));

            Set<String> scanned = new HashSet<>();
            ScanIterator.sscan(redis, p + "b", ScanArgs.Builder.limit(1)).forEachRemaining(scanned::add);
            assertEquals(Set.of("3", "4"), scanned);
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void sortedSets(ProtocolVersion protocol) {
        try (Connected c = new Connected(protocol)) {
            RedisCommands<String, String> redis = c.redis;
            String p = prefix();

            assertEquals(3, redis.zadd(p + "z", ScoredValue.just(1, "a"), ScoredValue.just(2, "b"), ScoredValue.just(3, "c")));
            assertEquals(2.0, redis.zscore(p + "z", "b"));
            assertNull(redis.zscore(p + "z", "nope"));
            assertEquals(1L, redis.zrank(p + "z", "b"));
            assertEquals(0L, redis.zrevrank(p + "z", "c"));
            assertEquals(4.5, redis.zincrby(p + "z", 3.5, "a"));
            assertEquals(List.of("b", "c", "a"), redis.zrange(p + "z", 0, -1));
            assertEquals(Arrays.asList(2.0, null), redis.zmscore(p + "z", "b", "nope"));

            assertEquals(List.of("b=2.0", "c=3.0"), render(redis.zrangebyscoreWithScores(p + "z", Range.create(2, 4))));
            assertEquals(List.of("b=2.0", "c=3.0", "a=4.5"), render(redis.zrangeWithScores(p + "z", 0, -1)));
            assertEquals(2, redis.zcount(p + "z", Range.create(2, 3.5)));
            assertEquals(List.of("a", "c"),
                    redis.zrevrangebyscore(p + "z", Range.<Double>unbounded(), Limit.create(0, 2)));

            redis.zadd(p + "y", ScoredValue.just(10, "b"), ScoredValue.just(1, "d"));
            assertEquals(4, redis.zunionstore(p + "u", p + "z", p + "y"));
            assertEquals(12.0, redis.zscore(p + "u", "b"));
            assertEquals(List.of("b=12.0"), render(redis.zinterWithScores(p + "z", p + "y")));

            ScoredValue<String> min = redis.zpopmin(p + "z");
            assertEquals("b", min.getValue());
            assertEquals(2.0, min.getScore());
            assertEquals(2, redis.zcard(p + "z"));
            assertEquals(1, redis.zrem(p + "z", "c"));

            Set<String> scanned = new HashSet<>();
            ScanIterator.zscan(redis, p + "u", ScanArgs.Builder.limit(1)).forEachRemaining(v -> scanned.add(v.getValue()));
            assertEquals(Set.of("a", "b", "c", "d"), scanned);

            redis.zadd(p + "lex", ScoredValue.just(0, "apple"), ScoredValue.just(0, "banana"), ScoredValue.just(0, "cherry"));
            assertEquals(List.of("banana", "cherry"), redis.zrangebylex(p + "lex",
                    Range.from(Range.Boundary.including("b"), Range.Boundary.excluding("d"))));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void bitmaps(ProtocolVersion protocol) {
        try (Connected c = new Connected(protocol)) {
            RedisCommands<String, String> redis = c.redis;
            String p = prefix();
            assertEquals(0, redis.setbit(p + "bits", 7, 1));
            assertEquals(1, redis.getbit(p + "bits", 7));
            assertEquals(1, redis.bitcount(p + "bits"));
            assertEquals(7, redis.bitpos(p + "bits", true));
        }
    }

    @Disabled("Rexyra: streams are not implemented (XADD: unknown command); documented")
    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void streams(ProtocolVersion protocol) {
        try (Connected c = new Connected(protocol)) {
            RedisCommands<String, String> redis = c.redis;
            String s = prefix() + "s";

            String first = redis.xadd(s, Map.of("n", "1"));
            String second = redis.xadd(s, Map.of("n", "2"));
            assertEquals(2, redis.xlen(s));
            List<StreamMessage<String, String>> range = redis.xrange(s, Range.create("-", "+"));
            assertEquals(List.of(first, second), range.stream().map(StreamMessage::getId).collect(Collectors.toList()));
            assertEquals(Map.of("n", "1"), range.get(0).getBody());

            assertEquals("OK", redis.xgroupCreate(XReadArgs.StreamOffset.from(s, "0"), "g"));
            List<StreamMessage<String, String>> read = redis.xreadgroup(Consumer.from("g", "c1"),
                    XReadArgs.StreamOffset.lastConsumed(s));
            assertEquals(2, read.size());
            assertEquals(2, redis.xpending(s, "g").getCount());
            assertEquals(1, redis.xack(s, "g", first));
            assertEquals(1, redis.xpending(s, "g").getCount());
            assertEquals(1, redis.xdel(s, second));
            assertEquals(1, redis.xlen(s));
        }
    }

    @Disabled("Rexyra: HyperLogLog is not implemented (PFADD: unknown command); documented")
    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void hyperLogLog(ProtocolVersion protocol) {
        try (Connected c = new Connected(protocol)) {
            String p = prefix();
            assertEquals(1, c.redis.pfadd(p + "hll", "a", "b", "c", "a"));
            assertEquals(3, c.redis.pfcount(p + "hll"));
        }
    }

    @Disabled("Rexyra: geospatial commands are not implemented (GEOADD: unknown command); documented")
    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void geo(ProtocolVersion protocol) {
        try (Connected c = new Connected(protocol)) {
            RedisCommands<String, String> redis = c.redis;
            String p = prefix();
            assertEquals(2, redis.geoadd(p + "geo", 13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania"));
            assertEquals(166.2742, redis.geodist(p + "geo", "Palermo", "Catania", GeoArgs.Unit.km), 0.001);
            assertEquals(Set.of("Palermo", "Catania"), redis.georadius(p + "geo", 15, 37, 200, GeoArgs.Unit.km));
            assertEquals(13.361389, redis.geopos(p + "geo", "Palermo").get(0).getX().doubleValue(), 0.0001);
        }
    }

    private static List<String> render(List<ScoredValue<String>> values) {
        List<String> out = new ArrayList<>();
        for (ScoredValue<String> v : values) {
            out.add(v.getValue() + "=" + v.getScore());
        }
        return out;
    }
}
