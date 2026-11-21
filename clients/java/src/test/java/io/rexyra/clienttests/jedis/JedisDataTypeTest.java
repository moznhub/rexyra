package io.rexyra.clienttests.jedis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.rexyra.clienttests.RexyraServer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.RedisProtocol;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.args.GeoUnit;
import redis.clients.jedis.args.ListDirection;
import redis.clients.jedis.args.ListPosition;
import redis.clients.jedis.params.GetExParams;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.params.ZParams;
import redis.clients.jedis.resps.GeoRadiusResponse;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.Tuple;
import redis.clients.jedis.util.KeyValue;

/**
 * The basics of every data type through Jedis's pooled client, with each
 * protocol Jedis offers.
 */
class JedisDataTypeTest {
    private static String prefix() {
        return RexyraServer.prefix("jedis");
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void strings(RedisProtocol protocol) {
        try (UnifiedJedis jedis = JedisSupport.pooled(protocol)) {
            String p = prefix();

            assertEquals("OK", jedis.set(p + "a", "hello"));
            assertEquals("hello", jedis.get(p + "a"));
            assertNull(jedis.set(p + "a", "other", SetParams.setParams().nx()));
            assertEquals(1, jedis.setnx(p + "b", "x"));
            assertEquals(0, jedis.setnx(p + "b", "y"));
            assertEquals(11, jedis.append(p + "a", " world"));
            assertEquals("world", jedis.getrange(p + "a", 6, -1));
            assertEquals(11, jedis.strlen(p + "a"));

            assertEquals(5, jedis.incrBy(p + "n", 5));
            assertEquals(3, jedis.decrBy(p + "n", 2));
            assertEquals(4, jedis.incr(p + "n"));
            assertEquals(4.5, jedis.incrByFloat(p + "n", 0.5));

            assertEquals("OK", jedis.mset(p + "m1", "1", p + "m2", "2"));
            assertEquals(Arrays.asList("1", null, "2"), jedis.mget(p + "m1", p + "missing", p + "m2"));
            assertEquals(0, jedis.msetnx(p + "m1", "x", p + "m3", "3"));

            assertEquals("hello world", jedis.setGet(p + "a", "replaced"));
            assertEquals("replaced", jedis.getEx(p + "a", GetExParams.getExParams().ex(100)));
            long ttl = jedis.ttl(p + "a");
            assertTrue(ttl > 90 && ttl <= 100, "ttl " + ttl);
            assertEquals("replaced", jedis.getDel(p + "a"));
            assertFalse(jedis.exists(p + "a"));

            assertEquals("OK", jedis.setex(p + "ex", 100, "v"));
            assertEquals("OK", jedis.set(p + "px", "v", SetParams.setParams().px(100_000).nx()));
            assertTrue(jedis.pttl(p + "px") > 90_000);

            byte[] key = (p + "bin").getBytes(StandardCharsets.UTF_8);
            byte[] bytes = {0, 1, 2, (byte) 255, 13, 10};
            jedis.set(key, bytes);
            assertArrayEquals(bytes, jedis.get(key));

            String big = "x".repeat(1 << 20);
            jedis.set(p + "big", big);
            assertEquals(big, jedis.get(p + "big"));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void keysAndExpiry(RedisProtocol protocol) throws InterruptedException {
        try (UnifiedJedis jedis = JedisSupport.pooled(protocol)) {
            String p = prefix();

            jedis.setex(p + "k", 100, "v");
            long ttl = jedis.ttl(p + "k");
            assertTrue(ttl > 90 && ttl <= 100, "ttl " + ttl);
            assertTrue(jedis.expireTime(p + "k") > System.currentTimeMillis() / 1000);
            assertEquals(1, jedis.persist(p + "k"));
            assertEquals(-1, jedis.ttl(p + "k"));
            assertEquals(-2, jedis.ttl(p + "nothing"));

            assertEquals(1, jedis.pexpire(p + "k", 100));
            Thread.sleep(300);
            // Docker's clock on Windows can lag a little: allow up to two seconds.
            for (int i = 0; i < 20 && jedis.exists(p + "k"); i++) {
                Thread.sleep(100);
            }
            assertFalse(jedis.exists(p + "k"));

            jedis.set(p + "short", "v", SetParams.setParams().px(50));
            Thread.sleep(200);
            for (int i = 0; i < 20 && jedis.get(p + "short") != null; i++) {
                Thread.sleep(100);
            }
            assertNull(jedis.get(p + "short"));

            jedis.set(p + "r", "v");
            assertEquals("OK", jedis.rename(p + "r", p + "r2"));
            assertEquals("string", jedis.type(p + "r2"));
            assertEquals("none", jedis.type(p + "r"));
            assertEquals(0, jedis.renamenx(p + "r2", p + "r2"));
            assertTrue(jedis.copy(p + "r2", p + "copy", false));
            assertEquals("v", jedis.get(p + "copy"));
            assertEquals(2, jedis.exists(p + "r2", p + "copy"));
            assertEquals(2, jedis.touch(p + "r2", p + "copy"));
            assertEquals(1, jedis.unlink(p + "copy"));

            byte[] dump = jedis.dump(p + "r2");
            assertNotNull(dump);
            assertEquals("OK", jedis.restore(p + "restored", 0, dump));
            assertEquals("v", jedis.get(p + "restored"));

            for (int i = 0; i < 25; i++) {
                jedis.set(p + "scan:" + i, Integer.toString(i));
            }
            Set<String> found = new HashSet<>();
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams params = new ScanParams().match(p + "scan:*").count(7);
            do {
                ScanResult<String> page = jedis.scan(cursor, params);
                found.addAll(page.getResult());
                cursor = page.getCursor();
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            assertEquals(25, found.size());
            assertEquals(25, jedis.keys(p + "scan:*").size());
            assertEquals(25, jedis.del(found.toArray(new String[0])));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void hashes(RedisProtocol protocol) {
        try (UnifiedJedis jedis = JedisSupport.pooled(protocol)) {
            String p = prefix();

            assertEquals(3, jedis.hset(p + "h", Map.of("a", "1", "b", "2", "c", "3")));
            assertEquals("2", jedis.hget(p + "h", "b"));
            assertEquals(3, jedis.hlen(p + "h"));
            assertEquals(11, jedis.hincrBy(p + "h", "a", 10));
            assertEquals(2.5, jedis.hincrByFloat(p + "h", "f", 2.5));
            assertTrue(jedis.hexists(p + "h", "c"));
            assertEquals(1, jedis.hdel(p + "h", "c"));
            assertEquals(0, jedis.hsetnx(p + "h", "a", "x"));
            assertEquals(2, jedis.hstrlen(p + "h", "a"));

            assertEquals(Map.of("a", "11", "b", "2", "f", "2.5"), jedis.hgetAll(p + "h"));
            assertEquals(Set.of("a", "b", "f"), new HashSet<>(jedis.hkeys(p + "h")));
            assertEquals(Set.of("11", "2", "2.5"), new HashSet<>(jedis.hvals(p + "h")));
            assertEquals(Arrays.asList("11", null), jedis.hmget(p + "h", "a", "zz"));
            assertTrue(Set.of("a", "b", "f").contains(jedis.hrandfield(p + "h")));
            assertEquals(2, jedis.hrandfieldWithValues(p + "h", 2).size());

            Map<String, String> big = new HashMap<>();
            for (int i = 0; i < 300; i++) {
                big.put("f" + i, Integer.toString(i));
            }
            jedis.hset(p + "big", big);
            Map<String, String> scanned = new HashMap<>();
            String cursor = ScanParams.SCAN_POINTER_START;
            do {
                ScanResult<Map.Entry<String, String>> page = jedis.hscan(p + "big", cursor, new ScanParams().count(50));
                page.getResult().forEach(e -> scanned.put(e.getKey(), e.getValue()));
                cursor = page.getCursor();
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            assertEquals(big, scanned);
        }
    }

    /** Field expiry, new in Redis 7.4. */
    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void hashFieldExpiry(RedisProtocol protocol) {
        try (UnifiedJedis jedis = JedisSupport.pooled(protocol)) {
            String p = prefix();
            jedis.hset(p + "h", Map.of("a", "1", "b", "2"));
            assertEquals(List.of(1L, -2L), jedis.hexpire(p + "h", 100, "a", "nope"));
            List<Long> ttl = jedis.httl(p + "h", "a", "b");
            assertTrue(ttl.get(0) > 90 && ttl.get(0) <= 100, "ttl " + ttl);
            assertEquals(-1L, ttl.get(1));
            assertEquals(List.of(1L), jedis.hpersist(p + "h", "a"));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void lists(RedisProtocol protocol) throws Exception {
        try (UnifiedJedis jedis = JedisSupport.pooled(protocol)) {
            String p = prefix();

            assertEquals(3, jedis.rpush(p + "l", "a", "b", "c"));
            assertEquals(4, jedis.lpush(p + "l", "z"));
            assertEquals(List.of("z", "a", "b", "c"), jedis.lrange(p + "l", 0, -1));
            assertEquals("b", jedis.lindex(p + "l", 2));
            assertEquals(2L, jedis.lpos(p + "l", "b"));
            assertEquals("z", jedis.lpop(p + "l"));
            assertEquals("c", jedis.rpop(p + "l"));
            assertEquals("b", jedis.lmove(p + "l", p + "l2", ListDirection.RIGHT, ListDirection.LEFT));
            assertEquals(2, jedis.linsert(p + "l", ListPosition.AFTER, "a", "after"));
            assertEquals("OK", jedis.lset(p + "l", 1, "set"));
            assertEquals("OK", jedis.ltrim(p + "l", 0, 0));
            assertEquals(List.of("a"), jedis.lrange(p + "l", 0, -1));
            assertEquals(1, jedis.llen(p + "l2"));
            assertEquals(1, jedis.lrem(p + "l2", 0, "b"));

            jedis.rpush(p + "m", "1", "2", "3");
            KeyValue<String, List<String>> popped = jedis.lmpop(ListDirection.LEFT, 2, p + "empty", p + "m");
            assertEquals(p + "m", popped.getKey());
            assertEquals(List.of("1", "2"), popped.getValue());
            assertEquals(List.of("3"), jedis.lpop(p + "m", 5));

            // A blocking pop that a push from another connection wakes up.
            try (UnifiedJedis other = JedisSupport.pooled(protocol)) {
                CompletableFuture<KeyValue<String, String>> waiting =
                        CompletableFuture.supplyAsync(() -> jedis.brpop(5.0, p + "q"));
                Thread.sleep(200);
                other.lpush(p + "q", "job");
                KeyValue<String, String> job = waiting.get(10, TimeUnit.SECONDS);
                assertEquals(p + "q", job.getKey());
                assertEquals("job", job.getValue());
            }
            assertNull(jedis.blpop(0.1, p + "nothing"));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void sets(RedisProtocol protocol) {
        try (UnifiedJedis jedis = JedisSupport.pooled(protocol)) {
            String p = prefix();

            assertEquals(3, jedis.sadd(p + "a", "1", "2", "3"));
            assertEquals(2, jedis.sadd(p + "b", "3", "4"));
            assertTrue(jedis.sismember(p + "a", "2"));
            assertEquals(List.of(true, false), jedis.smismember(p + "a", "1", "9"));
            assertEquals(3, jedis.scard(p + "a"));
            assertEquals(Set.of("1", "2", "3"), jedis.smembers(p + "a"));

            assertEquals(Set.of("1", "2", "3", "4"), jedis.sunion(p + "a", p + "b"));
            assertEquals(Set.of("3"), jedis.sinter(p + "a", p + "b"));
            assertEquals(Set.of("1", "2"), jedis.sdiff(p + "a", p + "b"));
            assertEquals(2, jedis.sdiffstore(p + "d", p + "a", p + "b"));
            assertEquals(1, jedis.sintercard(p + "a", p + "b"));

            assertEquals(1, jedis.smove(p + "a", p + "b", "1"));
            assertEquals(1, jedis.srem(p + "b", "1"));
            assertNotNull(jedis.spop(p + "a"));
            assertEquals(1, jedis.scard(p + "a"));
            assertNotNull(jedis.srandmember(p + "b"));

            Set<String> scanned = new TreeSet<>();
            String cursor = ScanParams.SCAN_POINTER_START;
            do {
                ScanResult<String> page = jedis.sscan(p + "b", cursor, new ScanParams().count(1));
                scanned.addAll(page.getResult());
                cursor = page.getCursor();
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            assertEquals(Set.of("3", "4"), scanned);
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void sortedSets(RedisProtocol protocol) {
        try (UnifiedJedis jedis = JedisSupport.pooled(protocol)) {
            String p = prefix();

            assertEquals(3, jedis.zadd(p + "z", Map.of("a", 1.0, "b", 2.0, "c", 3.0)));
            assertEquals(2.0, jedis.zscore(p + "z", "b"));
            assertNull(jedis.zscore(p + "z", "nope"));
            assertEquals(1L, jedis.zrank(p + "z", "b"));
            assertEquals(0L, jedis.zrevrank(p + "z", "c"));
            assertEquals(4.5, jedis.zincrby(p + "z", 3.5, "a"));
            assertEquals(List.of("b", "c", "a"), jedis.zrange(p + "z", 0, -1));
            assertEquals(Arrays.asList(2.0, null), jedis.zmscore(p + "z", "b", "nope"));

            List<Tuple> withScores = jedis.zrangeByScoreWithScores(p + "z", 2, 4);
            assertEquals(List.of("b=2.0", "c=3.0"), render(withScores));
            assertEquals(List.of("b=2.0", "c=3.0", "a=4.5"), render(jedis.zrangeWithScores(p + "z", 0, -1)));
            assertEquals(2, jedis.zcount(p + "z", 2, 3.5));
            assertEquals(List.of("a", "c"), jedis.zrevrangeByScore(p + "z", "+inf", "-inf", 0, 2));

            jedis.zadd(p + "y", Map.of("b", 10.0, "d", 1.0));
            assertEquals(4, jedis.zunionstore(p + "u", p + "z", p + "y"));
            assertEquals(12.0, jedis.zscore(p + "u", "b"));
            assertEquals(List.of("b=12.0"), render(jedis.zinterWithScores(new ZParams(), p + "z", p + "y")));

            Tuple min = jedis.zpopmin(p + "z");
            assertEquals("b", min.getElement());
            assertEquals(2.0, min.getScore());
            assertEquals(2, jedis.zcard(p + "z"));
            assertEquals(1, jedis.zrem(p + "z", "c"));

            Set<String> scanned = new TreeSet<>();
            String cursor = ScanParams.SCAN_POINTER_START;
            do {
                ScanResult<Tuple> page = jedis.zscan(p + "u", cursor, new ScanParams().count(1));
                page.getResult().forEach(t -> scanned.add(t.getElement()));
                cursor = page.getCursor();
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
            assertEquals(Set.of("a", "b", "c", "d"), scanned);

            jedis.zadd(p + "lex", Map.of("apple", 0.0, "banana", 0.0, "cherry", 0.0));
            assertEquals(List.of("banana", "cherry"), jedis.zrangeByLex(p + "lex", "[b", "(d"));
        }
    }

    @Disabled("Rexyra: streams are not implemented (XADD: unknown command); documented")
    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void streams(RedisProtocol protocol) {
        try (UnifiedJedis jedis = JedisSupport.pooled(protocol)) {
            String p = prefix();
            String s = p + "s";

            StreamEntryID first = jedis.xadd(s, XAddParams.xAddParams(), Map.of("n", "1"));
            StreamEntryID second = jedis.xadd(s, XAddParams.xAddParams(), Map.of("n", "2"));
            assertTrue(second.compareTo(first) > 0);
            assertEquals(2, jedis.xlen(s));

            List<StreamEntry> range = jedis.xrange(s, "-", "+");
            assertEquals(2, range.size());
            assertEquals(first, range.get(0).getID());
            assertEquals(Map.of("n", "1"), range.get(0).getFields());

            assertEquals("OK", jedis.xgroupCreate(s, "g", new StreamEntryID(0, 0), false));
            List<Map.Entry<String, List<StreamEntry>>> read = jedis.xreadGroup("g", "c1",
                    XReadGroupParams.xReadGroupParams().count(10),
                    Map.of(s, StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY));
            assertEquals(1, read.size());
            assertEquals(s, read.get(0).getKey());
            assertEquals(2, read.get(0).getValue().size());
            assertEquals(2, jedis.xpending(s, "g").getTotal());
            assertEquals(1, jedis.xack(s, "g", first));
            assertEquals(1, jedis.xpending(s, "g").getTotal());
            assertEquals(1, jedis.xdel(s, second));
            assertEquals(1, jedis.xlen(s));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void bitmaps(RedisProtocol protocol) {
        try (UnifiedJedis jedis = JedisSupport.pooled(protocol)) {
            String p = prefix();
            assertFalse(jedis.setbit(p + "bits", 7, true));
            assertTrue(jedis.getbit(p + "bits", 7));
            assertEquals(1, jedis.bitcount(p + "bits"));
            assertEquals(7, jedis.bitpos(p + "bits", true));
        }
    }

    @Disabled("Rexyra: HyperLogLog is not implemented (PFADD: unknown command); documented")
    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void hyperLogLog(RedisProtocol protocol) {
        try (UnifiedJedis jedis = JedisSupport.pooled(protocol)) {
            String p = prefix();
            assertEquals(1, jedis.pfadd(p + "hll", "a", "b", "c", "a"));
            assertEquals(3, jedis.pfcount(p + "hll"));
        }
    }

    @Disabled("Rexyra: geospatial commands are not implemented (GEOADD: unknown command); documented")
    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void geo(RedisProtocol protocol) {
        try (UnifiedJedis jedis = JedisSupport.pooled(protocol)) {
            String p = prefix();
            assertEquals(2, jedis.geoadd(p + "geo", Map.of(
                    "Palermo", new GeoCoordinate(13.361389, 38.115556),
                    "Catania", new GeoCoordinate(15.087269, 37.502669))));
            assertEquals(166.2742, jedis.geodist(p + "geo", "Palermo", "Catania", GeoUnit.KM), 0.001);
            List<GeoRadiusResponse> near = jedis.geosearch(p + "geo", new GeoCoordinate(15, 37), 200, GeoUnit.KM);
            assertEquals(Set.of("Palermo", "Catania"),
                    near.stream().map(GeoRadiusResponse::getMemberByString).collect(Collectors.toSet()));
            GeoCoordinate pos = jedis.geopos(p + "geo", "Palermo").get(0);
            assertEquals(13.361389, pos.getLongitude(), 0.0001);
        }
    }

    private static List<String> render(List<Tuple> tuples) {
        List<String> out = new ArrayList<>();
        for (Tuple t : tuples) {
            out.add(t.getElement() + "=" + t.getScore());
        }
        return out;
    }
}
