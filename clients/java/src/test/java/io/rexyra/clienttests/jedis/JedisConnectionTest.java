package io.rexyra.clienttests.jedis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.rexyra.clienttests.RexyraServer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.RedisProtocol;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.args.FlushMode;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.ClientKillParams;

/**
 * Connecting, pools, transactions, pipelines, scripts, the server API,
 * reconnecting and Pub/Sub through Jedis.
 */
class JedisConnectionTest {
    private static final RexyraServer SERVER = JedisSupport.SERVER;

    private static String prefix() {
        return RexyraServer.prefix("jedis");
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void connectsCleanly(RedisProtocol protocol) {
        try (UnifiedJedis pooled = JedisSupport.pooled(protocol)) {
            assertEquals("PONG", pooled.ping());
        }
        try (Jedis jedis = new Jedis(JedisSupport.hostAndPort(),
                JedisSupport.config(protocol).clientName("jedis-conn").build())) {
            assertEquals("PONG", jedis.ping());
            assertEquals("jedis-conn", jedis.clientGetname());
            String info = jedis.clientInfo();
            // HELLO 3 when RESP3 is asked for, and the CLIENT SETINFO Jedis
            // sends on connect.
            assertTrue(info.contains(" resp=" + (protocol == RedisProtocol.RESP3 ? 3 : 2) + " "), info);
            assertTrue(info.contains(" lib-name=jedis "), info);
            assertTrue(info.contains(" name=jedis-conn "), info);
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void jedisPoolUnderConcurrency(RedisProtocol protocol) throws Exception {
        String p = prefix();
        ExecutorService threads = Executors.newFixedThreadPool(16);
        try (JedisPool pool = JedisSupport.pool(protocol)) {
            List<Future<?>> work = new ArrayList<>();
            for (int t = 0; t < 16; t++) {
                work.add(threads.submit(() -> {
                    for (int i = 0; i < 100; i++) {
                        try (Jedis jedis = pool.getResource()) {
                            jedis.incr(p + "count");
                            jedis.hincrBy(p + "h", Thread.currentThread().getName(), 1);
                        }
                    }
                }));
            }
            for (Future<?> f : work) {
                f.get(60, TimeUnit.SECONDS);
            }
            try (Jedis jedis = pool.getResource()) {
                assertEquals("1600", jedis.get(p + "count"));
                assertEquals(1600, jedis.hvals(p + "h").stream().mapToLong(Long::parseLong).sum());
            }
            assertTrue(pool.getNumIdle() >= 1);
        } finally {
            threads.shutdownNow();
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void selectDatabase(RedisProtocol protocol) {
        String p = prefix();
        try (UnifiedJedis db3 = JedisSupport.pooled(JedisSupport.config(protocol).database(3).build());
             UnifiedJedis db0 = JedisSupport.pooled(protocol)) {
            db3.set(p + "k", "three");
            assertNull(db0.get(p + "k"));
            assertEquals("three", db3.get(p + "k"));
        }
        try (Jedis jedis = JedisSupport.connect(protocol)) {
            assertEquals("OK", jedis.select(3));
            assertEquals(3, jedis.getDB());
            assertEquals("three", jedis.get(p + "k"));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void moveBetweenDatabases(RedisProtocol protocol) {
        String p = prefix();
        try (Jedis jedis = JedisSupport.connect(protocol)) {
            jedis.select(3);
            jedis.set(p + "k", "three");
            assertEquals(1, jedis.move(p + "k", 4));
            assertNull(jedis.get(p + "k"));
            jedis.select(4);
            assertEquals("three", jedis.get(p + "k"));
            assertEquals(0, jedis.move(p + "nothing", 3));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void wrongPasswordIsRefused(RedisProtocol protocol) {
        assumeTrue(SERVER.password != null, "the server has no password");
        DefaultJedisClientConfig.Builder config = DefaultJedisClientConfig.builder().password("not-the-password");
        if (protocol != null) {
            config.protocol(protocol);
        }
        try (UnifiedJedis jedis = JedisSupport.pooled(config.build())) {
            JedisException error = assertThrows(JedisException.class, jedis::ping);
            assertTrue(String.valueOf(messages(error)).contains("WRONGPASS"), messages(error));
        }

        // No password at all.
        DefaultJedisClientConfig.Builder none = DefaultJedisClientConfig.builder();
        if (protocol != null) {
            none.protocol(protocol);
        }
        // With a protocol set, the HELLO sent on connect is refused already.
        JedisException error = assertThrows(JedisException.class, () -> {
            try (Jedis jedis = new Jedis(JedisSupport.hostAndPort(), none.build())) {
                jedis.get("anything");
            }
        });
        assertTrue(messages(error).contains("NOAUTH"), messages(error));
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void transactionsWithWatch(RedisProtocol protocol) {
        String p = prefix();
        try (Jedis a = JedisSupport.connect(protocol); Jedis b = JedisSupport.connect(protocol)) {
            a.watch(p + "lock");
            Transaction tx = a.multi();
            Response<String> set = tx.set(p + "lock", "owner");
            Response<Long> incr = tx.incr(p + "count");
            List<Object> results = tx.exec();
            assertEquals(2, results.size());
            assertEquals("OK", set.get());
            assertEquals(1L, incr.get());

            // Another client changes the watched key: nothing runs.
            assertEquals("OK", a.watch(p + "lock"));
            b.set(p + "lock", "thief");
            Transaction aborted = a.multi();
            aborted.incr(p + "count");
            assertNull(aborted.exec());
            assertEquals("1", a.get(p + "count"));

            // UNWATCH, then DISCARD.
            a.watch(p + "lock");
            assertEquals("OK", a.unwatch());
            Transaction discarded = a.multi();
            discarded.incr(p + "count");
            assertEquals("OK", discarded.discard());
            assertEquals("1", a.get(p + "count"));

            // A command that fails inside EXEC fails alone.
            Transaction partial = a.multi();
            partial.set(p + "s", "text");
            Response<Long> bad = partial.incr(p + "s");
            Response<Long> good = partial.incr(p + "count");
            List<Object> mixed = partial.exec();
            assertEquals(3, mixed.size());
            assertInstanceOf(JedisDataException.class, mixed.get(1));
            assertThrows(JedisDataException.class, bad::get);
            assertEquals(2L, good.get());
        }

        // The pooled client's transaction.
        try (UnifiedJedis pooled = JedisSupport.pooled(protocol)) {
            var tx = pooled.multi();
            Response<String> set = tx.set(p + "pooled", "v");
            Response<String> get = tx.get(p + "pooled");
            tx.exec();
            assertEquals("OK", set.get());
            assertEquals("v", get.get());
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void pipelines(RedisProtocol protocol) {
        String p = prefix();
        try (UnifiedJedis pooled = JedisSupport.pooled(protocol)) {
            List<Response<String>> sets = new ArrayList<>();
            List<Response<String>> gets = new ArrayList<>();
            try (var pipeline = pooled.pipelined()) {
                for (int i = 0; i < 1000; i++) {
                    sets.add(pipeline.set(p + i, Integer.toString(i)));
                }
                for (int i = 0; i < 1000; i++) {
                    gets.add(pipeline.get(p + i));
                }
                pipeline.sync();
            }
            for (int i = 0; i < 1000; i++) {
                assertEquals("OK", sets.get(i).get());
                assertEquals(Integer.toString(i), gets.get(i).get());
            }
        }
        try (Jedis jedis = JedisSupport.connect(protocol)) {
            var pipeline = jedis.pipelined();
            pipeline.incr(p + "n");
            pipeline.hset(p + "h", "f", "v");
            pipeline.incr(p + "0");
            pipeline.lpush(p + "0", "wrong type");
            pipeline.zadd(p + "z", 1.5, "m");
            List<Object> all = pipeline.syncAndReturnAll();
            assertEquals(1L, all.get(0));
            assertEquals(1L, all.get(1));
            assertEquals(1L, all.get(2));
            assertInstanceOf(JedisDataException.class, all.get(3));
            assertEquals(1L, all.get(4));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void luaScripts(RedisProtocol protocol) {
        String p = prefix();
        try (UnifiedJedis jedis = JedisSupport.pooled(protocol)) {
            Object result = jedis.eval(
                    "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('INCRBY', KEYS[2], ARGV[2])",
                    List.of(p + "s", p + "n"), List.of("hello", "41"));
            assertEquals(41L, result);
            assertEquals("hello", jedis.get(p + "s"));

            String limiter = "local n = redis.call('INCR', KEYS[1]) "
                    + "if n == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end "
                    + "if n > tonumber(ARGV[2]) then return 0 else return 1 end";
            String sha = jedis.scriptLoad(limiter, p + "rate");
            assertEquals(40, sha.length());
            long allowed = 0;
            for (int i = 0; i < 8; i++) {
                allowed += (Long) jedis.evalsha(sha, List.of(p + "rate"), List.of("60000", "5"));
            }
            assertEquals(5, allowed);
            assertEquals(List.of(true, false), jedis.scriptExists(p + "rate", sha, "0".repeat(40)));

            JedisDataException missing = assertThrows(JedisDataException.class,
                    () -> jedis.evalsha("0".repeat(40), List.of(), List.of()));
            assertTrue(missing.getMessage().startsWith("NOSCRIPT"), missing.getMessage());

            // Replies of every Lua type.
            @SuppressWarnings("unchecked")
            List<Object> mixed = (List<Object>) jedis.eval("return {1, 'two', {3}, redis.status_reply('OK')}");
            assertEquals(1L, mixed.get(0));
            assertEquals("two", mixed.get(1));
            assertEquals(List.of(3L), mixed.get(2));
            assertEquals("OK", mixed.get(3));
            assertNull(jedis.eval("return nil"));
            assertEquals(1L, jedis.eval("return true"));

            JedisDataException error = assertThrows(JedisDataException.class,
                    () -> jedis.eval("return redis.error_reply('my error')"));
            assertTrue(error.getMessage().contains("my error"), error.getMessage());
            JedisDataException runtime = assertThrows(JedisDataException.class,
                    () -> jedis.eval("return redis.call('INCR', KEYS[1])", List.of(p + "s"), List.of()));
            assertTrue(runtime.getMessage().contains("not an integer"), runtime.getMessage());
        }
        try (Jedis jedis = JedisSupport.connect(protocol)) {
            String sha = jedis.scriptLoad("return ARGV[1]");
            assertEquals("x", jedis.evalsha(sha, 0, "x"));
            assertEquals(List.of(true), jedis.scriptExists(new String[] {sha}));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void serverApi(RedisProtocol protocol) {
        String p = prefix();
        try (Jedis jedis = JedisSupport.connect(protocol)) {
            assertTrue(jedis.info().contains("redis_version:"));
            assertTrue(jedis.info("server").contains("redis_version:"));
            // Jedis before 8 leaves the "txt:" prefix of the RESP3 verbatim string on.
            assertTrue(jedis.info("keyspace").contains("# Keyspace"));
            Map<String, String> maxmemory = jedis.configGet("maxmemory");
            assertTrue(maxmemory.containsKey("maxmemory"), maxmemory.toString());
            assertTrue(jedis.configGet("*max-*-entries*").size() >= 2);
            assertTrue(jedis.clientList().contains("id="));
            assertEquals("OK", jedis.clientSetname("jedis-server-api"));
            assertEquals("jedis-server-api", jedis.clientGetname());
            assertTrue(jedis.clientList().contains("name=jedis-server-api"));
            assertTrue(jedis.clientId() > 0);
            List<String> time = jedis.time();
            assertEquals(2, time.size());
            assertTrue(Math.abs(Long.parseLong(time.get(0)) - System.currentTimeMillis() / 1000) < 300);
            assertEquals("echo", jedis.echo("echo"));
            assertNotNull(jedis.slowlogGet(10));
            assertTrue(jedis.lastsave() >= 0);
            assertNull(jedis.memoryUsage(p + "nothing"));
            assertNotNull(jedis.commandCount());

            assertEquals("OK", jedis.select(5));
            jedis.set(p + "x", "1");
            assertTrue(jedis.memoryUsage(p + "x") > 0);
            assertTrue(jedis.dbSize() >= 1);
            assertEquals("OK", jedis.flushDB());
            assertEquals(0, jedis.dbSize());
            jedis.set(p + "y", "1");
            assertEquals("OK", jedis.flushDB(FlushMode.ASYNC));
            assertEquals(0, jedis.dbSize());
        }
        try (UnifiedJedis pooled = JedisSupport.pooled(protocol)) {
            assertTrue(pooled.dbSize() >= 0);
        }
    }

    @Test
    void reconnectsAfterClientKill() {
        String p = prefix();
        try (UnifiedJedis pooled = JedisSupport.pooled((RedisProtocol) null);
             JedisPool pool = JedisSupport.pool((RedisProtocol) null)) {
            pooled.set(p + "k", "before");
            try (Jedis jedis = pool.getResource()) {
                assertEquals("before", jedis.get(p + "k"));
            }

            long killed;
            try (Jedis admin = JedisSupport.connect(null)) {
                killed = admin.clientKill(ClientKillParams.clientKillParams().skipMe(ClientKillParams.SkipMe.YES));
            }
            assertTrue(killed >= 2, "killed " + killed);

            assertEquals("before", retry(() -> pooled.get(p + "k")));
            assertEquals("before", retry(() -> {
                try (Jedis jedis = pool.getResource()) {
                    return jedis.get(p + "k");
                }
            }));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.jedis.JedisSupport#protocols")
    void pubSub(RedisProtocol protocol) throws Exception {
        String p = prefix();
        String channel = p + "channel";
        BlockingQueue<String> received = new LinkedBlockingQueue<>();
        CountDownLatch subscribed = new CountDownLatch(1);
        CountDownLatch psubscribed = new CountDownLatch(1);
        JedisPubSub listener = new JedisPubSub() {
            @Override
            public void onMessage(String ch, String message) {
                received.add(ch + "|" + message);
            }

            @Override
            public void onPMessage(String pattern, String ch, String message) {
                received.add(pattern + "|" + ch + "|" + message);
            }

            @Override
            public void onSubscribe(String ch, int count) {
                subscribed.countDown();
            }

            @Override
            public void onPSubscribe(String pattern, int count) {
                psubscribed.countDown();
            }
        };

        ExecutorService thread = Executors.newSingleThreadExecutor();
        try (Jedis subscriber = JedisSupport.connect(protocol);
             UnifiedJedis publisher = JedisSupport.pooled(protocol);
             Jedis admin = JedisSupport.connect(protocol)) {
            Future<?> loop = thread.submit(() -> subscriber.subscribe(listener, channel));
            assertTrue(subscribed.await(5, TimeUnit.SECONDS));
            assertEquals(1, publisher.publish(channel, "hi"));
            assertEquals(channel + "|hi", received.poll(5, TimeUnit.SECONDS));
            assertEquals(Map.of(channel, 1L), admin.pubsubNumSub(channel));
            assertTrue(admin.pubsubChannels(p + "*").contains(channel));

            listener.psubscribe(p + "news.*");
            assertTrue(psubscribed.await(5, TimeUnit.SECONDS));
            assertEquals(1, publisher.publish(p + "news.sport", "goal"));
            assertEquals(p + "news.*|" + p + "news.sport|goal", received.poll(5, TimeUnit.SECONDS));
            assertEquals(1, admin.pubsubNumPat());

            listener.unsubscribe();
            listener.punsubscribe();
            loop.get(5, TimeUnit.SECONDS);
            assertEquals(0, publisher.publish(channel, "nobody"));
            // The connection is usable again after leaving Pub/Sub.
            assertEquals("PONG", subscriber.ping());
        } finally {
            thread.shutdownNow();
        }
    }

    private static String messages(Throwable error) {
        StringBuilder text = new StringBuilder();
        for (Throwable t = error; t != null; t = t.getCause()) {
            text.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage()).append(" / ");
        }
        return text.toString();
    }

    private interface Call<T> {
        T run();
    }

    private static <T> T retry(Call<T> call) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        JedisConnectionException last = null;
        while (System.nanoTime() < deadline) {
            try {
                return call.run();
            } catch (JedisConnectionException e) {
                last = e;
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(ie);
                }
            }
        }
        throw new AssertionError("did not reconnect within 15 seconds", last);
    }
}
