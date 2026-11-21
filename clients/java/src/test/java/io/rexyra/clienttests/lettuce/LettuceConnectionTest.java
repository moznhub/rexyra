package io.rexyra.clienttests.lettuce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.lettuce.core.FlushMode;
import io.lettuce.core.KillArgs;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import io.rexyra.clienttests.RexyraServer;
import io.rexyra.clienttests.lettuce.LettuceSupport.Connected;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Connecting, the three APIs, transactions, pipelining, scripts, the server
 * API, reconnecting, Pub/Sub and connection pooling through Lettuce.
 */
class LettuceConnectionTest {
    private static final RexyraServer SERVER = LettuceSupport.SERVER;

    private static String prefix() {
        return RexyraServer.prefix("lettuce");
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void connectsCleanly(ProtocolVersion protocol) {
        RedisURI uri = LettuceSupport.uri().withClientName("lettuce-conn").build();
        try (Connected c = new Connected(uri, protocol)) {
            assertEquals("PONG", c.redis.ping());
            assertEquals("lettuce-conn", c.redis.clientGetname());
            String info = c.redis.clientInfo();
            // HELLO 3 unless RESP2 is asked for (Lettuce would fall back to
            // RESP2 silently if HELLO failed), and the CLIENT SETINFO it sends.
            int resp = protocol == ProtocolVersion.RESP2 ? 2 : 3;
            assertTrue(info.contains(" resp=" + resp + " "), info);
            assertTrue(info.contains(" lib-name=Lettuce "), info);
            assertTrue(info.contains(" name=lettuce-conn "), info);
            assertTrue(c.redis.info("server").contains("redis_version:"));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void syncAsyncAndReactive(ProtocolVersion protocol) throws Exception {
        String p = prefix();
        try (Connected c = new Connected(protocol)) {
            assertEquals("OK", c.redis.set(p + "sync", "1"));

            RedisAsyncCommands<String, String> async = c.connection.async();
            RedisFuture<String> set = async.set(p + "async", "2");
            RedisFuture<Long> incr = async.incr(p + "sync");
            RedisFuture<Map<String, String>> hash = async.hgetall(p + "none");
            assertEquals("OK", set.get(10, TimeUnit.SECONDS));
            assertEquals(2L, incr.get(10, TimeUnit.SECONDS));
            assertEquals(Map.of(), hash.get(10, TimeUnit.SECONDS));

            RedisReactiveCommands<String, String> reactive = c.connection.reactive();
            assertEquals("OK", reactive.set(p + "reactive", "3").block(Duration.ofSeconds(10)));
            assertEquals("2", reactive.get(p + "async").block(Duration.ofSeconds(10)));
            assertNull(reactive.get(p + "missing").block(Duration.ofSeconds(10)));
            reactive.rpush(p + "list", "a", "b", "c").block(Duration.ofSeconds(10));
            assertEquals(List.of("a", "b", "c"),
                    reactive.lrange(p + "list", 0, -1).collectList().block(Duration.ofSeconds(10)));
            assertEquals(List.of("2", "2", "3"), reactive.mget(p + "sync", p + "async", p + "reactive")
                    .map(kv -> kv.getValue()).collectList().block(Duration.ofSeconds(10)));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void wrongPasswordIsRefused(ProtocolVersion protocol) {
        assumeTrue(SERVER.password != null, "the server has no password");
        RedisURI wrong = RedisURI.builder().withHost(SERVER.host).withPort(SERVER.port)
                .withPassword("not-the-password".toCharArray()).withTimeout(Duration.ofSeconds(5)).build();
        RedisClient client = LettuceSupport.client(wrong, protocol);
        try {
            RedisConnectionException error = assertThrows(RedisConnectionException.class, client::connect);
            assertTrue(messages(error).contains("WRONGPASS"), messages(error));
        } finally {
            LettuceSupport.shutdown(client);
        }

        // No password at all: Lettuce connects, and commands are refused.
        RedisURI none = RedisURI.builder().withHost(SERVER.host).withPort(SERVER.port)
                .withTimeout(Duration.ofSeconds(5)).build();
        RedisClient anonymous = LettuceSupport.client(none, protocol);
        try {
            RedisException error = assertThrows(RedisException.class, () -> {
                try (StatefulRedisConnection<String, String> connection = anonymous.connect()) {
                    connection.sync().get("anything");
                }
            });
            assertTrue(messages(error).contains("NOAUTH"), messages(error));
        } finally {
            LettuceSupport.shutdown(anonymous);
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void transactionsWithWatch(ProtocolVersion protocol) throws Exception {
        String p = prefix();
        try (Connected a = new Connected(protocol); Connected b = new Connected(protocol)) {
            RedisCommands<String, String> redis = a.redis;

            assertEquals("OK", redis.watch(p + "lock"));
            assertEquals("OK", redis.multi());
            assertNull(redis.set(p + "lock", "owner"));
            assertNull(redis.incr(p + "count"));
            TransactionResult result = redis.exec();
            assertFalse(result.wasDiscarded());
            assertEquals(2, result.size());
            assertEquals("OK", result.get(0));
            assertEquals(1L, (Long) result.get(1));

            // Another client changes the watched key: nothing runs.
            redis.watch(p + "lock");
            b.redis.set(p + "lock", "thief");
            redis.multi();
            redis.incr(p + "count");
            assertTrue(redis.exec().wasDiscarded());
            assertEquals("1", redis.get(p + "count"));

            // UNWATCH, then DISCARD.
            redis.watch(p + "lock");
            assertEquals("OK", redis.unwatch());
            redis.multi();
            redis.incr(p + "count");
            assertEquals("OK", redis.discard());
            assertEquals("1", redis.get(p + "count"));

            // A command that fails inside EXEC fails alone.
            redis.multi();
            redis.set(p + "s", "text");
            redis.incr(p + "s");
            redis.incr(p + "count");
            TransactionResult mixed = redis.exec();
            assertEquals(3, mixed.size());
            assertInstanceOf(RedisCommandExecutionException.class, mixed.get(1));
            assertEquals(2L, (Long) mixed.get(2));

            // The same through the async API.
            RedisAsyncCommands<String, String> async = a.connection.async();
            async.multi();
            RedisFuture<String> set = async.set(p + "async", "v");
            RedisFuture<String> get = async.get(p + "async");
            TransactionResult asyncResult = async.exec().get(10, TimeUnit.SECONDS);
            assertFalse(asyncResult.wasDiscarded());
            assertEquals("OK", set.get(10, TimeUnit.SECONDS));
            assertEquals("v", get.get(10, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void pipelining(ProtocolVersion protocol) {
        String p = prefix();
        try (Connected c = new Connected(protocol)) {
            RedisAsyncCommands<String, String> async = c.connection.async();
            c.connection.setAutoFlushCommands(false);
            List<RedisFuture<?>> futures = new ArrayList<>();
            List<RedisFuture<String>> gets = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                futures.add(async.set(p + i, Integer.toString(i)));
            }
            for (int i = 0; i < 1000; i++) {
                RedisFuture<String> get = async.get(p + i);
                gets.add(get);
                futures.add(get);
            }
            RedisFuture<Long> wrongType = async.lpush(p + "0", "x");
            c.connection.flushCommands();
            assertTrue(LettuceFutures.awaitAll(Duration.ofSeconds(30), futures.toArray(new RedisFuture[0])));
            c.connection.setAutoFlushCommands(true);
            for (int i = 0; i < 1000; i++) {
                assertEquals(Integer.toString(i), gets.get(i).toCompletableFuture().join());
            }
            Exception error = assertThrows(Exception.class, () -> wrongType.get(10, TimeUnit.SECONDS));
            assertTrue(messages(error).contains("WRONGTYPE"), messages(error));
            assertEquals("PONG", c.redis.ping());
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void luaScripts(ProtocolVersion protocol) {
        String p = prefix();
        try (Connected c = new Connected(protocol)) {
            RedisCommands<String, String> redis = c.redis;

            Long result = redis.eval(
                    "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('INCRBY', KEYS[2], ARGV[2])",
                    ScriptOutputType.INTEGER, new String[] {p + "s", p + "n"}, "hello", "41");
            assertEquals(41L, result);
            assertEquals("hello", redis.get(p + "s"));

            String limiter = "local n = redis.call('INCR', KEYS[1]) "
                    + "if n == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end "
                    + "if n > tonumber(ARGV[2]) then return 0 else return 1 end";
            String sha = redis.scriptLoad(limiter);
            assertEquals(redis.digest(limiter), sha);
            long allowed = 0;
            for (int i = 0; i < 8; i++) {
                Long one = redis.evalsha(sha, ScriptOutputType.INTEGER, new String[] {p + "rate"}, "60000", "5");
                allowed += one;
            }
            assertEquals(5, allowed);
            assertEquals(List.of(true, false), redis.scriptExists(sha, "0".repeat(40)));
            assertThrows(RedisNoScriptException.class,
                    () -> redis.evalsha("0".repeat(40), ScriptOutputType.VALUE));

            // Replies of every Lua type.
            List<Object> mixed = redis.eval("return {1, 'two', {3}, redis.status_reply('OK')}", ScriptOutputType.MULTI);
            assertEquals(1L, mixed.get(0));
            assertEquals("two", mixed.get(1));
            assertEquals(List.of(3L), mixed.get(2));
            assertEquals("OK", mixed.get(3));
            assertEquals("OK", redis.eval("return redis.status_reply('OK')", ScriptOutputType.STATUS));
            assertEquals(Boolean.TRUE, redis.eval("return true", ScriptOutputType.BOOLEAN));
            assertNull(redis.eval("return nil", ScriptOutputType.VALUE));

            RedisCommandExecutionException error = assertThrows(RedisCommandExecutionException.class,
                    () -> redis.eval("return redis.error_reply('my error')", ScriptOutputType.STATUS));
            assertTrue(error.getMessage().contains("my error"), error.getMessage());
            RedisCommandExecutionException runtime = assertThrows(RedisCommandExecutionException.class,
                    () -> redis.eval("return redis.call('INCR', KEYS[1])", ScriptOutputType.INTEGER, p + "s"));
            assertTrue(runtime.getMessage().contains("not an integer"), runtime.getMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void serverApi(ProtocolVersion protocol) {
        String p = prefix();
        try (Connected c = new Connected(protocol)) {
            RedisCommands<String, String> redis = c.redis;
            assertTrue(redis.info().contains("redis_version:"));
            assertTrue(redis.info("keyspace").startsWith("# Keyspace"));
            Map<String, String> maxmemory = redis.configGet("maxmemory");
            assertTrue(maxmemory.containsKey("maxmemory"), maxmemory.toString());
            assertTrue(redis.configGet("*max-*-entries*").size() >= 2);
            assertTrue(redis.clientList().contains("id="));
            assertEquals("OK", redis.clientSetname("lettuce-server-api"));
            assertEquals("lettuce-server-api", redis.clientGetname());
            assertTrue(redis.clientList().contains("name=lettuce-server-api"));
            assertTrue(redis.clientId() > 0);
            List<String> time = redis.time();
            assertEquals(2, time.size());
            assertTrue(Math.abs(Long.parseLong(time.get(0)) - System.currentTimeMillis() / 1000) < 300);
            assertEquals("echo", redis.echo("echo"));
            assertNotNull(redis.slowlogGet(10));
            assertNotNull(redis.lastsave());
            assertNull(redis.memoryUsage(p + "nothing"));
            assertTrue(redis.commandCount() > 100);

            assertEquals("OK", redis.select(5));
            redis.set(p + "x", "1");
            assertTrue(redis.memoryUsage(p + "x") > 0);
            assertTrue(redis.dbsize() >= 1);
            assertEquals("OK", redis.flushdb());
            assertEquals(0, redis.dbsize());
            redis.set(p + "y", "1");
            assertEquals("OK", redis.flushdb(FlushMode.ASYNC));
            assertEquals(0, redis.dbsize());
        }

        // A database chosen in the URI.
        try (Connected c = new Connected(LettuceSupport.uri().withDatabase(3).build(), protocol);
             Connected zero = new Connected(protocol)) {
            c.redis.set(p + "k", "three");
            assertNull(zero.redis.get(p + "k"));
            zero.redis.select(3);
            assertEquals("three", zero.redis.get(p + "k"));
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void reconnectsAfterClientKill(ProtocolVersion protocol) throws Exception {
        String p = prefix();
        try (Connected c = new Connected(LettuceSupport.uri().withDatabase(2).withClientName("victim").build(), protocol);
             Connected admin = new Connected(protocol)) {
            c.redis.set(p + "k", "before");
            long id = c.redis.clientId();
            assertEquals(1L, admin.redis.clientKill(KillArgs.Builder.id(id)));

            // Lettuce reconnects by itself, restoring the database and the
            // client name.
            String value = retry(() -> c.redis.get(p + "k"));
            assertEquals("before", value);
            assertTrue(c.connection.isOpen());
            assertTrue(c.redis.clientId() != id);
            assertEquals("victim", c.redis.clientGetname());
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void pubSub(ProtocolVersion protocol) throws Exception {
        String p = prefix();
        String channel = p + "channel";
        BlockingQueue<String> received = new LinkedBlockingQueue<>();
        RedisClient client = LettuceSupport.client(protocol);
        try (StatefulRedisPubSubConnection<String, String> sub = client.connectPubSub();
             StatefulRedisConnection<String, String> pub = client.connect()) {
            sub.addListener(new RedisPubSubAdapter<>() {
                @Override
                public void message(String ch, String message) {
                    received.add(ch + "|" + message);
                }

                @Override
                public void message(String pattern, String ch, String message) {
                    received.add(pattern + "|" + ch + "|" + message);
                }
            });
            // Asked before subscribing: a RESP2 subscriber may only manage
            // its subscriptions.
            long id = sub.sync().clientId();
            sub.sync().subscribe(channel);
            assertEquals(1L, pub.sync().publish(channel, "hi"));
            assertEquals(channel + "|hi", received.poll(5, TimeUnit.SECONDS));
            assertEquals(Map.of(channel, 1L), pub.sync().pubsubNumsub(channel));
            assertTrue(pub.sync().pubsubChannels(p + "*").contains(channel));

            sub.sync().psubscribe(p + "news.*");
            assertEquals(1L, pub.sync().publish(p + "news.sport", "goal"));
            assertEquals(p + "news.*|" + p + "news.sport|goal", received.poll(5, TimeUnit.SECONDS));
            assertEquals(1L, pub.sync().pubsubNumpat());

            // After the server drops the subscriber, Lettuce reconnects and
            // subscribes again.
            assertEquals(1L, pub.sync().clientKill(KillArgs.Builder.id(id)));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            String again = null;
            while (again == null && System.nanoTime() < deadline) {
                if (pub.sync().publish(channel, "again") > 0) {
                    again = received.poll(5, TimeUnit.SECONDS);
                } else {
                    Thread.sleep(100);
                }
            }
            assertEquals(channel + "|again", again);

            sub.sync().unsubscribe(channel);
            sub.sync().punsubscribe(p + "news.*");
            assertEquals(0L, pub.sync().publish(channel, "nobody"));
        } finally {
            LettuceSupport.shutdown(client);
        }
    }

    @ParameterizedTest
    @MethodSource("io.rexyra.clienttests.lettuce.LettuceSupport#protocols")
    void connectionPool(ProtocolVersion protocol) throws Exception {
        String p = prefix();
        RedisClient client = LettuceSupport.client(protocol);
        GenericObjectPoolConfig<StatefulRedisConnection<String, String>> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(8);
        ExecutorService threads = Executors.newFixedThreadPool(8);
        try (GenericObjectPool<StatefulRedisConnection<String, String>> pool =
                     ConnectionPoolSupport.createGenericObjectPool(client::connect, config)) {
            List<Future<?>> work = new ArrayList<>();
            for (int t = 0; t < 8; t++) {
                work.add(threads.submit(() -> {
                    for (int i = 0; i < 50; i++) {
                        // Pooled connections are how Lettuce applications run
                        // transactions without sharing a connection.
                        try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {
                            RedisCommands<String, String> redis = connection.sync();
                            redis.multi();
                            redis.incr(p + "count");
                            redis.rpush(p + "log", Thread.currentThread().getName());
                            if (redis.exec().wasDiscarded()) {
                                throw new AssertionError("transaction discarded");
                            }
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> f : work) {
                f.get(60, TimeUnit.SECONDS);
            }
            try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {
                assertEquals("400", connection.sync().get(p + "count"));
                assertEquals(400, connection.sync().llen(p + "log"));
            }
            assertTrue(pool.getCreatedCount() <= 8);
        } finally {
            threads.shutdownNow();
            LettuceSupport.shutdown(client);
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

    private static <T> T retry(Call<T> call) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        RedisException last = null;
        while (System.nanoTime() < deadline) {
            try {
                return call.run();
            } catch (RedisException e) {
                last = e;
                Thread.sleep(200);
            }
        }
        throw new AssertionError("did not reconnect within 15 seconds", last);
    }
}
