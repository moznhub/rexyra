package io.rexyra.clienttests.jedis;

import io.rexyra.clienttests.RexyraServer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.stream.Stream;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.provider.Arguments;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.RedisProtocol;
import redis.clients.jedis.UnifiedJedis;

/** How the Jedis tests connect, as an application would. */
final class JedisSupport {
    static final RexyraServer SERVER = RexyraServer.get();

    private JedisSupport() {
    }

    /**
     * The protocols every test runs with: Jedis's default (no HELLO, plain
     * AUTH), and HELLO 2 / HELLO 3 when a protocol is set.
     */
    static Stream<Arguments> protocols() {
        return Stream.of(
                Arguments.of(Named.of("default", null)),
                Arguments.of(Named.of("RESP2", RedisProtocol.RESP2)),
                Arguments.of(Named.of("RESP3", RedisProtocol.RESP3)));
    }

    static HostAndPort hostAndPort() {
        return new HostAndPort(SERVER.host, SERVER.port);
    }

    static DefaultJedisClientConfig.Builder config(RedisProtocol protocol) {
        DefaultJedisClientConfig.Builder builder = DefaultJedisClientConfig.builder()
                .timeoutMillis(10_000);
        if (SERVER.password != null) {
            builder.password(SERVER.password);
        }
        if (protocol != null) {
            builder.protocol(protocol);
        }
        return builder;
    }

    /**
     * The pooled client applications use: JedisPooled up to Jedis 7, and
     * RedisClient, which replaces it, from Jedis 8 on.
     */
    static UnifiedJedis pooled(JedisClientConfig config) {
        try {
            try {
                Class<?> pooled = Class.forName("redis.clients.jedis.JedisPooled");
                return (UnifiedJedis) pooled.getConstructor(HostAndPort.class, JedisClientConfig.class)
                        .newInstance(hostAndPort(), config);
            } catch (ClassNotFoundException e) {
                Class<?> client = Class.forName("redis.clients.jedis.RedisClient");
                Object builder = client.getMethod("builder").invoke(null);
                builder = method(builder, "hostAndPort", HostAndPort.class).invoke(builder, hostAndPort());
                builder = method(builder, "clientConfig", JedisClientConfig.class).invoke(builder, config);
                return (UnifiedJedis) method(builder, "build").invoke(builder);
            }
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException(e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    static UnifiedJedis pooled(RedisProtocol protocol) {
        return pooled(config(protocol).build());
    }

    static JedisPool pool(JedisClientConfig config) {
        GenericObjectPoolConfig<Jedis> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(16);
        return new JedisPool(poolConfig, hostAndPort(), config);
    }

    static JedisPool pool(RedisProtocol protocol) {
        return pool(config(protocol).build());
    }

    /** One connection of its own, for WATCH, SELECT and the server API. */
    static Jedis connect(RedisProtocol protocol) {
        return new Jedis(hostAndPort(), config(protocol).build());
    }

    private static Method method(Object target, String name, Class<?>... parameters) throws NoSuchMethodException {
        Method method = target.getClass().getMethod(name, parameters);
        method.setAccessible(true);
        return method;
    }
}
