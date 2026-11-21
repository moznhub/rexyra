package io.rexyra.clienttests.lettuce;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.protocol.ProtocolVersion;
import io.rexyra.clienttests.RexyraServer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.provider.Arguments;

/** How the Lettuce tests connect, as an application would. */
final class LettuceSupport {
    static final RexyraServer SERVER = RexyraServer.get();

    private LettuceSupport() {
    }

    /**
     * Lettuce's default, which sends HELLO 3 (and would quietly fall back to
     * RESP2 if HELLO failed), and RESP2 set explicitly.
     */
    static Stream<Arguments> protocols() {
        return Stream.of(
                Arguments.of(Named.of("RESP3 (default)", null)),
                Arguments.of(Named.of("RESP2", ProtocolVersion.RESP2)));
    }

    static RedisURI.Builder uri() {
        RedisURI.Builder builder = RedisURI.builder()
                .withHost(SERVER.host)
                .withPort(SERVER.port)
                .withTimeout(Duration.ofSeconds(10));
        if (SERVER.password != null) {
            builder.withPassword(SERVER.password.toCharArray());
        }
        return builder;
    }

    static RedisClient client(ProtocolVersion protocol) {
        return client(uri().build(), protocol);
    }

    static RedisClient client(RedisURI uri, ProtocolVersion protocol) {
        RedisClient client = RedisClient.create(uri);
        ClientOptions.Builder options = ClientOptions.builder();
        if (protocol != null) {
            options.protocolVersion(protocol);
        }
        client.setOptions(options.build());
        return client;
    }

    static void shutdown(RedisClient client) {
        client.shutdown(0, 2, TimeUnit.SECONDS);
    }

    /** A client with one connection, closed together. */
    static final class Connected implements AutoCloseable {
        final RedisClient client;
        final StatefulRedisConnection<String, String> connection;
        final RedisCommands<String, String> redis;

        Connected(ProtocolVersion protocol) {
            this(uri().build(), protocol);
        }

        Connected(RedisURI uri, ProtocolVersion protocol) {
            client = client(uri, protocol);
            try {
                connection = client.connect();
            } catch (RuntimeException e) {
                shutdown(client);
                throw e;
            }
            redis = connection.sync();
        }

        @Override
        public void close() {
            connection.close();
            shutdown(client);
        }
    }
}
