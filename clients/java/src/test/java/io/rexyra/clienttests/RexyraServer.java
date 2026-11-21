package io.rexyra.clienttests;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Starts one rexyra-server for the whole test run, on a free port with an
 * empty data folder, and stops it when the JVM exits.
 *
 * <p>REXYRA_SERVER names the executable (default: target/release/rexyra-server.exe
 * in the repository). REXYRA_ENDPOINT (host:port) and REXYRA_PASSWORD use a
 * server that is already running instead, which is how the same tests are run
 * against Redis to check the tests themselves. That server's data is written
 * to and database 5 is flushed.
 */
public final class RexyraServer {
    public static final String PASSWORD = "client-tests-password";

    private static RexyraServer instance;

    /** Host the tests connect to. */
    public final String host;
    /** Port the tests connect to. */
    public final int port;
    /** Password, or null when the server has none. */
    public final String password;
    /** True when the tests run against a server they did not start. */
    public final boolean external;

    private final Process process;
    private final Path dataDir;

    public static synchronized RexyraServer get() {
        if (instance == null) {
            instance = start();
            Runtime.getRuntime().addShutdownHook(new Thread(instance::stop, "rexyra-server-stop"));
        }
        return instance;
    }

    /** A key prefix no other test uses. */
    public static String prefix(String library) {
        return library + ":" + UUID.randomUUID().toString().replace("-", "") + ":";
    }

    private RexyraServer(String host, int port, String password, boolean external, Process process, Path dataDir) {
        this.host = host;
        this.port = port;
        this.password = password;
        this.external = external;
        this.process = process;
        this.dataDir = dataDir;
    }

    private static RexyraServer start() {
        String endpoint = System.getenv("REXYRA_ENDPOINT");
        if (endpoint != null && !endpoint.isEmpty()) {
            int colon = endpoint.lastIndexOf(':');
            String password = System.getenv("REXYRA_PASSWORD");
            return new RexyraServer(endpoint.substring(0, colon), Integer.parseInt(endpoint.substring(colon + 1)),
                    password == null || password.isEmpty() ? null : password, true, null, null);
        }

        String exe = System.getenv("REXYRA_SERVER");
        if (exe == null || exe.isEmpty()) {
            exe = repositoryRoot().resolve("target").resolve("release").resolve("rexyra-server.exe").toString();
        }
        if (!Files.isRegularFile(Paths.get(exe))) {
            throw new IllegalStateException("rexyra-server not found at " + exe
                    + "; build it with `cargo build --release -p rexyra-server` or set REXYRA_SERVER");
        }

        try {
            int port = freePort();
            Path dataDir = Files.createTempDirectory("rexyra-client-tests-");
            Process process = new ProcessBuilder(exe,
                    "--port", Integer.toString(port), "--dir", dataDir.toString(), "--save", "",
                    "--requirepass", PASSWORD)
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .redirectError(ProcessBuilder.Redirect.DISCARD)
                    .start();
            RexyraServer server = new RexyraServer("127.0.0.1", port, PASSWORD, false, process, dataDir);
            try {
                server.waitUntilListening();
            } catch (RuntimeException e) {
                server.stop();
                throw e;
            }
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void stop() {
        if (process != null && process.isAlive()) {
            process.destroyForcibly();
            try {
                process.waitFor(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (dataDir != null) {
            try (Stream<Path> files = Files.walk(dataDir)) {
                files.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            } catch (IOException ignored) {
                // Best effort, as with any temp folder.
            }
        }
    }

    private static Path repositoryRoot() {
        Path dir = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        while (dir != null && !Files.isRegularFile(dir.resolve("Cargo.toml"))) {
            dir = dir.getParent();
        }
        if (dir == null) {
            throw new IllegalStateException("repository root (Cargo.toml) not found");
        }
        return dir;
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    private void waitUntilListening() {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                throw new IllegalStateException("rexyra-server exited during start-up with code " + process.exitValue());
            }
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 1000);
                return;
            } catch (IOException e) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(ie);
                }
            }
        }
        throw new IllegalStateException("rexyra-server did not start listening within 30 seconds");
    }
}
