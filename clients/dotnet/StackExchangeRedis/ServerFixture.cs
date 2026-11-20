using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using StackExchange.Redis;
using Xunit;

namespace Rexyra.ClientTests;

/// <summary>
/// Starts one rexyra-server for the whole test run, on a free port with an
/// empty data folder, and stops it at the end.
///
/// REXYRA_SERVER names the executable (default: target/release/rexyra-server.exe
/// in the repository). REXYRA_ENDPOINT (host:port) uses a server that is
/// already running instead, which is how the same tests are run against Redis
/// to check the tests themselves. That server's data is flushed.
/// </summary>
public sealed class ServerFixture : IDisposable
{
    public const string Password = "client-tests-password";

    private readonly Process? process;
    private readonly string? dataDir;

    public string Endpoint { get; }

    /// <summary>True when the tests run against a server they did not start.</summary>
    public bool External { get; }

    public ServerFixture()
    {
        var external = Environment.GetEnvironmentVariable("REXYRA_ENDPOINT");
        if (!string.IsNullOrEmpty(external))
        {
            Endpoint = external;
            External = true;
            return;
        }

        var exe = Environment.GetEnvironmentVariable("REXYRA_SERVER");
        if (string.IsNullOrEmpty(exe))
        {
            exe = Path.Combine(RepositoryRoot(), "target", "release", "rexyra-server.exe");
        }
        if (!File.Exists(exe))
        {
            throw new InvalidOperationException(
                $"rexyra-server not found at {exe}; build it with `cargo build --release -p rexyra-server` or set REXYRA_SERVER");
        }

        var port = FreePort();
        dataDir = Directory.CreateTempSubdirectory("rexyra-client-tests-").FullName;
        var start = new ProcessStartInfo(exe)
        {
            UseShellExecute = false,
            RedirectStandardError = true,
            RedirectStandardOutput = true,
        };
        foreach (var arg in new[]
                 {
                     "--port", port.ToString(), "--dir", dataDir, "--save", "",
                     "--requirepass", Password,
                 })
        {
            start.ArgumentList.Add(arg);
        }
        process = Process.Start(start) ?? throw new InvalidOperationException("could not start rexyra-server");
        process.ErrorDataReceived += (_, _) => { };
        process.OutputDataReceived += (_, _) => { };
        process.BeginErrorReadLine();
        process.BeginOutputReadLine();
        Endpoint = $"127.0.0.1:{port}";
        WaitUntilListening(port);
    }

    /// <summary>The options every test connects with, as an application would.</summary>
    public ConfigurationOptions Options(RedisProtocol protocol = RedisProtocol.Resp2, bool allowAdmin = true)
    {
        var options = ConfigurationOptions.Parse(Endpoint);
        options.AllowAdmin = allowAdmin;
        options.Protocol = protocol;
        options.ConnectTimeout = 10_000;
        options.SyncTimeout = 10_000;
        options.AbortOnConnectFail = true;
        var password = External
            ? Environment.GetEnvironmentVariable("REXYRA_PASSWORD")
            : Password;
        if (!string.IsNullOrEmpty(password))
        {
            options.Password = password;
        }
        return options;
    }

    public ConnectionMultiplexer Connect(RedisProtocol protocol = RedisProtocol.Resp2, TextWriter? log = null) =>
        ConnectionMultiplexer.Connect(Options(protocol), log);

    public void Dispose()
    {
        if (process is { HasExited: false })
        {
            process.Kill();
            process.WaitForExit(10_000);
        }
        process?.Dispose();
        if (dataDir is not null)
        {
            try { Directory.Delete(dataDir, recursive: true); } catch (IOException) { }
        }
    }

    private static string RepositoryRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Cargo.toml")))
        {
            dir = dir.Parent;
        }
        return dir?.FullName ?? throw new InvalidOperationException("repository root (Cargo.toml) not found");
    }

    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private void WaitUntilListening(int port)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            if (process!.HasExited)
            {
                throw new InvalidOperationException($"rexyra-server exited during start-up with code {process.ExitCode}");
            }
            try
            {
                using var client = new TcpClient();
                client.Connect(IPAddress.Loopback, port);
                return;
            }
            catch (SocketException)
            {
                Thread.Sleep(50);
            }
        }
        throw new InvalidOperationException("rexyra-server did not start listening within 30 seconds");
    }
}

[CollectionDefinition(Name)]
public sealed class ServerCollection : ICollectionFixture<ServerFixture>
{
    public const string Name = "rexyra-server";
}
