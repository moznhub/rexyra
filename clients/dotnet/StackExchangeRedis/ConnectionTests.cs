using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace Rexyra.ClientTests;

/// <summary>
/// Connecting, transactions, batches, scripts, locks and the server API.
/// </summary>
[Collection(ServerCollection.Name)]
public sealed class ConnectionTests(ServerFixture server, ITestOutputHelper output)
{
    public static TheoryData<RedisProtocol> Protocols => new() { RedisProtocol.Resp2, RedisProtocol.Resp3 };

    private static string Prefix() => $"se:{Guid.NewGuid():N}:";

    [Theory, MemberData(nameof(Protocols))]
    public void ConnectsCleanly(RedisProtocol protocol)
    {
        var log = new StringWriter();
        using var conn = server.Connect(protocol, log);
        output.WriteLine(log.ToString());

        Assert.True(conn.IsConnected);
        var endpoint = conn.GetEndPoints().Single();
        var srv = conn.GetServer(endpoint);
        Assert.True(srv.IsConnected);
        Assert.Equal(ServerType.Standalone, srv.ServerType);
        Assert.False(srv.IsReplica);
        Assert.True(srv.Version >= new Version(7, 0));
        Assert.Equal(protocol, srv.Protocol);
        Assert.True(srv.Ping() < TimeSpan.FromSeconds(1));

        // Anything the client logged as a failure while connecting.
        var text = log.ToString();
        Assert.DoesNotContain("failed", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("ERR", text);
    }

    [Fact]
    public void WrongPasswordIsRefused()
    {
        if (server.External)
        {
            return;
        }
        var options = server.Options();
        options.Password = "not-the-password";
        options.ConnectTimeout = 3000;
        Assert.ThrowsAny<RedisConnectionException>(() => ConnectionMultiplexer.Connect(options));
    }

    [Theory, MemberData(nameof(Protocols))]
    public async Task TransactionWithConditions(RedisProtocol protocol)
    {
        using var conn = server.Connect(protocol);
        var db = conn.GetDatabase();
        var p = Prefix();

        var tx = db.CreateTransaction();
        tx.AddCondition(Condition.KeyNotExists(p + "lock"));
        var set = tx.StringSetAsync(p + "lock", "owner");
        var incr = tx.StringIncrementAsync(p + "count");
        Assert.True(await tx.ExecuteAsync());
        Assert.True(await set);
        Assert.Equal(1, await incr);

        // The condition no longer holds: nothing runs.
        var tx2 = db.CreateTransaction();
        tx2.AddCondition(Condition.KeyNotExists(p + "lock"));
        _ = tx2.StringIncrementAsync(p + "count");
        Assert.False(await tx2.ExecuteAsync());
        Assert.Equal(1, (long)await db.StringGetAsync(p + "count"));

        var tx3 = db.CreateTransaction();
        tx3.AddCondition(Condition.StringEqual(p + "lock", "owner"));
        tx3.AddCondition(Condition.HashNotExists(p + "h", "f"));
        _ = tx3.HashSetAsync(p + "h", "f", "v");
        Assert.True(await tx3.ExecuteAsync());
        Assert.Equal("v", await db.HashGetAsync(p + "h", "f"));
    }

    [Theory, MemberData(nameof(Protocols))]
    public async Task BatchAndPipelining(RedisProtocol protocol)
    {
        using var conn = server.Connect(protocol);
        var db = conn.GetDatabase();
        var p = Prefix();

        var batch = db.CreateBatch();
        var tasks = Enumerable.Range(0, 1000).Select(i => batch.StringSetAsync($"{p}{i}", i)).ToList();
        batch.Execute();
        Assert.All(await Task.WhenAll(tasks), Assert.True);

        // Many concurrent callers on one multiplexer, as a web application does.
        var gets = Enumerable.Range(0, 1000).Select(i => db.StringGetAsync($"{p}{i}")).ToArray();
        var values = await Task.WhenAll(gets);
        Assert.Equal(Enumerable.Range(0, 1000).Select(i => (RedisValue)i), values);
    }

    [Theory, MemberData(nameof(Protocols))]
    public async Task LuaScripts(RedisProtocol protocol)
    {
        using var conn = server.Connect(protocol);
        var db = conn.GetDatabase();
        var p = Prefix();

        var result = await db.ScriptEvaluateAsync(
            "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('INCRBY', KEYS[2], ARGV[2])",
            new RedisKey[] { p + "s", p + "n" }, new RedisValue[] { "hello", 41 });
        Assert.Equal(41, (long)result);
        Assert.Equal("hello", await db.StringGetAsync(p + "s"));

        // LuaScript with named parameters, as applications usually write them.
        var limiter = LuaScript.Prepare(
            "local n = redis.call('INCR', @key) " +
            "if n == 1 then redis.call('PEXPIRE', @key, @windowMs) end " +
            "if n > tonumber(@limit) then return 0 else return 1 end");
        var allowed = 0;
        for (var i = 0; i < 8; i++)
        {
            allowed += (int)await db.ScriptEvaluateAsync(limiter, new { key = (RedisKey)(p + "rate"), windowMs = 60_000, limit = 5 });
        }
        Assert.Equal(5, allowed);

        // Loaded once, then run by its SHA1 (EVALSHA).
        var srv = conn.GetServer(conn.GetEndPoints()[0]);
        var loaded = await limiter.LoadAsync(srv);
        Assert.Equal(0, (int)await db.ScriptEvaluateAsync(loaded, new { key = (RedisKey)(p + "rate"), windowMs = 60_000, limit = 5 }));
        Assert.True(srv.ScriptExists(limiter.ExecutableScript));

        // Replies of every Lua type.
        var mixed = await db.ScriptEvaluateAsync("return {1, 'two', {3}, redis.status_reply('OK')}");
        var items = (RedisResult[])mixed!;
        Assert.Equal(1, (long)items[0]);
        Assert.Equal("two", (string?)items[1]);
        Assert.Equal(3, (long)((RedisResult[])items[2]!)[0]);
        Assert.Equal("OK", (string?)items[3]);

        var error = await Assert.ThrowsAsync<RedisServerException>(() => db.ScriptEvaluateAsync("return redis.error_reply('my error')"));
        Assert.Contains("my error", error.Message);
    }

    [Theory, MemberData(nameof(Protocols))]
    public async Task Locks(RedisProtocol protocol)
    {
        using var conn = server.Connect(protocol);
        var db = conn.GetDatabase();
        var p = Prefix();

        Assert.True(await db.LockTakeAsync(p + "lock", "me", TimeSpan.FromSeconds(30)));
        Assert.False(await db.LockTakeAsync(p + "lock", "you", TimeSpan.FromSeconds(30)));
        Assert.Equal("me", await db.LockQueryAsync(p + "lock"));
        Assert.True(await db.LockExtendAsync(p + "lock", "me", TimeSpan.FromSeconds(60)));
        Assert.False(await db.LockReleaseAsync(p + "lock", "you"));
        Assert.True(await db.LockReleaseAsync(p + "lock", "me"));
        Assert.True(await db.LockTakeAsync(p + "lock", "you", TimeSpan.FromSeconds(30)));
    }

    [Theory, MemberData(nameof(Protocols))]
    public async Task ServerApi(RedisProtocol protocol)
    {
        using var conn = server.Connect(protocol);
        var srv = conn.GetServer(conn.GetEndPoints()[0]);
        var db = conn.GetDatabase(5);
        var p = Prefix();

        var info = await srv.InfoAsync("server");
        Assert.Contains(info.SelectMany(g => g), kv => kv.Key == "redis_version");
        Assert.NotEmpty(await srv.ConfigGetAsync("maxmemory"));
        Assert.True((await srv.TimeAsync()) > DateTime.UtcNow.AddMinutes(-5));
        Assert.NotEmpty(await srv.ClientListAsync());
        _ = await srv.SlowlogGetAsync(10);
        _ = await srv.LastSaveAsync();
        Assert.Equal("echo", srv.Echo("echo"));

        await db.StringSetAsync(p + "x", 1);
        Assert.True(await srv.DatabaseSizeAsync(5) >= 1);
        await srv.FlushDatabaseAsync(5);
        Assert.Equal(0, await srv.DatabaseSizeAsync(5));

        var memory = await db.ExecuteAsync("MEMORY", "USAGE", "nothing-here");
        Assert.True(memory.IsNull);
    }

    [Fact]
    public async Task ReconnectsAfterTheServerDropsTheConnection()
    {
        using var conn = server.Connect();
        var db = conn.GetDatabase();
        var p = Prefix();
        await db.StringSetAsync(p + "k", "before");

        var srv = conn.GetServer(conn.GetEndPoints()[0]);
        // CLIENT KILL of every other connection drops the multiplexer's own.
        using var admin = server.Connect();
        var killed = await admin.GetDatabase().ExecuteAsync("CLIENT", "KILL", "SKIPME", "yes");
        Assert.True((long)killed >= 1);

        var deadline = DateTime.UtcNow.AddSeconds(15);
        RedisValue value = RedisValue.Null;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                value = await db.StringGetAsync(p + "k");
                if (!value.IsNull) break;
            }
            catch (RedisConnectionException) { }
            await Task.Delay(200);
        }
        Assert.Equal("before", value);
        Assert.True(srv.IsConnected);
    }

    [Fact]
    public async Task PubSub()
    {
        using var conn = server.Connect();
        var sub = conn.GetSubscriber();
        var received = new TaskCompletionSource<string>();
        await sub.SubscribeAsync(RedisChannel.Literal("se:channel"), (_, msg) => received.TrySetResult(msg!));
        Assert.Equal(1, await sub.PublishAsync(RedisChannel.Literal("se:channel"), "hi"));
        Assert.Equal("hi", await received.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }
}
