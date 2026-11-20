using StackExchange.Redis;
using Xunit;

namespace Rexyra.ClientTests;

/// <summary>
/// The basics of every data type, through the API an application uses, in
/// RESP2 and RESP3.
/// </summary>
[Collection(ServerCollection.Name)]
public sealed class DataTypeTests(ServerFixture server)
{
    public static TheoryData<RedisProtocol> Protocols => new() { RedisProtocol.Resp2, RedisProtocol.Resp3 };

    private static string Prefix() => $"se:{Guid.NewGuid():N}:";

    [Theory, MemberData(nameof(Protocols))]
    public async Task Strings(RedisProtocol protocol)
    {
        using var conn = server.Connect(protocol);
        var db = conn.GetDatabase();
        var p = Prefix();

        Assert.True(await db.StringSetAsync(p + "a", "hello"));
        Assert.Equal("hello", await db.StringGetAsync(p + "a"));
        Assert.False(await db.StringSetAsync(p + "a", "other", when: When.NotExists));
        Assert.True(await db.StringSetAsync(p + "b", "x", when: When.NotExists));
        Assert.Equal(11, await db.StringAppendAsync(p + "a", " world"));
        Assert.Equal("world", await db.StringGetRangeAsync(p + "a", 6, -1));
        Assert.Equal(11, await db.StringLengthAsync(p + "a"));

        Assert.Equal(5, await db.StringIncrementAsync(p + "n", 5));
        Assert.Equal(3, await db.StringDecrementAsync(p + "n", 2));
        Assert.Equal(3.5, await db.StringIncrementAsync(p + "n", 0.5));

        Assert.True(await db.StringSetAsync(new[]
        {
            new KeyValuePair<RedisKey, RedisValue>(p + "m1", "1"),
            new KeyValuePair<RedisKey, RedisValue>(p + "m2", "2"),
        }));
        var values = await db.StringGetAsync(new RedisKey[] { p + "m1", p + "missing", p + "m2" });
        Assert.Equal(new RedisValue[] { "1", RedisValue.Null, "2" }, values);

        Assert.Equal("hello world", await db.StringGetSetAsync(p + "a", "replaced"));
        Assert.Equal("replaced", await db.StringGetDeleteAsync(p + "a"));
        Assert.False(await db.KeyExistsAsync(p + "a"));

        var bytes = new byte[] { 0, 1, 2, 255, 13, 10 };
        await db.StringSetAsync(p + "bin", bytes);
        Assert.Equal(bytes, (byte[]?)await db.StringGetAsync(p + "bin"));
    }

    [Theory, MemberData(nameof(Protocols))]
    public async Task KeysAndExpiry(RedisProtocol protocol)
    {
        using var conn = server.Connect(protocol);
        var db = conn.GetDatabase();
        var p = Prefix();

        await db.StringSetAsync(p + "k", "v", TimeSpan.FromSeconds(100));
        var ttl = await db.KeyTimeToLiveAsync(p + "k");
        Assert.NotNull(ttl);
        Assert.InRange(ttl!.Value.TotalSeconds, 90, 100);
        Assert.True(await db.KeyPersistAsync(p + "k"));
        Assert.Null(await db.KeyTimeToLiveAsync(p + "k"));

        Assert.True(await db.KeyExpireAsync(p + "k", TimeSpan.FromMilliseconds(100)));
        await Task.Delay(300);
        Assert.False(await db.KeyExistsAsync(p + "k"));

        await db.StringSetAsync(p + "short", "v", TimeSpan.FromMilliseconds(50));
        await Task.Delay(200);
        Assert.True((await db.StringGetAsync(p + "short")).IsNull);

        await db.StringSetAsync(p + "r", "v");
        Assert.True(await db.KeyRenameAsync(p + "r", p + "r2"));
        Assert.Equal(RedisType.String, await db.KeyTypeAsync(p + "r2"));
        Assert.Equal(RedisType.None, await db.KeyTypeAsync(p + "r"));

        var dump = await db.KeyDumpAsync(p + "r2");
        Assert.NotNull(dump);
        await db.KeyRestoreAsync(p + "restored", dump!);
        Assert.Equal("v", await db.StringGetAsync(p + "restored"));

        for (var i = 0; i < 25; i++)
        {
            await db.StringSetAsync($"{p}scan:{i}", i);
        }
        var found = conn.GetServer(conn.GetEndPoints()[0]).Keys(pattern: p + "scan:*", pageSize: 7).ToList();
        Assert.Equal(25, found.Count);
        Assert.Equal(25, await db.KeyDeleteAsync(found.ToArray()));
    }

    [Theory, MemberData(nameof(Protocols))]
    public async Task Hashes(RedisProtocol protocol)
    {
        using var conn = server.Connect(protocol);
        var db = conn.GetDatabase();
        var p = Prefix();

        await db.HashSetAsync(p + "h", new HashEntry[] { new("a", "1"), new("b", "2"), new("c", "3") });
        Assert.Equal("2", await db.HashGetAsync(p + "h", "b"));
        Assert.Equal(3, await db.HashLengthAsync(p + "h"));
        Assert.Equal(11, await db.HashIncrementAsync(p + "h", "a", 10));
        Assert.Equal(2.5, await db.HashIncrementAsync(p + "h", "f", 2.5));
        Assert.True(await db.HashExistsAsync(p + "h", "c"));
        Assert.True(await db.HashDeleteAsync(p + "h", "c"));
        Assert.False(await db.HashSetAsync(p + "h", "a", "x", When.NotExists));

        var all = (await db.HashGetAllAsync(p + "h")).ToDictionary(e => (string)e.Name!, e => (string)e.Value!);
        Assert.Equal(new Dictionary<string, string> { ["a"] = "11", ["b"] = "2", ["f"] = "2.5" }, all);

        for (var i = 0; i < 300; i++)
        {
            await db.HashSetAsync(p + "big", $"f{i}", i);
        }
        Assert.Equal(300, db.HashScan(p + "big", pageSize: 50).Count());
        Assert.Equal(new RedisValue[] { "11", RedisValue.Null }, await db.HashGetAsync(p + "h", new RedisValue[] { "a", "zz" }));
    }

    [Theory, MemberData(nameof(Protocols))]
    public async Task Lists(RedisProtocol protocol)
    {
        using var conn = server.Connect(protocol);
        var db = conn.GetDatabase();
        var p = Prefix();

        Assert.Equal(3, await db.ListRightPushAsync(p + "l", new RedisValue[] { "a", "b", "c" }));
        Assert.Equal(4, await db.ListLeftPushAsync(p + "l", "z"));
        Assert.Equal(new RedisValue[] { "z", "a", "b", "c" }, await db.ListRangeAsync(p + "l"));
        Assert.Equal("b", await db.ListGetByIndexAsync(p + "l", 2));
        Assert.Equal(2, await db.ListPositionAsync(p + "l", "b"));
        Assert.Equal("z", await db.ListLeftPopAsync(p + "l"));
        Assert.Equal("c", await db.ListRightPopAsync(p + "l"));
        Assert.Equal("b", await db.ListMoveAsync(p + "l", p + "l2", ListSide.Right, ListSide.Left));
        Assert.Equal(2, await db.ListInsertAfterAsync(p + "l", "a", "after"));
        await db.ListTrimAsync(p + "l", 0, 0);
        Assert.Equal(new RedisValue[] { "a" }, await db.ListRangeAsync(p + "l"));
        Assert.Equal(1, await db.ListLengthAsync(p + "l2"));
        await db.ListRightPushAsync(p + "m", new RedisValue[] { "1", "2", "3" });
        var popped = await db.ListLeftPopAsync(new RedisKey[] { p + "empty", p + "m" }, 2);
        Assert.Equal(p + "m", (string?)popped.Key);
        Assert.Equal(new RedisValue[] { "1", "2" }, popped.Values);
    }

    [Theory, MemberData(nameof(Protocols))]
    public async Task Sets(RedisProtocol protocol)
    {
        using var conn = server.Connect(protocol);
        var db = conn.GetDatabase();
        var p = Prefix();

        Assert.Equal(3, await db.SetAddAsync(p + "a", new RedisValue[] { "1", "2", "3" }));
        Assert.Equal(2, await db.SetAddAsync(p + "b", new RedisValue[] { "3", "4" }));
        Assert.True(await db.SetContainsAsync(p + "a", "2"));
        Assert.Equal(new[] { true, false }, await db.SetContainsAsync(p + "a", new RedisValue[] { "1", "9" }));
        Assert.Equal(3, await db.SetLengthAsync(p + "a"));

        static string[] Sorted(RedisValue[] v) => v.Select(x => (string)x!).OrderBy(x => x).ToArray();
        Assert.Equal(new[] { "1", "2", "3", "4" }, Sorted(await db.SetCombineAsync(SetOperation.Union, p + "a", p + "b")));
        Assert.Equal(new[] { "3" }, Sorted(await db.SetCombineAsync(SetOperation.Intersect, p + "a", p + "b")));
        Assert.Equal(new[] { "1", "2" }, Sorted(await db.SetCombineAsync(SetOperation.Difference, p + "a", p + "b")));
        Assert.Equal(2, await db.SetCombineAndStoreAsync(SetOperation.Difference, p + "d", p + "a", p + "b"));
        Assert.Equal(1, await db.SetIntersectionLengthAsync(new RedisKey[] { p + "a", p + "b" }));

        Assert.True(await db.SetMoveAsync(p + "a", p + "b", "1"));
        Assert.True(await db.SetRemoveAsync(p + "b", "1"));
        var popped = await db.SetPopAsync(p + "a");
        Assert.False(popped.IsNull);
        Assert.Equal(1, await db.SetLengthAsync(p + "a"));
        Assert.Equal(2, db.SetScan(p + "b", pageSize: 1).Count());
    }

    [Theory, MemberData(nameof(Protocols))]
    public async Task SortedSets(RedisProtocol protocol)
    {
        using var conn = server.Connect(protocol);
        var db = conn.GetDatabase();
        var p = Prefix();

        Assert.Equal(3, await db.SortedSetAddAsync(p + "z", new SortedSetEntry[] { new("a", 1), new("b", 2), new("c", 3) }));
        Assert.Equal(2, await db.SortedSetScoreAsync(p + "z", "b"));
        Assert.Equal(1, await db.SortedSetRankAsync(p + "z", "b"));
        Assert.Equal(0, await db.SortedSetRankAsync(p + "z", "c", Order.Descending));
        Assert.Equal(4.5, await db.SortedSetIncrementAsync(p + "z", "a", 3.5));
        Assert.Equal(new RedisValue[] { "b", "c", "a" }, await db.SortedSetRangeByRankAsync(p + "z"));

        var withScores = await db.SortedSetRangeByScoreWithScoresAsync(p + "z", 2, 4);
        Assert.Equal(new[] { ("b", 2.0), ("c", 3.0) }, withScores.Select(e => ((string)e.Element!, e.Score)).ToArray());
        Assert.Equal(2, await db.SortedSetLengthAsync(p + "z", 2, 3.5));
        Assert.Equal(new RedisValue[] { "a", "c" }, await db.SortedSetRangeByScoreAsync(p + "z", order: Order.Descending, take: 2));

        await db.SortedSetAddAsync(p + "y", new SortedSetEntry[] { new("b", 10), new("d", 1) });
        Assert.Equal(4, await db.SortedSetCombineAndStoreAsync(SetOperation.Union, p + "u", p + "z", p + "y"));
        Assert.Equal(12, await db.SortedSetScoreAsync(p + "u", "b"));
        var inter = await db.SortedSetCombineWithScoresAsync(SetOperation.Intersect, new RedisKey[] { p + "z", p + "y" });
        Assert.Equal(("b", 12.0), ((string)inter.Single().Element!, inter.Single().Score));

        var min = await db.SortedSetPopAsync(p + "z");
        Assert.Equal("b", (string?)min!.Value.Element);
        Assert.Equal(1, await db.SortedSetRangeAndStoreAsync(p + "z", p + "copy", 0, 0));
        Assert.Equal(2, db.SortedSetScan(p + "u", pageSize: 1).Take(2).Count());

        await db.SortedSetAddAsync(p + "lex", new SortedSetEntry[] { new("apple", 0), new("banana", 0), new("cherry", 0) });
        Assert.Equal(new RedisValue[] { "banana", "cherry" }, await db.SortedSetRangeByValueAsync(p + "lex", "b", "d"));
    }

    [Theory, MemberData(nameof(Protocols))]
    public async Task SelectDatabase(RedisProtocol protocol)
    {
        using var conn = server.Connect(protocol);
        var p = Prefix();
        await conn.GetDatabase(3).StringSetAsync(p + "k", "three");
        Assert.True((await conn.GetDatabase(0).StringGetAsync(p + "k")).IsNull);
        Assert.Equal("three", await conn.GetDatabase(3).StringGetAsync(p + "k"));
    }
}
