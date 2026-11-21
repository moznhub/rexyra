"""Every data type, through redis-py's command methods, in RESP2 and RESP3."""

import time

import pytest
import redis


def differs(reason):
    return pytest.mark.xfail("server_is_rexyra", reason=reason, strict=True)


def pairs(reply):
    """WITHSCORES replies as (member, score) tuples, whichever protocol.

    In RESP3 a single ZPOPMIN/ZPOPMAX result comes back flat: [member, score].
    """
    if reply and not isinstance(reply[0], (list, tuple)):
        reply = [reply]
    return [(m, float(s)) for m, s in reply]


def test_strings(r, p):
    assert r.set(p + "s", "hello") is True
    assert r.get(p + "s") == "hello"
    assert r.get(p + "missing") is None
    assert r.set(p + "s", "x", nx=True) is None
    assert r.set(p + "s", "world", xx=True, get=True) == "hello"
    assert r.set(p + "new", "x", xx=True) is None
    assert r.append(p + "s", "!") == 6
    assert r.strlen(p + "s") == 6
    assert r.getrange(p + "s", 0, 2) == "wor"
    assert r.setrange(p + "s", 0, "W") == 6
    assert r.get(p + "s") == "World!"
    assert r.lcs(p + "s", p + "s") == "World!"

    assert r.mset({p + "a": "1", p + "b": "2"})
    assert r.mget([p + "a", p + "b", p + "missing"]) == ["1", "2", None]
    assert r.msetnx({p + "a": "x", p + "c": "3"}) is False
    assert r.incr(p + "a") == 2
    assert r.incrby(p + "a", 10) == 12
    assert r.decr(p + "a") == 11
    assert r.decrby(p + "a", 2) == 9
    assert r.incrbyfloat(p + "f", 1.5) == 1.5
    assert r.incrbyfloat(p + "f", "2.25") == 3.75
    with pytest.raises(redis.ResponseError):
        r.incr(p + "s")

    assert r.getdel(p + "b") == "2"
    assert r.exists(p + "b") == 0
    assert r.setex(p + "e", 100, "v")
    assert 0 < r.ttl(p + "e") <= 100
    assert r.psetex(p + "pe", 100_000, "v")
    assert 0 < r.pttl(p + "pe") <= 100_000
    assert r.getex(p + "s", ex=50) == "World!"
    assert 0 < r.ttl(p + "s") <= 50
    assert r.getex(p + "s", persist=True) == "World!"
    assert r.ttl(p + "s") == -1
    assert r.set(p + "e", "kept", keepttl=True)
    assert r.ttl(p + "e") > 0
    assert r.setnx(p + "e", "no") is False


def test_binary_and_large_values(server, protocol, parser, p):
    with server.client(protocol, parser, decode_responses=False) as r:
        value = bytes(range(256)) + b"\r\n\x00end"
        r.set(p + "bin", value)
        assert r.get(p + "bin") == value
        big = b"x" * (2 * 1024 * 1024)
        r.set(p + "big", big)
        assert r.get(p + "big") == big
        assert r.strlen(p + "big") == len(big)
        r.set(p.encode() + b"key\x00with\r\nbytes", b"ok")
        assert r.get(p.encode() + b"key\x00with\r\nbytes") == b"ok"
        assert r.set(p + "unicode", "héllo 世界".encode())
        assert r.get(p + "unicode").decode() == "héllo 世界"


def test_bitmaps(r, p):
    assert r.setbit(p + "b", 7, 1) == 0
    assert r.setbit(p + "b", 7, 1) == 1
    assert r.getbit(p + "b", 7) == 1
    assert r.getbit(p + "b", 100) == 0
    r.setbit(p + "b", 9, 1)
    assert r.bitcount(p + "b") == 2
    assert r.bitcount(p + "b", 0, 0) == 1
    assert r.bitcount(p + "b", 0, 7, "BIT") == 1
    assert r.bitpos(p + "b", 1) == 7
    assert r.bitpos(p + "b", 0) == 0
    r.set(p + "x", "\xff")  # one UTF-8 encoded char = two bytes
    r.set(p + "y", "a")
    assert r.bitop("AND", p + "and", p + "x", p + "y") == 2
    assert r.bitop("NOT", p + "not", p + "y") == 1

    bf = r.bitfield(p + "bf")
    reply = bf.set("u8", 0, 200).incrby("u8", 0, 100).get("u4", 0).execute()
    assert reply == [0, 44, 2]
    reply = r.bitfield(p + "bf2").overflow("SAT").incrby("u8", 0, 300).execute()
    assert reply == [255]
    assert r.bitfield_ro(p + "bf", "u8", 0) == [44]


def test_hashes(r, p):
    k = p + "h"
    assert r.hset(k, mapping={"a": "1", "b": "2", "c": "3"}) == 3
    assert r.hset(k, "a", "one") == 0
    assert r.hget(k, "a") == "one"
    assert r.hget(k, "zz") is None
    assert r.hmget(k, ["a", "b", "zz"]) == ["one", "2", None]
    assert r.hgetall(k) == {"a": "one", "b": "2", "c": "3"}
    assert r.hgetall(p + "missing") == {}
    assert r.hlen(k) == 3
    assert sorted(r.hkeys(k)) == ["a", "b", "c"]
    assert sorted(r.hvals(k)) == ["2", "3", "one"]
    assert r.hexists(k, "b") is True
    assert r.hexists(k, "zz") is False
    assert r.hincrby(k, "b", 5) == 7
    assert r.hincrbyfloat(k, "f", 0.5) == 0.5
    assert r.hsetnx(k, "a", "x") == 0
    assert r.hsetnx(k, "d", "4") == 1
    assert r.hstrlen(k, "a") == 3
    assert r.hdel(k, "c", "zz") == 1
    assert r.hrandfield(k) in {"a", "b", "d", "f"}
    assert len(r.hrandfield(k, 2)) == 2
    with_values = r.hrandfield(k, 2, withvalues=True)
    assert len(with_values) in (2, 4)  # RESP2: flat list; RESP3: pairs
    assert dict(r.hscan_iter(k, match="*")) == r.hgetall(k)
    with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
        r.lpush(k, "x")


@differs("HEXPIRE, HPEXPIRE, HTTL, HPERSIST (Redis 7.4) are not implemented: ERR unknown command")
def test_hash_field_expiry(r, p):
    # HEXPIRE and friends are new in Redis 7.4.
    if not hasattr(r, "hexpire"):
        pytest.skip("this redis-py has no hexpire")
    k = p + "h"
    r.hset(k, mapping={"a": "1", "b": "2"})
    assert r.hexpire(k, 100, "a", "zz") == [1, -2]
    assert 0 < r.httl(k, "a")[0] <= 100
    assert r.httl(k, "b") == [-1]
    assert r.hpexpire(k, 50, "b") == [1]
    time.sleep(0.2)
    assert r.hgetall(k) == {"a": "1"}
    assert r.hpersist(k, "a") == [1]
    assert r.httl(k, "a") == [-1]


def test_lists(r, p):
    k = p + "l"
    assert r.rpush(k, "a", "b", "c") == 3
    assert r.lpush(k, "z") == 4
    assert r.lrange(k, 0, -1) == ["z", "a", "b", "c"]
    assert r.llen(k) == 4
    assert r.lindex(k, 1) == "a"
    assert r.lindex(k, 99) is None
    assert r.lset(k, 1, "A")
    assert r.linsert(k, "AFTER", "A", "a2") == 5
    assert r.lpos(k, "b") == 3
    assert r.lpos(k, "nope") is None
    assert r.lrem(k, 0, "a2") == 1
    assert r.lpop(k) == "z"
    assert r.rpop(k) == "c"
    assert r.rpush(k, "x", "y", "z") == 5
    assert r.lpop(k, 2) == ["A", "b"]
    assert r.rpop(k, 5) == ["z", "y", "x"]
    assert r.lpop(k) is None
    assert r.lpop(k, 2) is None
    assert r.ltrim(k, 0, 1)

    r.rpush(k, "1", "2", "3")
    assert r.lmove(k, p + "dst", "LEFT", "RIGHT") == "1"
    assert r.rpoplpush(k, p + "dst") == "3"
    assert r.lrange(p + "dst", 0, -1) == ["3", "1"]
    assert r.lmpop(2, p + "empty", k, direction="LEFT") == [k, ["2"]]
    assert r.lmpop(1, p + "empty", direction="LEFT") is None

    # Blocking pops: data ready, and a timeout with nothing to pop.
    r.rpush(k, "v")
    assert tuple(r.blpop([p + "empty", k], timeout=1)) == (k, "v")
    assert r.brpop([p + "empty"], timeout=0.1) is None
    r.rpush(k, "w")
    assert r.blmove(k, p + "dst", 1, "RIGHT", "LEFT") == "w"
    assert r.blmpop(0.1, 1, p + "empty", direction="LEFT") is None


def test_blocking_pop_is_woken_by_another_client(server, protocol, parser, p):
    import threading

    k = p + "q"
    result = {}
    with server.client(protocol, parser) as waiter, server.client(protocol, parser) as pusher:
        t = threading.Thread(target=lambda: result.update(v=waiter.blpop(k, timeout=5)))
        t.start()
        time.sleep(0.2)
        pusher.rpush(k, "job")
        t.join(6)
        assert tuple(result["v"]) == (k, "job")


def test_sets(r, p):
    a, b = p + "a", p + "b"
    assert r.sadd(a, "1", "2", "3") == 3
    assert r.sadd(a, "3", "4") == 1
    assert r.sadd(b, "3", "4", "5") == 3
    assert r.smembers(a) == {"1", "2", "3", "4"}
    assert r.smembers(p + "missing") == set()
    assert r.sismember(a, "1") == 1
    assert r.sismember(a, "9") == 0
    assert r.smismember(a, ["1", "9"]) == [True, False]
    assert r.scard(a) == 4
    assert r.sinter([a, b]) == {"3", "4"}
    assert r.sunion([a, b]) == {"1", "2", "3", "4", "5"}
    assert r.sdiff([a, b]) == {"1", "2"}
    assert r.sinterstore(p + "i", [a, b]) == 2
    assert r.sunionstore(p + "u", [a, b]) == 5
    assert r.sdiffstore(p + "d", [a, b]) == 2
    assert r.sintercard(2, [a, b]) == 2
    assert r.sintercard(2, [a, b], limit=1) == 1
    assert r.srem(a, "1", "9") == 1
    assert r.smove(a, b, "2") == 1
    assert r.spop(p + "missing") is None
    popped = r.spop(a)
    assert popped in {"3", "4"}
    assert r.srandmember(b) in r.smembers(b)
    assert len(r.srandmember(b, 2)) == 2
    assert len(r.srandmember(b, -10)) == 10
    assert set(r.sscan_iter(b)) == r.smembers(b)
    assert len(r.spop(b, 2)) == 2


def test_sorted_sets(r, p):
    k = p + "z"
    assert r.zadd(k, {"a": 1, "b": 2, "c": 3}) == 3
    assert r.zadd(k, {"a": 10}, nx=True) == 0
    assert r.zadd(k, {"a": 0.5}, gt=True, ch=True) == 0
    assert r.zadd(k, {"d": 4}, xx=True) == 0
    assert r.zadd(k, {"a": 2}, incr=True) == 3.0
    assert r.zscore(k, "a") == 3.0
    assert r.zscore(k, "missing") is None
    assert r.zincrby(k, 1.5, "b") == 3.5
    assert r.zcard(k) == 3
    assert r.zrange(k, 0, -1) == ["a", "c", "b"]
    assert pairs(r.zrange(k, 0, -1, withscores=True)) == [("a", 3.0), ("c", 3.0), ("b", 3.5)]
    assert r.zrange(k, 0, -1, desc=True) == ["b", "c", "a"]
    assert r.zrevrange(k, 0, 0) == ["b"]
    assert r.zrangebyscore(k, 3, "(3.5") == ["a", "c"]
    assert r.zrangebyscore(k, "-inf", "+inf", start=1, num=1) == ["c"]
    assert r.zrevrangebyscore(k, "+inf", 3.5) == ["b"]
    assert r.zrange(k, "(3", "+inf", byscore=True) == ["b"]
    assert r.zcount(k, 3, 3) == 2
    assert r.zrank(k, "b") == 2
    assert r.zrevrank(k, "b") == 0
    assert r.zrank(k, "missing") is None
    assert r.zmscore(k, ["a", "missing"]) == [3.0, None]
    assert r.zrem(k, "c", "missing") == 1

    r.zadd(p + "lex", {"a": 0, "b": 0, "c": 0, "d": 0})
    assert r.zrangebylex(p + "lex", "[b", "(d") == ["b", "c"]
    assert r.zlexcount(p + "lex", "-", "+") == 4
    assert r.zremrangebylex(p + "lex", "[a", "[a") == 1

    r.zadd(p + "o", {"b": 1, "x": 5})
    assert r.zunionstore(p + "un", [k, p + "o"]) == 3
    assert r.zscore(p + "un", "b") == 4.5
    assert r.zinterstore(p + "in", {k: 1, p + "o": 10}) == 1
    assert r.zscore(p + "in", "b") == 13.5
    assert r.zunion([k, p + "o"]) == ["a", "b", "x"]
    assert r.zinter([k, p + "o"]) == ["b"]
    assert r.zdiff([k, p + "o"]) == ["a"]
    assert r.zrangestore(p + "rs", k, 0, 0) == 1

    assert pairs(r.zpopmin(k)) == [("a", 3.0)]
    r.zadd(k, {"a": 1, "c": 2})
    assert pairs(r.zpopmax(k, 2)) == [("b", 3.5), ("c", 2.0)]
    reply = r.bzpopmin([p + "empty", k], timeout=1)
    assert (reply[0], reply[1], float(reply[2])) == (k, "a", 1.0)
    assert r.bzpopmax([p + "empty"], timeout=0.1) is None

    r.zadd(k, {"m": 1, "n": 2})
    key, popped = r.zmpop(1, [k], min=True)
    assert key == k and pairs(popped) == [("m", 1.0)]
    assert r.zrandmember(k) == "n"
    assert dict(r.zscan_iter(k)) == {"n": 2.0}
    assert r.zremrangebyscore(k, "-inf", "+inf") == 1
    assert r.exists(k) == 0


def test_zrank_withscore(r, p):
    # ZRANK ... WITHSCORE is new in Redis 7.2.
    r.zadd(p + "z", {"a": 1, "b": 2})
    # redis-py 5.x leaves the RESP2 score a string.
    rank, score = r.zrank(p + "z", "b", withscore=True)
    assert (rank, float(score)) == (1, 2.0)
    rank, score = r.zrevrank(p + "z", "b", withscore=True)
    assert (rank, float(score)) == (0, 2.0)


def test_keys_and_expiry(r, p):
    r.set(p + "a", "1")
    r.rpush(p + "l", "x")
    assert r.exists(p + "a", p + "l", p + "missing") == 2
    assert r.type(p + "a") == "string"
    assert r.type(p + "l") == "list"
    assert r.type(p + "missing") == "none"
    assert r.rename(p + "a", p + "b")
    assert r.renamenx(p + "b", p + "l") is False
    with pytest.raises(redis.ResponseError):
        r.rename(p + "missing", p + "x")
    assert r.copy(p + "b", p + "c") is True
    assert r.copy(p + "b", p + "c") is False
    assert r.copy(p + "b", p + "c", replace=True) is True
    assert r.touch(p + "b", p + "c", p + "missing") == 2

    assert r.ttl(p + "b") == -1
    assert r.ttl(p + "missing") == -2
    assert r.expire(p + "b", 100) is True
    assert r.expire(p + "missing", 100) is False
    assert r.expire(p + "b", 50, gt=True) is False
    assert r.expire(p + "b", 50, lt=True) is True
    assert r.expire(p + "c", 50, xx=True) is False
    assert r.expire(p + "c", 50, nx=True) is True
    assert 0 < r.ttl(p + "b") <= 50
    assert 0 < r.pttl(p + "b") <= 50_000
    assert r.expiretime(p + "b") > time.time()
    assert r.pexpiretime(p + "b") > time.time() * 1000
    assert r.persist(p + "b") is True
    assert r.persist(p + "b") is False
    assert r.expireat(p + "b", int(time.time()) + 100)
    assert r.pexpireat(p + "c", int(time.time() * 1000) + 100_000)
    assert r.pexpire(p + "l", 100)
    time.sleep(0.3)
    assert r.exists(p + "l") == 0
    assert r.get(p + "l") is None

    assert sorted(r.keys(p + "*")) == [p + "b", p + "c"]
    assert sorted(r.scan_iter(match=p + "*")) == [p + "b", p + "c"]
    r.sadd(p + "s", "m")
    assert list(r.scan_iter(match=p + "*", _type="SET")) == [p + "s"]
    assert r.delete(p + "b", p + "missing") == 1
    assert r.unlink(p + "c") == 1
    assert r.randomkey() is not None
    assert r.object("encoding", p + "s") in ("listpack", "intset", "hashtable")


def test_dump_and_restore(server, protocol, parser, p):
    with server.client(protocol, parser, decode_responses=False) as r:
        r.rpush(p + "l", "a", "b")
        r.zadd(p + "z", {"m": 1.5})
        for key in (p + "l", p + "z"):
            payload = r.dump(key)
            assert isinstance(payload, bytes)
            r.restore(key + ":copy", 0, payload)
        assert r.lrange(p + "l:copy", 0, -1) == [b"a", b"b"]
        assert r.zscore(p + "z:copy", "m") == 1.5
        with pytest.raises(redis.ResponseError, match="BUSYKEY"):
            r.restore(p + "l:copy", 0, r.dump(p + "l"))
        assert r.restore(p + "l:copy", 5000, r.dump(p + "l"), replace=True)
        assert r.pttl(p + "l:copy") > 0
        assert r.dump(p + "missing") is None


def test_sort(r, p):
    r.rpush(p + "l", "3", "1", "2")
    assert r.sort(p + "l") == ["1", "2", "3"]
    assert r.sort(p + "l", desc=True, start=0, num=2) == ["3", "2"]
    r.rpush(p + "w", "b", "a")
    assert r.sort(p + "w", alpha=True) == ["a", "b"]
    assert r.sort(p + "l", store=p + "sorted") == 3
    assert r.sort_ro(p + "l") == ["1", "2", "3"]
