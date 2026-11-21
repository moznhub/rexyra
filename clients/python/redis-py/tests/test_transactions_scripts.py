"""Transactions, WATCH, pipelines, Lua scripts and the Lock helper."""

import threading
import time

import pytest
import redis
from redis.exceptions import LockError, LockNotOwnedError, NoScriptError, WatchError


def differs(reason):
    return pytest.mark.xfail("server_is_rexyra", reason=reason, strict=True)


# Transactions and pipelines


def test_transaction_pipeline(r, p):
    with r.pipeline(transaction=True) as pipe:
        pipe.set(p + "a", "1").incr(p + "a").lpush(p + "l", "x", "y").get(p + "a")
        pipe.hset(p + "h", mapping={"f": "v"}).hgetall(p + "h").smembers(p + "missing")
        assert pipe.execute() == [True, 2, 2, "2", 1, {"f": "v"}, set()]


def test_transaction_with_a_failing_command(r, p):
    r.set(p + "s", "text")
    with r.pipeline() as pipe:
        pipe.set(p + "a", "1").incr(p + "s").incr(p + "a")
        with pytest.raises(redis.ResponseError, match="not an integer"):
            pipe.execute()
    # The other commands of the transaction still ran.
    assert r.get(p + "a") == "2"

    with r.pipeline() as pipe:
        pipe.incr(p + "s").incr(p + "a")
        result = pipe.execute(raise_on_error=False)
        assert isinstance(result[0], redis.ResponseError)
        assert result[1] == 3


def test_transaction_aborted_by_a_queueing_error(r, p):
    with r.pipeline() as pipe:
        pipe.set(p + "a", "1")
        pipe.execute_command("NO-SUCH-COMMAND", "x")
        with pytest.raises(redis.ResponseError):
            pipe.execute()
    # EXECABORT: nothing ran.
    assert r.exists(p + "a") == 0
    # The pooled connection is still usable.
    assert r.ping()


def test_watch_with_transaction_helper_retries(server, r, p):
    k = p + "counter"
    r.set(k, "10")
    calls = []

    def double(pipe):
        value = int(pipe.get(k))
        if not calls:
            # Another client changes the key between WATCH and EXEC.
            with server.client() as other:
                other.incr(k)
        calls.append(value)
        pipe.multi()
        pipe.set(k, value * 2)

    result = r.transaction(double, k)
    assert result == [True]
    assert calls == [10, 11]
    assert r.get(k) == "22"

    # value_from_callable returns what the function returned.
    assert r.transaction(lambda pipe: int(pipe.get(k)), k, value_from_callable=True) == 22


def test_watch_by_hand(server, r, p):
    k = p + "k"
    r.set(k, "1")
    with r.pipeline() as pipe:
        pipe.watch(k)
        assert pipe.get(k) == "1"  # runs immediately while watching
        with server.client() as other:
            other.set(k, "changed")
        pipe.multi()
        pipe.set(k, "mine")
        with pytest.raises(WatchError):
            pipe.execute()
    assert r.get(k) == "changed"

    with r.pipeline() as pipe:
        pipe.watch(k)
        pipe.multi()
        pipe.set(k, "mine")
        assert pipe.execute() == [True]
    assert r.get(k) == "mine"

    # UNWATCH via reset, then a plain transaction.
    with r.pipeline() as pipe:
        pipe.watch(p + "other")
        pipe.unwatch()
        pipe.multi()
        pipe.incr(p + "n")
        assert pipe.execute() == [1]


def test_watch_sees_expiry_and_delete(server, r, p):
    k = p + "k"
    r.set(k, "v")
    with r.pipeline() as pipe:
        pipe.watch(k)
        with server.client() as other:
            other.delete(k)
        pipe.multi()
        pipe.set(p + "out", "1")
        with pytest.raises(WatchError):
            pipe.execute()
    assert r.exists(p + "out") == 0


def test_plain_pipeline(r, p):
    with r.pipeline(transaction=False) as pipe:
        for i in range(1000):
            pipe.set(f"{p}{i}", i)
        assert all(pipe.execute())
    with r.pipeline(transaction=False) as pipe:
        for i in range(1000):
            pipe.get(f"{p}{i}")
        assert pipe.execute() == [str(i) for i in range(1000)]
    # Errors are returned in place when asked to.
    with r.pipeline(transaction=False) as pipe:
        pipe.lpush(p + "0", "x").get(p + "1")
        result = pipe.execute(raise_on_error=False)
        assert isinstance(result[0], redis.ResponseError)
        assert result[1] == "1"


def test_pipeline_select_database(server, protocol, p):
    with server.client(protocol, db=7) as r7:
        with r7.pipeline() as pipe:
            pipe.set(p + "k", "seven").get(p + "k")
            assert pipe.execute() == [True, "seven"]
        r7.delete(p + "k")


# Lua


def test_eval_and_reply_types(r, p):
    script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('INCRBY', KEYS[2], ARGV[2])"
    assert r.eval(script, 2, p + "s", p + "n", "hello", 41) == 41
    assert r.get(p + "s") == "hello"

    assert r.eval("return {1, 'two', {3}, redis.status_reply('OK')}", 0) == [1, "two", [3], "OK"]
    assert r.eval("return nil", 0) is None
    assert r.eval("return true", 0) == 1
    assert r.eval("return false", 0) is None
    assert r.eval("return 3.99", 0) == 3
    assert r.eval("return redis.call('GET', KEYS[1])", 1, p + "missing") is None
    assert r.eval("return redis.call('HGETALL', KEYS[1])", 1, p + "missing") == []
    with pytest.raises(redis.ResponseError, match="my error"):
        r.eval("return redis.error_reply('my error')", 0)
    with pytest.raises(redis.ResponseError):
        r.eval("return redis.call('NO-SUCH-COMMAND')", 0)
    with pytest.raises(redis.ResponseError):
        r.eval("this is not lua", 0)
    assert r.eval("return redis.pcall('INCR', KEYS[1])['err'] ~= nil", 1, p + "s") == 1
    assert r.eval_ro("return redis.call('GET', KEYS[1])", 1, p + "s") == "hello"
    with pytest.raises(redis.ResponseError):
        r.eval_ro("return redis.call('SET', KEYS[1], 'x')", 1, p + "s")


def test_lua_libraries(r):
    assert r.eval("return cjson.encode({a = 1})", 0) == '{"a":1}'
    assert r.eval("return cjson.decode(ARGV[1])['b']", 0, '{"b": "c"}') == "c"
    assert r.eval("return redis.sha1hex('')", 0) == "da39a3ee5e6b4b0d3255bfef95601890afd80709"
    assert r.eval("return string.format('%d-%s', 7, 'x')", 0) == "7-x"
    assert r.eval("return tostring(math.floor(2.7))", 0) == "2"


def test_register_script_falls_back_to_load(r, p):
    limiter = r.register_script(
        "local n = redis.call('INCR', KEYS[1]) "
        "if n == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end "
        "if n > tonumber(ARGV[2]) then return 0 else return 1 end"
    )
    r.script_flush()
    assert r.script_exists(limiter.sha) == [False]
    # EVALSHA fails with NOSCRIPT, then redis-py loads it and tries again.
    allowed = sum(limiter(keys=[p + "rate"], args=[60_000, 5]) for _ in range(8))
    assert allowed == 5
    assert r.script_exists(limiter.sha) == [True]
    assert r.script_load(limiter.script) == limiter.sha
    assert 0 < r.pttl(p + "rate") <= 60_000

    with pytest.raises(NoScriptError):
        r.evalsha("0" * 40, 0)
    assert r.evalsha(limiter.sha, 1, p + "rate", 60_000, 100) == 1


def test_script_in_pipeline(r, p):
    script = r.register_script("return redis.call('INCRBY', KEYS[1], ARGV[1])")
    r.script_flush()
    with r.pipeline() as pipe:
        script(keys=[p + "n"], args=[5], client=pipe)
        script(keys=[p + "n"], args=[5], client=pipe)
        pipe.get(p + "n")
        assert pipe.execute() == [5, 10, "10"]


# Lock


def test_lock(server, r, p):
    lock = r.lock(p + "lock", timeout=30)
    assert lock.acquire(blocking=False)
    assert lock.locked() and lock.owned()
    other = r.lock(p + "lock", timeout=30)
    assert other.acquire(blocking=False) is False
    assert other.acquire(blocking_timeout=0.2) is False
    assert other.owned() is False
    assert lock.extend(30)
    assert 30_000 < r.pttl(p + "lock") <= 60_000
    assert lock.reacquire()
    assert 0 < r.pttl(p + "lock") <= 30_000
    with pytest.raises(LockNotOwnedError):
        other.do_release(b"not-my-token")
    lock.release()
    assert lock.locked() is False
    with pytest.raises(LockError):
        lock.release()

    with r.lock(p + "lock", timeout=5, blocking_timeout=1) as held:
        assert held.owned()
    assert r.exists(p + "lock") == 0


def test_lock_expires_and_is_handed_over(server, r, p):
    first = r.lock(p + "lock", timeout=0.3)
    assert first.acquire(blocking=False)
    taken = []

    def take():
        with server.client() as c:
            second = c.lock(p + "lock", timeout=5, sleep=0.05)
            taken.append(second.acquire(blocking_timeout=5))
            second.release()

    t = threading.Thread(target=take)
    t.start()
    t.join(10)
    assert taken == [True]
    with pytest.raises(LockNotOwnedError):
        first.release()
