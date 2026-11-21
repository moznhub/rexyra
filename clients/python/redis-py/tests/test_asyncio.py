"""A few basics through redis.asyncio."""

import asyncio

import pytest
import redis
import redis.asyncio


def differs(reason):
    return pytest.mark.xfail("server_is_rexyra", reason=reason, strict=True)


@pytest.fixture
async def ar(server, protocol):
    client = server.async_client(protocol)
    yield client
    await client.aclose()


async def test_basics(ar, protocol, p):
    assert await ar.ping() is True
    assert int((await ar.client_info())["resp"]) == protocol
    assert await ar.set(p + "k", "v")
    assert await ar.get(p + "k") == "v"
    await ar.hset(p + "h", mapping={"a": "1"})
    assert await ar.hgetall(p + "h") == {"a": "1"}
    await ar.zadd(p + "z", {"m": 2})
    assert await ar.zscore(p + "z", "m") == 2.0
    assert await ar.expire(p + "k", 100)
    assert 0 < await ar.ttl(p + "k") <= 100


async def test_many_concurrent_callers(server, protocol, p):
    # As a web application would: many tasks share a bounded pool.
    pool = redis.asyncio.BlockingConnectionPool(max_connections=20, timeout=20, **server.kwargs(protocol))
    ar = redis.asyncio.Redis(connection_pool=pool)
    await asyncio.gather(*(ar.incr(p + "n") for _ in range(500)))
    assert await ar.get(p + "n") == "500"
    values = await asyncio.gather(*(ar.set(f"{p}{i}", i) for i in range(200)))
    assert all(values)
    assert await ar.mget([f"{p}{i}" for i in range(200)]) == [str(i) for i in range(200)]
    await ar.aclose()
    await pool.disconnect()


async def test_transaction_and_watch(server, ar, p):
    k = p + "k"
    await ar.set(k, "1")
    async with ar.pipeline(transaction=True) as pipe:
        assert await pipe.incr(k).incr(k).get(k).execute() == [2, 3, "3"]

    async with ar.pipeline() as pipe:
        await pipe.watch(k)
        other = server.async_client()
        await other.set(k, "changed")
        await other.aclose()
        pipe.multi()
        pipe.set(k, "mine")
        with pytest.raises(redis.WatchError):
            await pipe.execute()
    assert await ar.get(k) == "changed"


async def test_script_and_lock(ar, p):
    script = ar.register_script("return redis.call('INCRBY', KEYS[1], ARGV[1])")
    await ar.script_flush()
    assert await script(keys=[p + "n"], args=[3]) == 3
    assert await ar.eval("return ARGV[1]", 0, "x") == "x"

    lock = ar.lock(p + "lock", timeout=10)
    assert await lock.acquire(blocking=False)
    assert await ar.lock(p + "lock", timeout=10).acquire(blocking=False) is False
    await lock.release()


async def test_pubsub(ar, p):
    async with ar.pubsub() as ps:
        await ps.subscribe(p + "ch")
        message = await ps.get_message(timeout=5)
        assert message["type"] == "subscribe"
        assert await ar.publish(p + "ch", "hi") == 1
        message = None
        for _ in range(50):
            message = await ps.get_message(timeout=0.1)
            if message:
                break
        assert message == {"type": "message", "pattern": None, "channel": p + "ch", "data": "hi"}


async def test_reconnects_after_client_kill(server, ar, p):
    await ar.set(p + "k", "before")
    my_id = await ar.client_id()
    admin = server.async_client()
    assert await admin.client_kill_filter(_id=my_id) == 1
    await admin.aclose()
    assert await ar.get(p + "k") == "before"


async def test_wrong_password(server, protocol):
    if server.password is None:
        pytest.skip("the server under test has no password")
    bad = server.async_client(protocol, password="not-the-password")
    with pytest.raises(redis.AuthenticationError):
        await bad.ping()
    await bad.aclose()
