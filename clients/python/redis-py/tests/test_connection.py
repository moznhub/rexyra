"""Connecting, authentication, pools, databases and reconnecting."""

import threading
import time

import pytest
import redis
from conftest import REDIS_PY, eventually, set_parser


def differs(reason):
    return pytest.mark.xfail("server_is_rexyra", reason=reason, strict=True)


def test_connects_cleanly(r, protocol):
    assert r.ping() is True
    info = r.info("server")
    major, minor = (int(x) for x in str(info["redis_version"]).split(".")[:2])
    assert (major, minor) >= (7, 0)
    assert info["redis_mode"] == "standalone"

    client = r.client_info()
    assert int(client["resp"]) == protocol
    # redis-py announces itself with CLIENT SETINFO while connecting.
    assert client["lib-name"].startswith("redis-py")
    assert client["lib-ver"] == redis.__version__


def test_hello_reply(r, protocol):
    hello = r.execute_command("HELLO", protocol)
    if isinstance(hello, list):  # RESP2 replies with a flat list
        hello = dict(zip(hello[::2], hello[1::2]))
    assert hello["proto"] == protocol
    assert hello["role"] == "master"
    assert hello["mode"] == "standalone"


def test_wrong_password_is_refused(server, protocol):
    if server.password is None:
        pytest.skip("the server under test has no password")
    with server.client(protocol, password="not-the-password") as bad:
        with pytest.raises(redis.AuthenticationError):
            bad.ping()


def test_missing_password_is_refused(server, protocol):
    if server.password is None:
        pytest.skip("the server under test has no password")
    with server.client(protocol, password=None) as anonymous:
        with pytest.raises(redis.AuthenticationError):
            anonymous.ping()


def test_default_user_with_username(server, protocol):
    if server.password is None:
        pytest.skip("the server under test has no password")
    # AUTH default <password> / HELLO 3 AUTH default <password>
    with server.client(protocol, username="default") as r:
        assert r.ping()
        assert r.acl_whoami() == "default"


def test_from_url(server, protocol, p):
    with redis.from_url(server.url(db=3, protocol=protocol), decode_responses=True) as r:
        r.set(p + "k", "v")
        assert int(r.client_info()["db"]) == 3
        assert int(r.client_info()["resp"]) == protocol
    with server.client(protocol, db=3) as r:
        assert r.get(p + "k") == "v"
        r.delete(p + "k")


def test_connection_pool_shared_by_threads(server, protocol, parser, p):
    pool = redis.BlockingConnectionPool(max_connections=4, timeout=20, **server.kwargs(protocol))
    set_parser(pool, parser)
    r = redis.Redis(connection_pool=pool)
    errors = []

    def work():
        try:
            for _ in range(50):
                r.incr(p + "n")
                r.lpush(p + "l", "x")
        except Exception as e:  # pragma: no cover - reported below
            errors.append(e)

    threads = [threading.Thread(target=work) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert r.get(p + "n") == "1000"
    assert r.llen(p + "l") == 1000
    assert len(pool._connections) <= 4
    r.close()
    pool.disconnect()


def test_select_database(server, protocol, parser, p):
    with server.client(protocol, parser, db=4) as r4, server.client(protocol, parser) as r0:
        r4.set(p + "k", "four")
        assert r0.get(p + "k") is None
        assert r4.get(p + "k") == "four"
        assert r4.dbsize() >= 1
        r4.delete(p + "k")


def test_move_between_databases(server, protocol, parser, p):
    with server.client(protocol, parser, db=4) as r4, server.client(protocol, parser) as r0:
        r4.set(p + "k", "four")
        assert r4.move(p + "k", 0) is True
        assert r0.get(p + "k") == "four"
        assert r4.exists(p + "k") == 0
        r0.set(p + "k2", "x")
        r4.set(p + "k2", "y")
        assert r4.move(p + "k2", 0) is False  # already there
        r0.delete(p + "k", p + "k2")
        r4.delete(p + "k2")


def test_swapdb(server, protocol, p):
    with server.client(protocol, db=8) as r8, server.client(protocol, db=9) as r9:
        r8.set(p + "k", "eight")
        assert r8.swapdb(8, 9)
        assert r9.get(p + "k") == "eight"
        assert r8.get(p + "k") is None
        r9.delete(p + "k")


def test_select_on_single_connection_client(server, protocol, p):
    with server.client(protocol, single_connection_client=True) as r:
        assert r.execute_command("SELECT", 6)
        r.set(p + "k", "six")
        assert int(r.client_info()["db"]) == 6
        assert r.execute_command("SELECT", 0)
        assert r.get(p + "k") is None
    with server.client(protocol, db=6) as r6:
        assert r6.getdel(p + "k") == "six"


def test_client_name(r):
    assert r.client_setname("py-tests")
    assert r.client_getname() == "py-tests"
    names = [c["name"] for c in r.client_list()]
    assert "py-tests" in names
    me = [c for c in r.client_list() if c["id"] == str(r.client_id())]
    assert me and me[0]["name"] == "py-tests"


def test_reconnects_after_client_kill_by_id(server, protocol, parser, p):
    with server.client(protocol, parser) as r, server.client() as admin:
        r.set(p + "k", "before")
        old_id = r.client_id()
        assert admin.client_kill_filter(_id=old_id) == 1
        # The pooled connection is dead; the next command reconnects
        # (redis-py 6+ retries by itself).
        assert eventually(lambda: r.get(p + "k")) == "before"
        assert r.client_id() != old_id


def test_reconnects_after_client_kill_by_addr(server, protocol, p):
    with server.client(protocol) as r, server.client() as admin:
        r.set(p + "k", "before")
        addr = r.client_info()["addr"]
        assert admin.client_kill(addr) is True
        assert eventually(lambda: r.get(p + "k")) == "before"


def test_reconnects_after_client_kill_skipme(server, p):
    # As the .NET suite does: CLIENT KILL SKIPME yes drops every other connection.
    with server.client() as r, server.client() as admin:
        r.set(p + "k", "before")
        assert admin.client_kill_filter(skipme=True) >= 1
        assert eventually(lambda: r.get(p + "k")) == "before"
        assert admin.ping()


def test_health_check_interval(server, protocol, p):
    if REDIS_PY < (6, 0) and protocol == 3:
        pytest.skip("redis-py 5.x sends the health-check PING before HELLO AUTH (fails on Redis too)")
    with server.client(protocol, health_check_interval=1) as r:
        r.set(p + "k", "v")
        time.sleep(1.2)
        # A PING is sent first on the idle connection.
        assert r.get(p + "k") == "v"


def test_quit_like_close_and_reuse(server, protocol, p):
    r = server.client(protocol)
    r.set(p + "k", "v")
    r.close()
    # A closed client's pool reconnects on demand.
    assert r.get(p + "k") == "v"
    r.close()


def test_client_side_caching(server, p):
    # redis-py's own cache (5.1+): CLIENT TRACKING ON over RESP3, and the
    # server's invalidation pushes drop entries another client changed.
    if REDIS_PY < (6, 0):
        pytest.skip("redis-py 5.x client-side caching is broken (fails on Redis too)")
    from redis.cache import CacheConfig
    with server.client(3, cache_config=CacheConfig()) as r, server.client() as other:
        r.set(p + "k", "v1")
        assert r.get(p + "k") == "v1"
        assert r.get(p + "k") == "v1"
        cache = r.get_cache()
        assert cache.size >= 1
        other.set(p + "k", "v2")
        deadline = time.monotonic() + 5
        while r.get(p + "k") != "v2":
            assert time.monotonic() < deadline, "the cached value was never invalidated"
            time.sleep(0.05)
