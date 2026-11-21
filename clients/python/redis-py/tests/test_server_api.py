"""INFO, CONFIG, CLIENT, FLUSHDB, DBSIZE, SCAN and other server commands."""

import datetime

import pytest
import redis


def differs(reason):
    return pytest.mark.xfail("server_is_rexyra", reason=reason, strict=True)


def test_info(r):
    info = r.info()
    assert "redis_version" in info
    assert isinstance(info["connected_clients"], int)
    assert isinstance(info["used_memory"], int)
    server = r.info("server")
    assert isinstance(server["tcp_port"], int)
    assert isinstance(server["uptime_in_seconds"], int)
    everything = r.info("everything")
    assert "redis_version" in everything
    assert "used_memory" in r.info("memory")
    stats = r.info("stats")
    assert isinstance(stats["total_commands_processed"], int)
    # Several sections at once (Redis 7).
    both = r.info("server", "clients")
    assert "redis_version" in both and "connected_clients" in both


def test_info_keyspace(server, protocol, parser, p):
    with server.client(protocol, parser, db=5) as r:
        r.flushdb()
        r.set(p + "a", "1")
        r.set(p + "b", "1", ex=100)
        keyspace = r.info("keyspace")
        assert keyspace["db5"]["keys"] == 2
        assert keyspace["db5"]["expires"] == 1


def test_config_get(r):
    maxmemory = r.config_get("maxmemory")
    assert list(maxmemory) == ["maxmemory"]
    assert int(maxmemory["maxmemory"]) >= 0
    several = r.config_get("maxmemory", "databases")
    assert set(several) == {"maxmemory", "databases"}
    assert int(r.config_get("databases")["databases"]) >= 16
    assert r.config_get("no-such-parameter") == {}
    assert "maxmemory-policy" in r.config_get("maxmemory*")


def test_config_set(r):
    old = r.config_get("slowlog-max-len")["slowlog-max-len"]
    assert r.config_set("slowlog-max-len", 200)
    assert r.config_get("slowlog-max-len")["slowlog-max-len"] == "200"
    assert r.config_set("slowlog-max-len", old)
    with pytest.raises(redis.ResponseError):
        r.config_set("no-such-parameter", "1")


def test_client_commands(r):
    my_id = r.client_id()
    assert isinstance(my_id, int)
    assert r.client_setname("api-tests")
    assert r.client_getname() == "api-tests"
    clients = r.client_list()
    assert isinstance(clients, list) and clients
    me = next(c for c in clients if c["id"] == str(my_id))
    assert me["name"] == "api-tests"
    assert {"addr", "age", "idle", "db", "cmd"} <= set(me)
    assert r.client_list(client_id=[str(my_id)])[0]["name"] == "api-tests"
    assert r.client_list(_type="normal")
    info = r.client_info()
    assert int(info["id"]) == my_id
    assert r.client_setinfo("lib-name", "my-app")
    assert r.client_info()["lib-name"] == "my-app"
    assert r.client_no_evict("on") == "OK"
    assert r.client_no_touch("on") == "OK"


def test_flushdb_and_dbsize(server, protocol, parser, p):
    with server.client(protocol, parser, db=5) as r:
        r.flushdb()
        assert r.dbsize() == 0
        r.mset({f"{p}{i}": i for i in range(10)})
        assert r.dbsize() == 10
        assert r.flushdb(asynchronous=True)
        assert r.dbsize() == 0


def test_scan_iter(server, protocol, parser, p):
    with server.client(protocol, parser, db=5) as r:
        r.flushdb()
        with r.pipeline(transaction=False) as pipe:
            for i in range(500):
                pipe.set(f"{p}k:{i}", i)
            for i in range(50):
                pipe.sadd(f"{p}s:{i}", i)
            pipe.execute()
        assert len(set(r.scan_iter(count=50))) == 550
        assert len(set(r.scan_iter(match=p + "s:*", count=1000))) == 50
        assert len(set(r.scan_iter(_type="set"))) == 50
        cursor, keys = r.scan(0, count=10)
        assert isinstance(cursor, int)
        assert keys
        r.flushdb()


def test_misc_server_commands(r):
    seconds, micros = r.time()
    now = datetime.datetime.now().timestamp()
    assert abs(seconds - now) < 300 and 0 <= micros < 1_000_000
    assert r.echo("echo") == "echo"
    assert isinstance(r.lastsave(), datetime.datetime)
    assert isinstance(r.slowlog_get(10), list)
    assert isinstance(r.slowlog_len(), int)
    assert r.slowlog_reset()
    assert r.memory_usage("py:nothing-here") is None
    assert r.command_count() > 100
    assert r.acl_whoami() == "default"
    assert r.dbsize() >= 0


def test_memory_usage_and_object(r, p):
    r.set(p + "k", "v" * 100)
    assert r.memory_usage(p + "k") >= 100
    assert r.object("encoding", p + "k") == "raw"
    r.set(p + "n", 12345)
    assert r.object("encoding", p + "n") == "int"
    assert isinstance(r.object("idletime", p + "n"), int)
    assert r.object("refcount", p + "n") >= 1


def test_command_docs_parsed_by_redis_py(r):
    # redis-py parses COMMAND for its key-extraction helpers.
    commands = r.command()
    assert "get" in commands
    assert commands["get"]["arity"] == 2


def test_command_getkeys(r):
    assert r.command_getkeys("SET", "a", "b") == ["a"]
    assert r.command_getkeys("MSET", "a", "1", "b", "2") == ["a", "b"]
