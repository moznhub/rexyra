"""Pub/Sub: subscribe, psubscribe, publish, get_message and run_in_thread."""

import threading
import time

import pytest
import redis


def differs(reason):
    return pytest.mark.xfail("server_is_rexyra", reason=reason, strict=True)


def next_message(pubsub, timeout=5.0):
    """The next message other than None, or fail after the timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        message = pubsub.get_message(timeout=0.1)
        if message is not None:
            return message
    raise AssertionError("no message within the timeout")


def test_subscribe_and_publish(pubsub_ok, r, p):
    ch = p + "ch"
    with r.pubsub() as ps:
        ps.subscribe(ch)
        assert next_message(ps) == {"type": "subscribe", "pattern": None, "channel": ch, "data": 1}
        assert r.publish(ch, "hello") == 1
        assert next_message(ps) == {"type": "message", "pattern": None, "channel": ch, "data": "hello"}
        assert r.publish(p + "other", "x") == 0

        ps.subscribe(ch + "2")
        assert next_message(ps)["data"] == 2
        ps.unsubscribe(ch)
        assert next_message(ps) == {"type": "unsubscribe", "pattern": None, "channel": ch, "data": 1}
        assert r.publish(ch, "gone") == 0
        ps.unsubscribe()
        assert next_message(ps)["type"] == "unsubscribe"
        assert ps.subscribed is False


def test_psubscribe(pubsub_ok, r, p):
    with r.pubsub() as ps:
        ps.psubscribe(p + "news.*")
        assert next_message(ps) == {"type": "psubscribe", "pattern": None, "channel": p + "news.*", "data": 1}
        assert r.publish(p + "news.sport", "goal") == 1
        assert next_message(ps) == {
            "type": "pmessage",
            "pattern": p + "news.*",
            "channel": p + "news.sport",
            "data": "goal",
        }
        ps.punsubscribe(p + "news.*")
        assert next_message(ps)["type"] == "punsubscribe"


def test_ignore_subscribe_messages_and_binary(pubsub_ok, server, protocol, parser, p):
    with server.client(protocol, parser, decode_responses=False) as r:
        with r.pubsub(ignore_subscribe_messages=True) as ps:
            ps.subscribe(p + "bin")
            # Let the subscribe confirmation be read and dropped.
            ps.get_message(timeout=0.5)
            payload = bytes(range(256))
            assert r.publish(p + "bin", payload) == 1
            message = next_message(ps)
            assert message["type"] == "message" and message["data"] == payload


def test_pubsub_introspection(pubsub_ok, r, p):
    with r.pubsub() as ps:
        ps.subscribe(p + "a", p + "b")
        ps.psubscribe(p + "*")
        for _ in range(3):
            next_message(ps)
        assert set(r.pubsub_channels(p + "*")) == {p + "a", p + "b"}
        assert dict(r.pubsub_numsub(p + "a", p + "none")) == {p + "a": 1, p + "none": 0}
        assert r.pubsub_numpat() >= 1


def test_ping_while_subscribed(pubsub_ok, r, protocol, p):
    if protocol == 3:
        # In RESP3 PING replies with a plain PONG even while subscribed, which
        # redis-py's PubSub.handle_message cannot parse (IndexError) on Redis too.
        pytest.skip("redis-py cannot read a RESP3 PONG on a subscribed connection")
    with r.pubsub() as ps:
        ps.subscribe(p + "ch")
        next_message(ps)
        ps.ping("hi")
        message = next_message(ps)
        assert message["type"] == "pong"
        assert message["data"] in ("hi", ["pong", "hi"])
        ps.check_health()


def test_run_in_thread(pubsub_ok, r, p):
    received = []
    event = threading.Event()

    def handler(message):
        received.append(message["data"])
        if len(received) == 3:
            event.set()

    ps = r.pubsub()
    ps.subscribe(**{p + "ch": handler})
    ps.psubscribe(**{p + "pat.*": handler})
    thread = ps.run_in_thread(sleep_time=0.01, daemon=True)
    try:
        deadline = time.monotonic() + 5
        while r.pubsub_numsub(p + "ch")[0][1] != 1 or r.pubsub_numpat() < 1:
            assert time.monotonic() < deadline
            time.sleep(0.05)
        r.publish(p + "ch", "one")
        r.publish(p + "pat.x", "two")
        r.publish(p + "ch", "three")
        assert event.wait(5)
        assert received == ["one", "two", "three"]
    finally:
        thread.stop()
        thread.join(5)
        ps.close()


def test_sharded_pubsub(pubsub_ok, r, p):
    with r.pubsub() as ps:
        ps.ssubscribe(p + "shard")
        assert next_message(ps)["type"] == "ssubscribe"
        assert r.spublish(p + "shard", "hello") == 1
        message = ps.get_sharded_message(timeout=5)
        assert message["type"] == "smessage" and message["data"] == "hello"
        assert r.pubsub_shardchannels(p + "*") == [p + "shard"]
        assert dict(r.pubsub_shardnumsub(p + "shard")) == {p + "shard": 1}


def test_pubsub_resubscribes_after_connection_drop(pubsub_ok, server, r, p):
    with r.pubsub() as ps:
        ps.subscribe(p + "ch")
        next_message(ps)
        with server.client() as admin:
            ids = [c["id"] for c in admin.client_list() if "subscribe" in c.get("cmd", "")]
            assert ids
            for client_id in ids:
                admin.client_kill_filter(_id=client_id)
        # redis-py reconnects and subscribes again on the next read.
        deadline = time.monotonic() + 10
        while r.pubsub_numsub(p + "ch")[0][1] == 0:
            try:
                ps.get_message(timeout=0.1)
            except redis.ConnectionError:
                pass  # the first read sees the closed connection
            assert time.monotonic() < deadline, "did not resubscribe"
        message = None
        while message is None or message["type"] != "message":
            r.publish(p + "ch", "again")
            message = ps.get_message(timeout=0.5)
            assert time.monotonic() < deadline + 5
        assert message["data"] == "again"


def test_keyspace_notifications(pubsub_ok, r, p):
    old = r.config_get("notify-keyspace-events").get("notify-keyspace-events", "")
    assert r.config_set("notify-keyspace-events", "KEA")
    try:
        with r.pubsub() as ps:
            ps.psubscribe(f"__keyspace@0__:{p}*")
            next_message(ps)
            r.set(p + "k", "v")
            r.expire(p + "k", 100)
            r.delete(p + "k")
            events = [next_message(ps) for _ in range(3)]
            assert [m["channel"] for m in events] == [f"__keyspace@0__:{p}k"] * 3
            assert [m["data"] for m in events] == ["set", "expire", "del"]
    finally:
        r.config_set("notify-keyspace-events", old)
