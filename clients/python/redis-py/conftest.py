"""Starts one rexyra-server for the whole test run and hands out redis-py clients.

REXYRA_SERVER names the executable (default: target/release/rexyra-server.exe in
the repository). The server listens on a free port, with an empty data folder,
no automatic saves and a password, and is stopped at the end of the run.

REXYRA_ENDPOINT (host:port) and REXYRA_PASSWORD use a server that is already
running instead, which is how the same tests are run against Redis to check the
tests themselves. That server's data is written to and some databases flushed.

Tests that show a known difference from Redis are marked with ``differs``:
they are expected to fail on Rexyra (strict xfail) and must pass on Redis.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

import pytest
import redis
import redis.asyncio
from redis._parsers import _HiredisParser, _RESP2Parser
from redis.utils import HIREDIS_AVAILABLE

PASSWORD = "client-tests-password"
# The redis-py version under test, e.g. (8, 1); some tests work around older ones.
REDIS_PY = tuple(int(x) for x in redis.__version__.split(".")[:2])
REPO_ROOT = Path(__file__).resolve().parents[3]


class Server:
    """The server under test: one started here, or one named by REXYRA_ENDPOINT."""

    def __init__(self) -> None:
        self.process: subprocess.Popen | None = None
        self.data_dir: str | None = None
        self.log = None

        external = os.environ.get("REXYRA_ENDPOINT")
        if external:
            host, _, port = external.rpartition(":")
            self.host = host or "127.0.0.1"
            self.port = int(port)
            self.password = os.environ.get("REXYRA_PASSWORD") or None
            self.external = True
        else:
            self.external = False
            self._start()
        self.is_rexyra = self._detect_rexyra()

    def _start(self) -> None:
        exe = os.environ.get("REXYRA_SERVER") or str(
            REPO_ROOT / "target" / "release" / "rexyra-server.exe"
        )
        if not os.path.isfile(exe):
            raise RuntimeError(
                f"rexyra-server not found at {exe}; build it with "
                "`cargo build --release -p rexyra-server` or set REXYRA_SERVER"
            )
        self.host = "127.0.0.1"
        self.port = _free_port()
        self.password = PASSWORD
        self.data_dir = tempfile.mkdtemp(prefix="rexyra-client-tests-")
        self.log = open(os.path.join(self.data_dir, "server.log"), "wb")
        self.process = subprocess.Popen(
            [
                exe,
                "--port", str(self.port),
                "--dir", self.data_dir,
                "--save", "",
                "--requirepass", PASSWORD,
            ],
            stdout=self.log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"rexyra-server exited during start-up with code "
                    f"{self.process.returncode}: {self._log_text()}"
                )
            try:
                socket.create_connection((self.host, self.port), timeout=1).close()
                return
            except OSError:
                time.sleep(0.05)
        self.stop()
        raise RuntimeError("rexyra-server did not start listening within 30 seconds")

    def _log_text(self) -> str:
        try:
            self.log.flush()
            with open(os.path.join(self.data_dir, "server.log"), "rb") as f:
                return f.read().decode(errors="replace")
        except OSError:
            return ""

    def _detect_rexyra(self) -> bool:
        if not self.external:
            return True
        with self.client() as r:
            info = r.info("server")
        return any("rexyra" in str(k).lower() or "rexyra" in str(v).lower() for k, v in info.items())

    def stop(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.kill()  # this process only, by its handle
            self.process.wait(10)
        self.process = None
        if self.log is not None:
            self.log.close()
            self.log = None
        if self.data_dir is not None:
            shutil.rmtree(self.data_dir, ignore_errors=True)
            self.data_dir = None

    # Clients, configured the way an application would.

    def kwargs(self, protocol: int = 2, **extra) -> dict:
        """The keyword arguments of redis.Redis(...) for this server."""
        kwargs = dict(
            host=self.host,
            port=self.port,
            password=self.password,
            protocol=protocol,
            socket_timeout=10,
            socket_connect_timeout=10,
            decode_responses=True,
        )
        kwargs.update(extra)
        return kwargs

    def client(self, protocol: int = 2, parser: str = "python", **extra) -> redis.Redis:
        """redis.Redis(...) with the library's defaults (pool, retries), and
        the reply parser chosen: the pure-Python one or hiredis."""
        client = redis.Redis(**self.kwargs(protocol, **extra))
        set_parser(client.connection_pool, parser)
        return client

    def async_client(self, protocol: int = 2, **extra) -> redis.asyncio.Redis:
        return redis.asyncio.Redis(**self.kwargs(protocol, **extra))

    def url(self, db: int = 0, protocol: int = 2) -> str:
        auth = f":{self.password}@" if self.password else ""
        return f"redis://{auth}{self.host}:{self.port}/{db}?protocol={protocol}"


def set_parser(pool: redis.ConnectionPool, parser: str) -> None:
    """Choose the parser of the connections a pool is yet to make. redis.Redis
    has no parser argument of its own; it uses hiredis whenever installed."""
    # The RESP2 parser is swapped for the RESP3 one when protocol=3.
    pool.connection_kwargs["parser_class"] = _HiredisParser if parser == "hiredis" else _RESP2Parser


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


_server: Server | None = None


def get_server() -> Server:
    global _server
    if _server is None:
        _server = Server()
    return _server


def pytest_markeval_namespace(config):
    # `differs` marks evaluate "server_is_rexyra" lazily, so the server starts
    # only when the first test runs.
    return {"server_is_rexyra": get_server().is_rexyra}


def pytest_sessionfinish(session, exitstatus):
    global _server
    if _server is not None:
        _server.stop()
        _server = None


def pytest_report_header(config):
    import importlib.metadata as md

    target = os.environ.get("REXYRA_ENDPOINT") or os.environ.get("REXYRA_SERVER") or "target/release/rexyra-server.exe"
    hiredis = md.version("hiredis") if HIREDIS_AVAILABLE else "not installed"
    return [f"redis-py {redis.__version__}, hiredis {hiredis}", f"server under test: {target}"]


# Fixtures

PROTOCOLS = [2, 3]
PARSERS = ["python", "hiredis"]


@pytest.fixture(scope="session")
def server() -> Server:
    return get_server()


@pytest.fixture(params=PROTOCOLS, ids=lambda p: f"resp{p}")
def protocol(request) -> int:
    return request.param


@pytest.fixture(params=PARSERS)
def parser(request) -> str:
    if request.param == "hiredis" and not HIREDIS_AVAILABLE:
        pytest.skip("hiredis is not installed")
    return request.param


@pytest.fixture
def r(server, protocol, parser):
    """A client on database 0 in each protocol, with each parser."""
    client = server.client(protocol, parser)
    yield client
    client.close()


@pytest.fixture
def pubsub_ok(protocol, parser):
    """redis-py 5.x's pure-Python RESP3 parser does not deliver Pub/Sub pushes
    (get_message times out, on Redis too); fixed in 6.0."""
    if REDIS_PY < (6, 0) and protocol == 3 and parser == "python":
        pytest.skip("redis-py 5.x: RESP3 Pub/Sub needs hiredis")


def eventually(fn, timeout: float = 15):
    """Call fn until it stops raising ConnectionError, as an application
    without the library's retries (redis-py 5.x has none by default) would."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            return fn()
        except redis.ConnectionError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.1)


@pytest.fixture
def p() -> str:
    """A key prefix of its own for each test."""
    return f"py:{uuid.uuid4().hex}:"
