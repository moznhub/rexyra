"""Compares what Rexyra says about its commands with what Redis says.

Usage:
    python tools/compare-command-info.py REDIS_PORT REXYRA_PORT
    python tools/compare-command-info.py --dump REDIS_PORT [COMMAND ...]

Both servers must be running on 127.0.0.1. For every command in Rexyra's
table (`COMMAND LIST`), it asks both servers for `COMMAND INFO` in RESP2 and
in RESP3 and compares the replies field by field, with their types (a set
is not an array, a simple string is not a bulk string). Then it runs
`COMMAND GETKEYS` and `COMMAND GETKEYSANDFLAGS` for a corpus of realistic
invocations, in both protocols, and compares those.

Rexyra does not list command tips (element 7) or subcommands (element 9),
so those are compared only where Redis has none either. Known differences,
each listed in docs/COMPATIBILITY.md, are in `KNOWN`.

`--dump` prints Redis's `COMMAND INFO` for the given commands (or for every
command in Rexyra's table) in the same notation, which is how the key
specifications in crates/commands/src/table.rs were checked.

Uses only the Python standard library. Exits with status 1 when anything
differs.
"""
import pathlib
import re
import socket
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

# Where Rexyra knowingly answers differently, and why. Keyed by the command
# line (as a tuple of arguments) or by command name for COMMAND INFO.
KNOWN = {
    ("MEMORY", "STATS"): "Rexyra has no MEMORY STATS, so it is an unknown subcommand",
}


class Conn:
    def __init__(self, port, resp3):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.buf = b""
        if resp3:
            self.call("HELLO", "3")

    def call(self, *args):
        out = b"*%d\r\n" % len(args)
        for arg in args:
            data = arg.encode() if isinstance(arg, str) else arg
            out += b"$%d\r\n%s\r\n" % (len(data), data)
        self.sock.sendall(out)
        return self.read()

    def line(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("connection closed")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def exactly(self, n):
        while len(self.buf) < n + 2:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("connection closed")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        return data

    def read(self):
        """One reply, as (type, value) so that types are compared too."""
        line = self.line()
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return ("simple", rest.decode())
        if kind == b"-":
            return ("error", rest.decode())
        if kind == b":":
            return ("int", int(rest))
        if kind == b"$":
            n = int(rest)
            return ("null",) if n < 0 else ("bulk", self.exactly(n).decode(errors="replace"))
        if kind == b"_":
            return ("null",)
        if kind == b"#":
            return ("bool", rest == b"t")
        if kind in (b"*", b"~", b">"):
            n = int(rest)
            if n < 0:
                return ("null",)
            name = {b"*": "array", b"~": "set", b">": "push"}[kind]
            return (name, [self.read() for _ in range(n)])
        if kind == b"%":
            n = int(rest)
            return ("map", [(self.read(), self.read()) for _ in range(n)])
        raise ValueError("unexpected reply %r" % line)


def show(reply):
    """A compact, readable rendering of a typed reply."""
    kind = reply[0]
    if kind in ("simple", "bulk"):
        return reply[1] if kind == "simple" else repr(reply[1])
    if kind == "error":
        return "(error) " + reply[1]
    if kind == "int":
        return str(reply[1])
    if kind == "null":
        return "nil"
    if kind == "bool":
        return "true" if reply[1] else "false"
    if kind == "map":
        return "{" + ", ".join("%s: %s" % (show(k), show(v)) for k, v in reply[1]) + "}"
    if kind == "ignored":
        return "(not compared)"
    open_, close = {"array": ("[", "]"), "set": ("~{", "}"), "push": (">[", "]")}[kind]
    return open_ + ", ".join(show(item) for item in reply[1]) + close


def table_commands():
    text = (ROOT / "crates/commands/src/table.rs").read_text(encoding="utf-8")
    return re.findall(r'^\s*cmd\("([a-z_]+)"', text, re.M)


def comparable_info(reply, reference):
    """COMMAND INFO's entry without what Rexyra does not fill in: the tips
    and the subcommands are compared only where Redis has none (so only
    their type), since Rexyra lists neither."""
    if reply[0] != "array" or not reply[1] or reply[1][0][0] != "array":
        return reply
    fields = list(reply[1][0][1])
    theirs = reference[1][0][1] if reference[0] == "array" and reference[1] else []
    for i in (7, 9):
        if len(fields) == 10 and len(theirs) == 10 and theirs[i][1]:
            fields[i] = ("ignored",)
    return ("array", [("array", fields)])


# Realistic invocations of every command that has keys, plus some edge
# cases of the commands whose keys Redis finds with code of its own.
GETKEYS_CORPUS = [
    "DEL a b c", "UNLINK a b", "EXISTS a b", "TOUCH a b", "TYPE a",
    "RENAME a b", "RENAMENX a b", "COPY a b", "COPY a b DB 1 REPLACE", "MOVE a 1",
    "EXPIRE a 10", "EXPIRE a 10 NX", "PEXPIRE a 10", "EXPIREAT a 10", "PEXPIREAT a 10",
    "TTL a", "PTTL a", "EXPIRETIME a", "PEXPIRETIME a", "PERSIST a",
    "OBJECT ENCODING a", "OBJECT FREQ a", "OBJECT IDLETIME a", "OBJECT REFCOUNT a",
    "OBJECT HELP",
    "GET a", "SET a 1", "SET a 1 GET", "SET a 1 EX 10 NX", "SETNX a 1", "SETEX a 10 1",
    "PSETEX a 10 1", "GETSET a 1", "GETDEL a", "GETEX a", "GETEX a PERSIST", "MGET a b c",
    "MSET a 1 b 2", "MSETNX a 1 b 2", "INCR a", "DECR a", "INCRBY a 1", "DECRBY a 1",
    "INCRBYFLOAT a 1.5", "APPEND a x", "STRLEN a", "GETRANGE a 0 1", "SUBSTR a 0 1",
    "SETRANGE a 0 x", "LCS a b", "LCS a b LEN",
    "SETBIT a 0 1", "GETBIT a 0", "BITCOUNT a", "BITCOUNT a 0 1", "BITPOS a 1",
    "BITOP AND d a b", "BITOP NOT d a", "BITFIELD a GET u8 0", "BITFIELD a SET u8 0 1",
    "BITFIELD_RO a GET u8 0",
    "HSET h f v", "HMSET h f v", "HSETNX h f v", "HGET h f", "HMGET h f g", "HDEL h f",
    "HGETALL h", "HKEYS h", "HVALS h", "HLEN h", "HEXISTS h f", "HSTRLEN h f",
    "HINCRBY h f 1", "HINCRBYFLOAT h f 1", "HSCAN h 0", "HRANDFIELD h", "HRANDFIELD h 2",
    "HEXPIRE h 10 FIELDS 1 f", "HPEXPIRE h 10 FIELDS 1 f", "HEXPIREAT h 10 FIELDS 1 f",
    "HPEXPIREAT h 10 FIELDS 1 f", "HTTL h FIELDS 1 f", "HPTTL h FIELDS 1 f",
    "HEXPIRETIME h FIELDS 1 f", "HPEXPIRETIME h FIELDS 1 f", "HPERSIST h FIELDS 1 f",
    "LPUSH l a", "RPUSH l a b", "LPUSHX l a", "RPUSHX l a", "LPOP l", "RPOP l 2",
    "LMPOP 2 a b LEFT", "LMPOP 1 a RIGHT COUNT 2", "LMOVE src dst LEFT RIGHT",
    "RPOPLPUSH a b", "BLPOP a b 0", "BRPOP a 0", "BLMPOP 0 2 a b LEFT",
    "BLMPOP 0 1 a LEFT COUNT 3", "BLMOVE a b LEFT RIGHT 0", "BRPOPLPUSH a b 0",
    "LLEN l", "LRANGE l 0 -1", "LINDEX l 0", "LPOS l a", "LSET l 0 a",
    "LINSERT l BEFORE a b", "LREM l 0 a", "LTRIM l 0 1",
    "SORT l", "SORT l ALPHA", "SORT l STORE d", "SORT l BY w_* GET # STORE d",
    "SORT l LIMIT 0 10 STORE d", "SORT l GET STORE STORE d", "SORT l STORE a STORE b",
    "SORT l BY nosort", "SORT_RO l", "SORT_RO l BY w_* GET o_*",
    "MEMORY USAGE a", "MEMORY USAGE a SAMPLES 5", "MEMORY STATS",
    "DUMP a", "RESTORE a 0 x", "RESTORE a 0 x REPLACE",
    "EVAL s 2 a b c", "EVAL s 0", "EVAL s 0 x", "EVAL s 3 a b", "EVAL s -1",
    "EVAL s x a", "EVALSHA s 1 a", "EVAL_RO s 1 a", "EVALSHA_RO s 2 a b",
    "FCALL f 1 a x", "FCALL f 0", "FCALL_RO f 2 a b",
    "WATCH a b",
    "SADD s a", "SREM s a", "SCARD s", "SISMEMBER s a", "SMISMEMBER s a b", "SMEMBERS s",
    "SMOVE a b m", "SPOP s", "SRANDMEMBER s 2", "SSCAN s 0", "SINTER a b", "SUNION a b",
    "SDIFF a b", "SINTERSTORE d a b", "SUNIONSTORE d a b", "SDIFFSTORE d a b",
    "SINTERCARD 2 a b", "SINTERCARD 2 a b LIMIT 1", "SINTERCARD 3 a b",
    "ZADD z 1 a", "ZINCRBY z 1 a", "ZREM z a", "ZCARD z", "ZSCORE z a", "ZMSCORE z a b",
    "ZRANK z a", "ZREVRANK z a", "ZCOUNT z 0 1", "ZLEXCOUNT z - +", "ZRANGE z 0 -1",
    "ZREVRANGE z 0 -1", "ZRANGEBYSCORE z 0 1", "ZREVRANGEBYSCORE z 1 0",
    "ZRANGEBYLEX z - +", "ZREVRANGEBYLEX z + -", "ZRANGESTORE d z 0 -1",
    "ZREMRANGEBYRANK z 0 1", "ZREMRANGEBYSCORE z 0 1", "ZREMRANGEBYLEX z - +",
    "ZPOPMIN z", "ZPOPMAX z 2", "ZMPOP 2 a b MIN", "ZMPOP 1 a MAX COUNT 2",
    "BZPOPMIN a b 0", "BZPOPMAX a 0", "BZMPOP 0 2 a b MIN", "ZUNION 2 a b",
    "ZUNION 2 a b WEIGHTS 1 2 WITHSCORES", "ZINTER 2 a b", "ZDIFF 2 a b",
    "ZUNIONSTORE d 2 a b", "ZUNIONSTORE d 2 a b WEIGHTS 1 2", "ZINTERSTORE d 2 a b",
    "ZDIFFSTORE d 2 a b", "ZUNIONSTORE d 3 a b", "ZUNIONSTORE d 0 a",
    "ZINTERCARD 2 a b", "ZINTERCARD 2 a b LIMIT 1", "ZSCAN z 0", "ZRANDMEMBER z",
    # Where the flags depend on the other arguments.
    "SET a 1 get", "SET a GET", "SET a 1 EX GET", "SET a 1 xGET", "BITFIELD a",
    "BITFIELD a OVERFLOW WRAP", "BITFIELD a OVERFLOW", "BITFIELD a GET SET 0",
    "BITFIELD a GET u8", "BITFIELD a x SET", "BITFIELD a INCRBY u8 0 1",
    "BITFIELD a GET u8 0 OVERFLOW SAT GET u8 8",
    # Where Redis falls back to a procedure of the command's own.
    "SORT l GET # GET STORE STORE e", "SORT l LIMIT 0 STORE d", "SORT l STORE",
    "SORT l STORE d LIMIT 1 2 STORE", "SORT STORE STORE d", "SORT l limit 1 2 by x get y store z",
    "SORT_RO l STORE d", "EVAL s 1x a", "EVAL s 01 a", "EVAL s +1 a", "EVAL s 2 a",
    "EVALSHA s 1x a", "EVAL_RO s 1x a", "FCALL f 1x a", "FCALL f 3 a", "FCALL_RO f 1x a",
    "ZUNIONSTORE d 1x a", "ZUNIONSTORE d x a", "ZUNIONSTORE d -1 a", "ZINTERSTORE d 01 a",
    "ZUNION 1x a", "ZUNION 3 a b", "ZDIFF 01 a", "LMPOP 1x a LEFT", "LMPOP 3 a b",
    "LMPOP 3 a b LEFT", "BLMPOP 0 1x a LEFT", "BLMPOP 0 3 a b", "ZMPOP 1x a MIN",
    "ZMPOP 3 a b MIN", "BZMPOP 0 1x a MIN", "SINTERCARD 1x a", "SINTERCARD 2 a",
    "SINTERCARD 0 a b", "ZINTERCARD 1x a", "ZINTERCARD 3 a b",
    # No keys, or wrong arguments: the errors must match.
    "PING", "ECHO", "PUBLISH c m", "SUBSCRIBE c", "SPUBLISH c m", "SSUBSCRIBE c d",
    "SUNSUBSCRIBE", "SUNSUBSCRIBE c", "CONFIG GET x", "CONFIG GET", "CONFIG NOPE", "NOPE a",
    "GET", "GET a b", "OBJECT", "OBJECT NOPE a", "OBJECT ENCODING", "OBJECT ENCODING a b",
    "OBJECT HELP x", "MEMORY", "MEMORY USAGE", "MEMORY NOPE", "LMPOP 0 a LEFT",
    "LMPOP x a LEFT", "ZUNION 0 a", "BLMPOP 0 3 a b LEFT", "BLMPOP 0 4 a b LEFT",
    "BLMPOP 0 0 a LEFT",
]


def compare(redis_port, rexyra_port):
    failures = 0
    known = set()
    commands = None

    def check(proto, what, key, want, got):
        nonlocal failures
        if want == got:
            return
        if key in KNOWN:
            known.add(key)
            return
        failures += 1
        print("%s %s\n  redis:  %s\n  rexyra: %s" % (proto, what, show(want), show(got)))

    for resp3 in (False, True):
        proto = "RESP3" if resp3 else "RESP2"
        redis, rexyra = Conn(redis_port, resp3), Conn(rexyra_port, resp3)
        if commands is None:
            listed = rexyra.call("COMMAND", "LIST")
            commands = sorted(item[1] for item in listed[1])
        for name in commands:
            reference = redis.call("COMMAND", "INFO", name)
            want = comparable_info(reference, reference)
            got = comparable_info(rexyra.call("COMMAND", "INFO", name), reference)
            check(proto, "COMMAND INFO " + name, name, want, got)
        for line in GETKEYS_CORPUS:
            args = line.split()
            for sub in ("GETKEYS", "GETKEYSANDFLAGS"):
                want = redis.call("COMMAND", sub, *args)
                got = rexyra.call("COMMAND", sub, *args)
                check(proto, "COMMAND %s %s" % (sub, line), tuple(args), want, got)
    for key in sorted(known, key=str):
        print("known difference, %s: %s" % (" ".join(key) if isinstance(key, tuple) else key,
                                            KNOWN[key]))
    print("%d commands' COMMAND INFO and %d GETKEYS/GETKEYSANDFLAGS lines compared in "
          "RESP2 and RESP3: %d known differences, %d others"
          % (len(commands), len(GETKEYS_CORPUS), len(known), failures))
    return failures


def dump(redis_port, names):
    redis = Conn(redis_port, True)
    for name in names or table_commands():
        reply = redis.call("COMMAND", "INFO", name)
        entry = reply[1][0]
        if entry[0] != "array":
            print(name, "unknown to Redis")
            continue
        fields = entry[1]
        print("%s arity=%s flags=%s keys=(%s,%s,%s) specs=%s"
              % (name, show(fields[1]), show(fields[2]), show(fields[3]), show(fields[4]),
                 show(fields[5]), show(fields[8])))
        for sub in fields[9][1]:
            sub_fields = sub[1]
            print("  %s arity=%s flags=%s keys=(%s,%s,%s) specs=%s"
                  % (show(sub_fields[0]), show(sub_fields[1]), show(sub_fields[2]),
                     show(sub_fields[3]), show(sub_fields[4]), show(sub_fields[5]),
                     show(sub_fields[8])))


def main(argv):
    if len(argv) >= 2 and argv[0] == "--dump":
        dump(int(argv[1]), argv[2:])
        return 0
    if len(argv) != 2:
        print(__doc__)
        return 2
    return 1 if compare(int(argv[0]), int(argv[1])) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
