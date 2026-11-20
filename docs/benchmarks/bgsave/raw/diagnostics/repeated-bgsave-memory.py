# Does used_memory grow on every BGSAVE, or only on the first after a load?
# Starts its own rr-server (stopped through its own handle), loads N keys,
# runs three BGSAVEs (and one after overwriting every key), prints used_memory.
import os, shutil, socket, subprocess, sys, time

EXE = r"target\release\rr-server.exe"
DIR = r"target\bench-data\twosaves"
PORT = 6410
N = 200_000

shutil.rmtree(DIR, ignore_errors=True)
os.makedirs(DIR)
proc = subprocess.Popen([EXE, "--port", str(PORT), "--dir", DIR, "--save", ""],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    for _ in range(200):
        try:
            s = socket.create_connection(("127.0.0.1", PORT)); break
        except OSError:
            time.sleep(0.05)
    f = s.makefile("rb")

    def enc(*a):
        return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)

    def reply():
        line = f.readline()
        if line[:1] == b"$":
            n = int(line[1:])
            data = f.read(n + 2)[:-2]
            return data
        return line.strip()

    def call(*a):
        s.sendall(enc(*[x if isinstance(x, bytes) else str(x).encode() for x in a]))
        return reply()

    def used():
        txt = call("INFO", "memory").decode()
        return int([l for l in txt.split("\r\n") if l.startswith("used_memory:")][0].split(":")[1])

    def load():
        v = b"x" * 1000
        for lo in range(0, N, 1000):
            s.sendall(b"".join(enc(b"SET", b"s:%d" % i, v) for i in range(lo, lo + 1000)))
            for _ in range(1000):
                reply()

    def bgsave():
        call("BGSAVE")
        while b"rdb_bgsave_in_progress:1" in call("INFO", "persistence"):
            time.sleep(0.01)
        time.sleep(0.5)

    load(); time.sleep(0.5)
    print("keys", call("DBSIZE").decode())
    u0 = used(); print("after load      ", u0)
    for i in (1, 2, 3):
        bgsave(); u = used(); print("after BGSAVE %d  " % i, u, "delta vs load %+d" % (u - u0), "(%.2f B/key)" % ((u - u0) / N))
    load(); time.sleep(0.5)
    u1 = used(); print("after re-SET all", u1, "delta vs load %+d" % (u1 - u0))
    bgsave(); u = used(); print("after BGSAVE 4  ", u, "delta vs re-SET %+d" % (u - u1), "(%.2f B/key)" % ((u - u1) / N))
    call("SHUTDOWN", "NOSAVE")
finally:
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
    shutil.rmtree(DIR, ignore_errors=True)
