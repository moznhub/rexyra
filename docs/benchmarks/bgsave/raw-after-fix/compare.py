"""Before/after comparison of the BGSAVE benchmark rounds.

Reads every top-level *.json run file in the old directory (default: ../raw)
and the new directory (default: this script's directory), groups them by
(scenario, label) taken from the JSON itself, and prints markdown tables of
the per-metric median over the runs of each configuration.

Usage (from the workspace root):
    python docs\\benchmarks\\bgsave\\raw-after-fix\\compare.py
    python docs\\benchmarks\\bgsave\\raw-after-fix\\compare.py --old <dir> --new <dir>

Definitions (all from the JSON files, nothing from `rr-bench summarize`):
  ops/s, p50, p99, max     windows.<w>.foreground.{throughput_per_s,p50_ms,p99_ms,max_ms}
  gap                      windows.<w>.longest_gap.longest_ms
  save s                   save.duration_ms / 1000
  peak-before MiB          memory.peak_used_minus_before / 2^20
  after-before MiB         (memory.after_last_used - memory.used_before) / 2^20
                           (after_last_used = last used_memory sample, i.e. at
                           the end of the 3 s after-window)
  median                   statistics.median of each metric separately over
                           the runs (3 runs -> the middle value), so a median
                           row need not be one actual run.
  stall buckets            50 ms timeline buckets lying wholly inside
                           [0, save end] whose foreground_max_ms > 10
                           (per run; the median is reported)
Checks: save.last_bgsave_status == "ok", verify.dbsize_match, and
verify.llen_match where present.
"""
import argparse
import glob
import json
import os
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
MiB = float(1 << 20)

ORDER = [
    ("small", None), ("str1m", None), ("str1m", "no-toucher"),
    ("str10m", None), ("str10m", "no-toucher"), ("str100m", None),
    ("str100m", "no-toucher"), ("coll", None), ("coll", "no-toucher"),
    ("slow-small", None), ("slow-str10m", None), ("idle", None),
]


def load(d):
    groups = {}
    for p in sorted(glob.glob(os.path.join(d, "*.json"))):
        with open(p, encoding="utf-8") as f:
            j = json.load(f)
        if j.get("tool") != "rr-bench":
            continue
        j["_file"] = os.path.basename(p)
        groups.setdefault((j["scenario"], j.get("label")), []).append(j)
    for runs in groups.values():
        runs.sort(key=lambda r: r["run"])
    return groups


def stall_buckets(j, threshold_ms=10.0):
    t = j.get("timeline")
    if not t:
        return None, None
    end = j["save"]["duration_ms"]
    b = t["bucket_ms"]
    s0 = t["first_bucket_starts_at_ms"]
    n_slow = 0
    n_inside = 0
    for i, m in enumerate(t["foreground_max_ms"]):
        lo = s0 + i * b
        if lo >= 0 and lo + b <= end:
            n_inside += 1
            if m is not None and m > threshold_ms:
                n_slow += 1
    return n_slow, n_inside


def metrics(j):
    m = {}
    if "windows" in j:
        for w in ("baseline", "during", "after"):
            fg = j["windows"][w]["foreground"]
            m[w + ".ops"] = fg["throughput_per_s"]
            m[w + ".p50"] = fg["p50_ms"]
            m[w + ".p99"] = fg["p99_ms"]
            m[w + ".max"] = fg["max_ms"]
            m[w + ".gap"] = j["windows"][w]["longest_gap"]["longest_ms"]
        slow, inside = stall_buckets(j)
        m["stall_buckets"] = slow
        m["buckets_inside"] = inside
    m["save_s"] = j["save"]["duration_ms"] / 1000.0
    if "blocking_save_ms" in j["save"]:
        m["blocking_save_s"] = j["save"]["blocking_save_ms"] / 1000.0
    mem = j["memory"]
    m["peak_minus_before"] = mem["peak_used_minus_before"] / MiB
    m["after_minus_before"] = (mem["after_last_used"] - mem["used_before"]) / MiB
    m["max_info_gap_during"] = mem.get("max_sample_interval_during_ms")
    for t in j.get("toucher_targets", []) or []:
        m["first:" + t["op"]] = t.get("first_started_after_ack_latency_ms")
    return m


def check(j):
    ok = j["save"].get("last_bgsave_status") == "ok"
    v = j.get("verify", {})
    ok_db = bool(v.get("dbsize_match"))
    ok_ll = v.get("llen_match", True) is not False
    return ok, ok_db and ok_ll


def med(runs, key):
    vals = [metrics(r).get(key) for r in runs]
    vals = [v for v in vals if v is not None]
    return statistics.median(vals) if vals else None


def f(v, nd=2):
    if v is None:
        return "–"
    if abs(v) >= 1000:
        return "{:,.0f}".format(v)
    return ("{:.%df}" % nd).format(v)


def pct(a, b):
    if a in (None, 0) or b is None:
        return "–"
    return "{:+.1f} %".format((b - a) / a * 100.0)


def name(k):
    return k[0] + (" (" + k[1] + ")" if k[1] else "")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", default=os.path.join(HERE, "..", "raw"))
    ap.add_argument("--new", default=HERE)
    a = ap.parse_args()
    old, new = load(a.old), load(a.new)
    keys = [k for k in ORDER if k in old or k in new]
    keys += sorted(k for k in set(old) | set(new) if k not in ORDER)

    out = []
    p = out.append
    p("# BGSAVE benchmark: before vs after the fix\n")
    p("old = `%s`, new = `%s`. Medians of each metric over the runs.\n"
      % (os.path.normpath(a.old), os.path.normpath(a.new)))

    p("## Checks\n")
    p("| config | round | runs | last_bgsave_status ok | reload check ok | files |")
    p("|---|---|---:|---:|---:|---|")
    tot = {"old": [0, 0, 0], "new": [0, 0, 0]}
    for k in keys:
        for rnd, g in (("old", old), ("new", new)):
            runs = g.get(k, [])
            if not runs:
                continue
            c = [check(r) for r in runs]
            s_ok = sum(x[0] for x in c)
            v_ok = sum(x[1] for x in c)
            tot[rnd][0] += len(runs); tot[rnd][1] += s_ok; tot[rnd][2] += v_ok
            p("| %s | %s | %d | %d | %d | %s |" % (name(k), rnd, len(runs), s_ok, v_ok,
                                                  ", ".join(r["_file"] for r in runs)))
    for rnd in ("old", "new"):
        p("\nTotal %s: %d runs, status ok %d, reload ok %d." % (rnd, *tot[rnd]))

    p("\n## Foreground traffic, baseline and during the save (old → new)\n")
    p("| config | window | ops/s | Δ | p50 ms | p99 ms | max ms | longest gap ms |")
    p("|---|---|---|---|---|---|---|---|")
    for k in keys:
        o, n = old.get(k, []), new.get(k, [])
        if not any("windows" in r for r in o + n):
            continue
        for w in ("baseline", "during"):
            row = [name(k), w]
            ao, an = med(o, w + ".ops"), med(n, w + ".ops")
            row.append("%s → %s" % (f(ao), f(an)))
            row.append(pct(ao, an))
            for x in ("p50", "p99", "max", "gap"):
                nd = 3 if x in ("p50", "p99") else 2
                row.append("%s → %s" % (f(med(o, w + "." + x), nd), f(med(n, w + "." + x), nd)))
            p("| " + " | ".join(row) + " |")

    p("\n## During vs baseline within each round (median ops/s change)\n")
    p("| config | old: during vs baseline | new: during vs baseline |")
    p("|---|---|---|")
    for k in keys:
        o, n = old.get(k, []), new.get(k, [])
        if not any("windows" in r for r in o + n):
            continue
        p("| %s | %s | %s |" % (name(k),
                                pct(med(o, "baseline.ops"), med(o, "during.ops")),
                                pct(med(n, "baseline.ops"), med(n, "during.ops"))))

    p("\n## Save duration and memory (old → new)\n")
    p("| config | save s | used_memory peak − before MiB | used_memory after − before MiB | 50 ms buckets in save with fg max > 10 ms | max INFO gap during ms |")
    p("|---|---|---|---|---|---|")
    for k in keys:
        o, n = old.get(k, []), new.get(k, [])
        save = "%s → %s" % (f(med(o, "save_s"), 3), f(med(n, "save_s"), 3))
        if k[0] == "idle":
            save += " (SAVE %s → %s)" % (f(med(o, "blocking_save_s"), 3), f(med(n, "blocking_save_s"), 3))
        sb = "–"
        if med(o, "stall_buckets") is not None or med(n, "stall_buckets") is not None:
            sb = "%s/%s → %s/%s" % (f(med(o, "stall_buckets"), 0), f(med(o, "buckets_inside"), 0),
                                    f(med(n, "stall_buckets"), 0), f(med(n, "buckets_inside"), 0))
        p("| %s | %s | %s → %s | %s → %s | %s | %s → %s |" % (
            name(k), save,
            f(med(o, "peak_minus_before"), 1), f(med(n, "peak_minus_before"), 1),
            f(med(o, "after_minus_before"), 1), f(med(n, "after_minus_before"), 1),
            sb, f(med(o, "max_info_gap_during"), 1), f(med(n, "max_info_gap_during"), 1)))

    p("\n## Per run (new round)\n")
    p("| file | base ops/s | during ops/s | during p99 | during max | during gap (starts at ms) | save s | peak−before MiB | after−before MiB | stall buckets | status | reload |")
    p("|---|---:|---:|---:|---:|---|---:|---:|---:|---:|---|---|")
    for k in keys:
        for r in new.get(k, []):
            m = metrics(r)
            s_ok, v_ok = check(r)
            gap = "–"
            if "windows" in r:
                lg = r["windows"]["during"]["longest_gap"]
                gap = "%s (%s)" % (f(lg["longest_ms"]), f(lg.get("starts_at_ms"), 1))
            p("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
                r["_file"], f(m.get("baseline.ops")), f(m.get("during.ops")),
                f(m.get("during.p99"), 3), f(m.get("during.max")), gap, f(m["save_s"], 3),
                f(m["peak_minus_before"], 1), f(m["after_minus_before"], 1),
                f(m.get("stall_buckets"), 0), "ok" if s_ok else "FAIL", "ok" if v_ok else "FAIL"))

    tk = [k for k in keys if any(any(x.startswith("first:") for x in metrics(r))
                                 for r in old.get(k, []) + new.get(k, []))]
    if tk:
        p("\n## Toucher: latency of the first command after the BGSAVE reply, per target (median ms, old → new)\n")
        for k in tk:
            ops = []
            for r in old.get(k, []) + new.get(k, []):
                for x in metrics(r):
                    if x.startswith("first:") and x not in ops:
                        ops.append(x)
            p("- %s: " % name(k) + "; ".join(
                "%s %s → %s" % (x[6:], f(med(old.get(k, []), x)), f(med(new.get(k, []), x))) for x in ops))

    sys.stdout.buffer.write(("\n".join(out) + "\n").encode("utf-8"))


if __name__ == "__main__":
    sys.exit(main())
