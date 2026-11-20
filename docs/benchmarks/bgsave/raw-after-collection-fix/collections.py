"""Detailed look at the collection runs (`coll` and `coll --no-toucher`), old vs new.

Reads coll-run*.json and coll-no-toucher-run*.json from the old directory
(default: ../raw-after-fix) and the new directory (default: this script's
directory) and prints markdown tables. Everything comes from the JSON files.

Usage (from the workspace root):
    python docs\\benchmarks\\bgsave\\raw-after-collection-fix\\collections.py
    python docs\\benchmarks\\bgsave\\raw-after-collection-fix\\collections.py --old <dir> --new <dir>

Definitions
  save ms            save.duration_ms (BGSAVE reply read -> first INFO reply with
                     rdb_bgsave_in_progress:0; ~10-12 ms resolution).
  gap / at           windows.during.longest_gap.longest_ms and .starts_at_ms
                     (ms after the BGSAVE reply). Only the single longest gap is
                     recorded in the files.
  buckets >X         number of 50 ms timeline buckets lying wholly inside
                     [0, save end] whose foreground_max_ms > X ms, over the
                     number of such buckets (same "inside" rule as compare.py).
                     Note the buckets are aligned to traffic start, not to 0.
  ops/s, p50, p99    windows.<baseline|during>.foreground.*; "d%" is
                     during vs baseline of the same run.
  peak-before MiB    memory.peak_used_minus_before / 2^20.
  toucher first      toucher_targets[].first_started_after_ack_latency_ms: the
                     latency of the toucher's first command to that collection
                     started after the BGSAVE reply; "done@" is
                     first_started_after_ack_completed_at_ms; "start@" is
                     done@ - latency.
  toucher later      for each target, max_during_ms (max over all its commands
                     completing inside the save) and count_during; plus the
                     largest timeline toucher_max_ms among buckets that start
                     after the last first-command completion and lie wholly
                     inside the save (i.e. commands after the first round).
  fg during waits    foreground requests per 50 ms bucket (foreground_count) for
                     buckets wholly inside the save, split by how much of the
                     bucket is covered by an outstanding toucher first command
                     whose latency > 2 ms: covered >= 50 % vs covered 0 %;
                     compared with the mean per-bucket count over the buckets
                     wholly inside the baseline window. Converted to ops/s
                     (count / 0.05 s).
"""
# This file is named collections.py, which shadows the standard library's
# `collections` module when run as a script (Python puts the script's own
# directory first on sys.path). Remove that entry before importing anything.
import sys
if __name__ == "__main__" and sys.path and not sys.flags.safe_path:
    del sys.path[0]

import argparse
import glob
import json
import os
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
MiB = float(1 << 20)
THRESH = (2.0, 5.0, 10.0, 50.0)


def load(d, pattern):
    runs = []
    for p in sorted(glob.glob(os.path.join(d, pattern))):
        with open(p, encoding="utf-8") as f:
            j = json.load(f)
        if j.get("tool") != "rr-bench":
            continue
        j["_file"] = os.path.basename(p)
        runs.append(j)
    runs.sort(key=lambda r: r["run"])
    return runs


def buckets(j):
    """Yield (lo, hi, fg_count, fg_max, t_count, t_max) for every bucket."""
    t = j["timeline"]
    b = t["bucket_ms"]
    s0 = t["first_bucket_starts_at_ms"]
    for i in range(len(t["foreground_count"])):
        lo = s0 + i * b
        yield (lo, lo + b, t["foreground_count"][i], t["foreground_max_ms"][i],
               t["toucher_count"][i], t["toucher_max_ms"][i])


def inside(j, lo, hi, win):
    w = j["windows"][win]
    return lo >= w["start_ms"] and hi <= w["end_ms"]


def waits(j, min_ms=2.0):
    out = []
    for t in j.get("toucher_targets") or []:
        lat = t.get("first_started_after_ack_latency_ms")
        done = t.get("first_started_after_ack_completed_at_ms")
        if lat is None or done is None:
            continue
        if lat > min_ms:
            out.append((done - lat, done, t["op"]))
    return out


def covered(lo, hi, ivs):
    # total length of [lo,hi] covered by the union of intervals
    segs = sorted((max(lo, a), min(hi, b)) for a, b, _ in ivs if b > lo and a < hi)
    tot, cur_a, cur_b = 0.0, None, None
    for a, b in segs:
        if cur_b is None or a > cur_b:
            if cur_b is not None:
                tot += cur_b - cur_a
            cur_a, cur_b = a, b
        else:
            cur_b = max(cur_b, b)
    if cur_b is not None:
        tot += cur_b - cur_a
    return tot / (hi - lo)


def f(v, nd=2):
    if v is None:
        return "–"
    if abs(v) >= 1000:
        return "{:,.0f}".format(v)
    return ("{:.%df}" % nd).format(v)


def per_run(j):
    d = j["windows"]["during"]
    bl = j["windows"]["baseline"]
    save = j["save"]["duration_ms"]
    over = {x: 0 for x in THRESH}
    n_in = 0
    for lo, hi, c, m, tc, tm in buckets(j):
        if lo >= 0 and hi <= save:
            n_in += 1
            for x in THRESH:
                if m is not None and m > x:
                    over[x] += 1
    return {
        "save": save,
        "gap": d["longest_gap"]["longest_ms"],
        "gap_at": d["longest_gap"].get("starts_at_ms"),
        "over": over, "n_in": n_in,
        "b_ops": bl["foreground"]["throughput_per_s"],
        "d_ops": d["foreground"]["throughput_per_s"],
        "b_p50": bl["foreground"]["p50_ms"], "d_p50": d["foreground"]["p50_ms"],
        "b_p99": bl["foreground"]["p99_ms"], "d_p99": d["foreground"]["p99_ms"],
        "peak": j["memory"]["peak_used_minus_before"] / MiB,
        "status": j["save"].get("last_bgsave_status"),
        "verify": bool(j.get("verify", {}).get("dbsize_match"))
                  and j.get("verify", {}).get("llen_match", True) is not False,
    }


def fg_table(p, title, rounds):
    p("\n### %s: foreground during the save, per run\n" % title)
    p("| round | file | save ms | longest gap ms (at ms) | buckets >2 | >5 | >10 | >50 "
      "| base ops/s | during ops/s | d% | p50 base→during | p99 base→during | peak−before MiB | status | reload |")
    p("|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---|---|---:|---|---|")
    for rnd, runs in rounds:
        for j in runs:
            r = per_run(j)
            n = r["n_in"]
            p("| %s | %s | %s | %s (%s) | %d/%d | %d/%d | %d/%d | %d/%d | %s | %s | %+.1f %% | %s→%s | %s→%s | %s | %s | %s |" % (
                rnd, j["_file"], f(r["save"], 1), f(r["gap"]), f(r["gap_at"], 1),
                r["over"][2.0], n, r["over"][5.0], n, r["over"][10.0], n, r["over"][50.0], n,
                f(r["b_ops"]), f(r["d_ops"]), (r["d_ops"] - r["b_ops"]) / r["b_ops"] * 100,
                f(r["b_p50"], 3), f(r["d_p50"], 3), f(r["b_p99"], 3), f(r["d_p99"], 3),
                f(r["peak"], 1), r["status"], "ok" if r["verify"] else "FAIL"))
    p("\nMedians:\n")
    p("| round | save ms | longest gap ms | buckets >2/>5/>10/>50 | base ops/s | during ops/s | d% | during p50 | during p99 | peak−before MiB |")
    p("|---|---:|---:|---|---:|---:|---:|---:|---:|---:|")
    for rnd, runs in rounds:
        if not runs:
            continue
        rs = [per_run(j) for j in runs]
        md = lambda k: statistics.median(r[k] for r in rs)
        mo = lambda x: statistics.median(r["over"][x] for r in rs)
        dp = statistics.median((r["d_ops"] - r["b_ops"]) / r["b_ops"] * 100 for r in rs)
        p("| %s | %s | %s | %s/%s/%s/%s of %s | %s | %s | %+.1f %% | %s | %s | %s |" % (
            rnd, f(md("save"), 1), f(md("gap")), f(mo(2.0), 0), f(mo(5.0), 0), f(mo(10.0), 0),
            f(mo(50.0), 0), f(md("n_in"), 0), f(md("b_ops")), f(md("d_ops")), dp,
            f(md("d_p50"), 3), f(md("d_p99"), 3), f(md("peak"), 1)))


def toucher_tables(p, rounds):
    p("\n### coll (toucher): the toucher's first command to each collection after BGSAVE, per run\n")
    p("| round | file | target | first latency ms | start@ ms | done@ ms | max during ms | count during |")
    p("|---|---|---|---:|---:|---:|---:|---:|")
    for rnd, runs in rounds:
        for j in runs:
            for t in j.get("toucher_targets") or []:
                lat = t.get("first_started_after_ack_latency_ms")
                done = t.get("first_started_after_ack_completed_at_ms")
                p("| %s | %s | %s | %s | %s | %s | %s | %s |" % (
                    rnd, j["_file"], t["op"], f(lat), f(None if done is None or lat is None else done - lat, 1),
                    f(done, 1), f(t.get("max_during_ms")), t.get("count_during")))
    p("\nSum of the four first-command latencies (time the toucher spent waiting on first writes), "
      "and the toucher's later commands:\n")
    p("| round | file | sum of firsts ms | last first done@ ms | save ms | toucher during p50 ms | "
      "max toucher latency in buckets after the first round (n buckets) | toucher baseline p50 / max ms |")
    p("|---|---|---:|---:|---:|---:|---|---|")
    for rnd, runs in rounds:
        for j in runs:
            tt = j.get("toucher_targets") or []
            s = sum(t["first_started_after_ack_latency_ms"] for t in tt
                    if t.get("first_started_after_ack_latency_ms") is not None)
            last = max((t["first_started_after_ack_completed_at_ms"] for t in tt
                        if t.get("first_started_after_ack_completed_at_ms") is not None), default=None)
            save = j["save"]["duration_ms"]
            later = [(tm, tc) for lo, hi, c, m, tc, tm in buckets(j)
                     if last is not None and lo >= last and hi <= save and tc]
            lm = max((x[0] for x in later), default=None)
            tb = j["windows"]["baseline"]["toucher"]
            p("| %s | %s | %s | %s | %s | %s | %s (%d) | %s / %s |" % (
                rnd, j["_file"], f(s), f(last, 1), f(save, 1),
                f(j["windows"]["during"]["toucher"]["p50_ms"], 3), f(lm), len(later),
                f(tb["p50_ms"], 3), f(tb["max_ms"])))


def wait_tables(p, rounds):
    p("\n### coll (toucher): foreground throughput in 50 ms buckets while a toucher first-write is outstanding\n")
    p("Buckets wholly inside the save; 'waiting' = >= 50 % of the bucket covered by an outstanding "
      "toucher first command with latency > 2 ms; 'not waiting' = 0 % covered. ops/s = count/0.05 s.\n")
    p("| round | file | baseline mean ops/s per bucket | waiting: n, mean ops/s (min) | not waiting: n, mean ops/s | waiting vs baseline | not waiting vs baseline |")
    p("|---|---|---:|---|---|---:|---:|")
    for rnd, runs in rounds:
        for j in runs:
            ivs = waits(j)
            save = j["save"]["duration_ms"]
            base = [c for lo, hi, c, m, tc, tm in buckets(j) if inside(j, lo, hi, "baseline")]
            bm = statistics.mean(base) / 0.05 if base else None
            w, nw = [], []
            for lo, hi, c, m, tc, tm in buckets(j):
                if lo >= 0 and hi <= save:
                    cv = covered(lo, hi, ivs)
                    if cv >= 0.5:
                        w.append(c / 0.05)
                    elif cv == 0:
                        nw.append(c / 0.05)
            wm = statistics.mean(w) if w else None
            nm = statistics.mean(nw) if nw else None
            p("| %s | %s | %s | %d, %s (%s) | %d, %s | %s | %s |" % (
                rnd, j["_file"], f(bm), len(w), f(wm), f(min(w) if w else None), len(nw), f(nm),
                "–" if wm is None or not bm else "%+.1f %%" % ((wm - bm) / bm * 100),
                "–" if nm is None or not bm else "%+.1f %%" % ((nm - bm) / bm * 100)))


def stretches(p, title, rounds, frac=0.9):
    """Contiguous runs of 50 ms buckets overlapping the save whose foreground
    throughput is below frac x the baseline mean per bucket."""
    p("\n### %s: stretches of depressed foreground throughput during the save\n" % title)
    p("A stretch = consecutive 50 ms buckets that overlap [0, save end] with foreground ops below "
      "%d %% of the run's baseline mean per bucket. 'max ms' = largest foreground_max_ms of its buckets. "
      "'est. mean ms' = 50 clients / ops/s (Little's law for 50 closed-loop clients; an estimate, "
      "since per-request latencies are not in the files, so p50/p99 per stretch cannot be computed). "
      "'overlaps' = toucher first commands (latency > 2 ms) whose wait overlaps the stretch.\n" % (frac * 100))
    p("| round | file | stretch ms | length ms | mean ops/s | min ops/s | max ms | est. mean ms | overlaps |")
    p("|---|---|---|---:|---:|---:|---:|---:|---|")
    rest_rows = []
    for rnd, runs in rounds:
        for j in runs:
            ivs = waits(j)
            save = j["save"]["duration_ms"]
            base = [c for lo, hi, c, m, tc, tm in buckets(j) if inside(j, lo, hi, "baseline")]
            bmean = statistics.mean(base)
            sel = [(lo, hi, c, m) for lo, hi, c, m, tc, tm in buckets(j) if hi > 0 and lo < save]
            groups, cur = [], []
            for b in sel:
                if b[2] < frac * bmean:
                    cur.append(b)
                else:
                    if cur:
                        groups.append(cur)
                    cur = []
            if cur:
                groups.append(cur)
            in_str = set()
            for g in groups:
                lo, hi = g[0][0], g[-1][1]
                ops = [b[2] / 0.05 for b in g]
                mx = max(b[3] for b in g if b[3] is not None) if any(b[3] for b in g) else None
                mo = statistics.mean(ops)
                ov = [op for a, b, op in ivs if b > lo and a < hi]
                for b in g:
                    in_str.add(b[0])
                p("| %s | %s | %.0f–%.0f | %.0f | %s | %s | %s | %s | %s |" % (
                    rnd, j["_file"], lo, hi, hi - lo, f(mo), f(min(ops)), f(mx),
                    f(50.0 / mo * 1000 if mo else None, 3), ", ".join(ov) or "–"))
            rest = [b for b in sel if b[0] not in in_str and b[0] >= 0 and b[1] <= save]
            rest_rows.append((rnd, j["_file"], len(rest),
                              statistics.mean(b[2] / 0.05 for b in rest) if rest else None,
                              max((b[3] for b in rest if b[3] is not None), default=None),
                              bmean / 0.05))
    p("\nRest of the save (buckets wholly inside the save and not in any stretch):\n")
    p("| round | file | n buckets | mean ops/s | max ms | baseline mean ops/s |")
    p("|---|---|---:|---:|---:|---:|")
    for r in rest_rows:
        p("| %s | %s | %d | %s | %s | %s |" % (r[0], r[1], r[2], f(r[3]), f(r[4]), f(r[5])))


def timeline_dump(p, title, rounds):
    p("\n### %s: 50 ms timeline from 100 ms before BGSAVE to 100 ms after the save end\n" % title)
    p("Each cell: foreground count / foreground max ms [/ toucher count / toucher max ms]. '*' = bucket wholly inside the save.\n")
    for rnd, runs in rounds:
        for j in runs:
            save = j["save"]["duration_ms"]
            cells = []
            for lo, hi, c, m, tc, tm in buckets(j):
                if hi < -100 or lo > save + 100:
                    continue
                star = "*" if lo >= 0 and hi <= save else ""
                s = "%s%.0f: %d/%s" % (star, lo, c, f(m))
                if not j["params"].get("no_toucher"):
                    s += "/%d/%s" % (tc, f(tm))
                cells.append(s)
            p("- %s %s (save %.1f ms): " % (rnd, j["_file"], save) + "; ".join(cells))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", default=os.path.join(HERE, "..", "raw-after-fix"))
    ap.add_argument("--new", default=HERE)
    ap.add_argument("--timeline", action="store_true", help="also print the raw 50 ms timelines")
    a = ap.parse_args()
    out = []
    p = out.append
    p("# Collections: old = `%s`, new = `%s`\n" % (os.path.normpath(a.old), os.path.normpath(a.new)))
    for pat, title in (("coll-run*.json", "coll (toucher)"), ("coll-no-toucher-run*.json", "coll --no-toucher")):
        rounds = [("old", load(a.old, pat)), ("new", load(a.new, pat))]
        fg_table(p, title, rounds)
        if pat.startswith("coll-run"):
            toucher_tables(p, rounds)
            wait_tables(p, rounds)
        stretches(p, title, rounds)
        if a.timeline:
            timeline_dump(p, title, rounds)
    sys.stdout.buffer.write(("\n".join(out) + "\n").encode("utf-8"))


if __name__ == "__main__":
    sys.exit(main())
