//! `rexyra-bench summarize`: Markdown tables (every run plus the median across
//! runs) from the per-run JSON files alone.

use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::PathBuf;

const MIB: f64 = 1_048_576.0;

fn get(v: &Value, pointer: &str) -> Option<f64> {
    v.pointer(pointer).and_then(Value::as_f64)
}

fn median(mut xs: Vec<f64>) -> Option<f64> {
    xs.retain(|x| x.is_finite());
    if xs.is_empty() {
        return None;
    }
    xs.sort_by(f64::total_cmp);
    let n = xs.len();
    Some(if n % 2 == 1 {
        xs[n / 2]
    } else {
        (xs[n / 2 - 1] + xs[n / 2]) / 2.0
    })
}

struct Col {
    head: &'static str,
    pointer: String,
    scale: f64,
    decimals: usize,
}

fn col(head: &'static str, pointer: impl Into<String>, scale: f64, decimals: usize) -> Col {
    Col {
        head,
        pointer: pointer.into(),
        scale,
        decimals,
    }
}

fn cell(x: Option<f64>, c: &Col) -> String {
    x.map_or_else(
        || "-".to_owned(),
        |x| format!("{:.*}", c.decimals, x / c.scale),
    )
}

/// One table: a row per run (and per window when `windows` is non-empty,
/// substituting `{w}` in the pointers), then median rows.
fn table(out: &mut String, runs: &[&Value], windows: &[&str], cols: &[Col]) {
    let mut head = String::from("| run |");
    let mut rule = String::from("|---|");
    if !windows.is_empty() {
        head.push_str(" window |");
        rule.push_str("---|");
    }
    for c in cols {
        let _ = write!(head, " {} |", c.head);
        rule.push_str("---:|");
    }
    let _ = writeln!(out, "{head}\n{rule}");
    let ws: Vec<&str> = if windows.is_empty() {
        vec![""]
    } else {
        windows.to_vec()
    };
    for r in runs {
        for w in &ws {
            let mut line = format!("| {} |", get(r, "/run").unwrap_or(0.0));
            if !w.is_empty() {
                let _ = write!(line, " {w} |");
            }
            for c in cols {
                let _ = write!(line, " {} |", cell(get(r, &c.pointer.replace("{w}", w)), c));
            }
            let _ = writeln!(out, "{line}");
        }
    }
    for w in &ws {
        let mut line = String::from("| **median** |");
        if !w.is_empty() {
            let _ = write!(line, " {w} |");
        }
        for c in cols {
            let pointer = c.pointer.replace("{w}", w);
            let m = median(runs.iter().filter_map(|r| get(r, &pointer)).collect());
            let _ = write!(line, " {} |", cell(m, c));
        }
        let _ = writeln!(out, "{line}");
    }
    out.push('\n');
}

fn bools(runs: &[&Value], pointer: &str) -> String {
    runs.iter()
        .map(|r| match r.pointer(pointer) {
            Some(Value::Bool(true)) => "yes",
            Some(Value::Bool(false)) => "NO",
            _ => "-",
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn texts(runs: &[&Value], pointer: &str) -> String {
    runs.iter()
        .map(|r| {
            r.pointer(pointer)
                .and_then(Value::as_str)
                .unwrap_or("-")
                .to_owned()
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn render(out: &mut String, title: &str, runs: &[&Value]) {
    let first = runs[0];
    let _ = writeln!(out, "### {title}\n");
    if let Some(about) = first.pointer("/spec/about").and_then(Value::as_str) {
        let _ = writeln!(out, "{about}.");
    }
    let _ = writeln!(
        out,
        "rdb-key-save-delay: {} µs. Runs: {}. Checks per run — rdb_last_bgsave_status: {}; DBSIZE after reload equals DBSIZE at BGSAVE: {}.\n",
        get(first, "/params/save_delay_us").unwrap_or(0.0),
        runs.len(),
        texts(runs, "/save/last_bgsave_status"),
        bools(runs, "/verify/dbsize_match"),
    );
    let idle = first.pointer("/spec/kind").and_then(Value::as_str) == Some("Idle");
    if idle {
        table(
            out,
            runs,
            &[],
            &[
                col("BGSAVE s", "/save/duration_ms", 1000.0, 3),
                col("BGSAVE file MiB", "/save/file_bytes", MIB, 1),
                col("SAVE s", "/save/blocking_save_ms", 1000.0, 3),
                col("SAVE file MiB", "/save/blocking_save_file_bytes", MIB, 1),
                col("used before MiB", "/memory/used_before", MIB, 1),
                col("peak used during MiB", "/memory/peak_used_during", MIB, 1),
                col(
                    "peak − before MiB",
                    "/memory/peak_used_minus_before",
                    MIB,
                    1,
                ),
                col("RSS peak MiB", "/memory/peak_rss_during", MIB, 1),
                col(
                    "used at end of after-window MiB",
                    "/memory/after_last_used",
                    MIB,
                    1,
                ),
                col(
                    "max INFO gap ms",
                    "/memory/max_sample_interval_during_ms",
                    1.0,
                    1,
                ),
                col("reload s", "/verify/reload_ready_seconds", 1.0, 2),
            ],
        );
        return;
    }
    let _ = writeln!(out, "Foreground latency (ms), all GET+SET together:\n");
    let w = "/windows/{w}/foreground";
    table(
        out,
        runs,
        &["baseline", "during", "after"],
        &[
            col("secs", "/windows/{w}/seconds", 1.0, 2),
            col("requests", format!("{w}/count"), 1.0, 0),
            col("ops/s", format!("{w}/throughput_per_s"), 1.0, 0),
            col("p50", format!("{w}/p50_ms"), 1.0, 3),
            col("p95", format!("{w}/p95_ms"), 1.0, 3),
            col("p99", format!("{w}/p99_ms"), 1.0, 3),
            col("p99.9", format!("{w}/p999_ms"), 1.0, 3),
            col("max", format!("{w}/max_ms"), 1.0, 2),
            col("longest gap", "/windows/{w}/longest_gap/longest_ms", 1.0, 2),
        ],
    );
    let _ = writeln!(out, "Save and memory (MiB = 2^20 bytes):\n");
    table(
        out,
        runs,
        &[],
        &[
            col("save s", "/save/duration_ms", 1000.0, 3),
            col("file MiB", "/save/file_bytes", MIB, 1),
            col("used before", "/memory/used_before", MIB, 1),
            col("baseline max used", "/memory/baseline_max_used", MIB, 1),
            col("peak used during", "/memory/peak_used_during", MIB, 1),
            col("peak − before", "/memory/peak_used_minus_before", MIB, 1),
            col(
                "peak − baseline max",
                "/memory/peak_used_minus_baseline_max",
                MIB,
                1,
            ),
            col("RSS before", "/memory/rss_before", MIB, 1),
            col("RSS peak during", "/memory/peak_rss_during", MIB, 1),
            col("used after (last)", "/memory/after_last_used", MIB, 1),
            col(
                "max INFO gap ms",
                "/memory/max_sample_interval_during_ms",
                1.0,
                1,
            ),
        ],
    );
    let has_toucher = first
        .pointer("/toucher_ops")
        .and_then(Value::as_array)
        .is_some_and(|a| !a.is_empty());
    if has_toucher {
        let _ = writeln!(out, "Toucher latency (ms):\n");
        let t = "/windows/{w}/toucher";
        table(
            out,
            runs,
            &["baseline", "during", "after"],
            &[
                col("ops", format!("{t}/count"), 1.0, 0),
                col("p50", format!("{t}/p50_ms"), 1.0, 3),
                col("p99", format!("{t}/p99_ms"), 1.0, 3),
                col("max", format!("{t}/max_ms"), 1.0, 2),
            ],
        );
    }
    let targets = first
        .pointer("/toucher_targets")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    if targets > 0 {
        let _ = writeln!(out, "Per collection: latency of the toucher's first command started after the BGSAVE reply, and its max during the save (ms):\n");
        let mut head = String::from("| run |");
        let mut rule = String::from("|---|");
        for i in 0..targets {
            let name = first
                .pointer(&format!("/toucher_targets/{i}/op"))
                .and_then(Value::as_str)
                .unwrap_or("?");
            let _ = write!(head, " {name} first | {name} max |");
            rule.push_str("---:|---:|");
        }
        let _ = writeln!(out, "{head}\n{rule}");
        let row = |label: String, f: &dyn Fn(&str) -> Option<f64>| {
            let mut line = format!("| {label} |");
            for i in 0..targets {
                for field in ["first_started_after_ack_latency_ms", "max_during_ms"] {
                    let x = f(&format!("/toucher_targets/{i}/{field}"));
                    let _ = write!(
                        line,
                        " {} |",
                        x.map_or("-".to_owned(), |x| format!("{x:.2}"))
                    );
                }
            }
            line
        };
        for r in runs {
            let _ = writeln!(
                out,
                "{}",
                row(format!("{}", get(r, "/run").unwrap_or(0.0)), &|p| get(r, p))
            );
        }
        let _ = writeln!(
            out,
            "{}\n",
            row("**median**".to_owned(), &|p| median(
                runs.iter().filter_map(|r| get(r, p)).collect()
            ))
        );
    }
}

pub fn main(args: &[String]) -> io::Result<()> {
    let mut raw = PathBuf::from("docs/benchmarks/bgsave/raw");
    let mut out_path = None;
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .ok_or_else(|| io::Error::other(format!("{flag} needs a value")))?;
        match flag.as_str() {
            "--raw" => raw = PathBuf::from(value),
            "--out" => out_path = Some(PathBuf::from(value)),
            _ => return Err(io::Error::other(format!("unknown option {flag}"))),
        }
    }
    let order: Vec<&str> = crate::scenario::all().iter().map(|s| s.name).collect();
    let mut groups: BTreeMap<(usize, String), Vec<Value>> = BTreeMap::new();
    let mut paths: Vec<PathBuf> = fs::read_dir(&raw)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect();
    paths.sort();
    for path in paths {
        let v: Value = serde_json::from_slice(&fs::read(&path)?)
            .map_err(|e| io::Error::other(format!("{}: {e}", path.display())))?;
        let scenario = v
            .pointer("/scenario")
            .and_then(Value::as_str)
            .unwrap_or("?")
            .to_owned();
        let label = v
            .pointer("/label")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let index = order
            .iter()
            .position(|n| *n == scenario)
            .unwrap_or(usize::MAX);
        let title = if label.is_empty() {
            scenario
        } else {
            format!("{scenario} ({label})")
        };
        groups.entry((index, title)).or_default().push(v);
    }
    let mut out = String::new();
    for ((_, title), mut runs) in groups {
        runs.sort_by(|a, b| {
            get(a, "/run")
                .partial_cmp(&get(b, "/run"))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let refs: Vec<&Value> = runs.iter().collect();
        render(&mut out, &title, &refs);
    }
    match out_path {
        Some(p) => fs::write(p, out),
        None => {
            print!("{out}");
            Ok(())
        }
    }
}
