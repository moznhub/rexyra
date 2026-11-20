//! rexyra-soak: runs a mixed workload against rexyra-server for a long time
//! and checks every reply against an exact model of the data.
//!
//! Each worker connection owns its own keys (strings, counters, hashes,
//! lists, sets, sorted sets, keys with a time to live) and sends a random mix
//! of reads, writes, transactions and Lua scripts, some workers one command
//! at a time and some in pipelines. Meanwhile one connection runs BGSAVE and
//! BGREWRITEAOF on a timer, one opens and closes connections, and a reporter
//! records throughput, latency and memory at a fixed interval. At the end
//! every key is read back and compared; if this tool started the server it
//! then restarts it on the same files and compares everything again.

mod model;
#[allow(dead_code)] // shared with rexyra-bench
mod resp;

use model::{Model, Op};
use resp::{encode, info_field, Conn, Reply};
use serde_json::{json, Value};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const USAGE: &str = "\
usage: rexyra-soak [--duration 1h] [--port 6400] [--server target/release/rexyra-server.exe]
                   [--external] [--password-env VAR] [--out target/soak/<time>]
                   [--clients 40] [--pipelined 10] [--depth 16] [--keys 1000]
                   [--report-secs 10] [--bgsave-secs 120] [--rewrite-secs 300]
                   [--churn-per-sec 20] [--appendonly yes] [--seed N]

Starts the server on --port with a fresh data folder under --out, unless
--external is given, in which case it uses a server already running there
(and skips the restart check). Durations take s, m or h.";

struct Options {
    duration: Duration,
    port: u16,
    server: PathBuf,
    external: bool,
    password: Option<String>,
    out: PathBuf,
    clients: usize,
    pipelined: usize,
    depth: usize,
    keys: usize,
    report: Duration,
    bgsave: Duration,
    rewrite: Duration,
    churn_per_sec: u64,
    appendonly: bool,
    seed: u64,
}

fn other(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

fn duration(text: &str) -> io::Result<Duration> {
    let (n, unit) = text.split_at(text.len().saturating_sub(1));
    let n: u64 = n
        .parse()
        .map_err(|_| other(format!("bad duration {text:?}")))?;
    Ok(Duration::from_secs(match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        _ => return Err(other(format!("bad duration {text:?}: use s, m or h"))),
    }))
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

impl Options {
    fn parse(argv: &[String]) -> io::Result<Self> {
        let mut o = Options {
            duration: Duration::from_secs(3600),
            port: 6400,
            server: PathBuf::from("target/release/rexyra-server.exe"),
            external: false,
            password: None,
            out: PathBuf::from(format!("target/soak/{}", unix_now())),
            clients: 40,
            pipelined: 10,
            depth: 16,
            keys: 1000,
            report: Duration::from_secs(10),
            bgsave: Duration::from_secs(120),
            rewrite: Duration::from_secs(300),
            churn_per_sec: 20,
            appendonly: true,
            seed: unix_now(),
        };
        let mut it = argv.iter();
        while let Some(flag) = it.next() {
            if flag == "--external" {
                o.external = true;
                continue;
            }
            let value = it
                .next()
                .ok_or_else(|| other(format!("{flag} needs a value")))?;
            let n = |v: &str| -> io::Result<u64> {
                v.parse()
                    .map_err(|_| other(format!("{flag}: bad number {v:?}")))
            };
            match flag.as_str() {
                "--duration" => o.duration = duration(value)?,
                "--port" => o.port = n(value)? as u16,
                "--server" => o.server = PathBuf::from(value),
                "--password-env" => {
                    o.password =
                        Some(std::env::var(value).map_err(|_| {
                            other(format!("environment variable {value} is not set"))
                        })?)
                }
                "--out" => o.out = PathBuf::from(value),
                "--clients" => o.clients = n(value)? as usize,
                "--pipelined" => o.pipelined = n(value)? as usize,
                "--depth" => o.depth = n(value)?.max(1) as usize,
                "--keys" => o.keys = n(value)?.max(1) as usize,
                "--report-secs" => o.report = Duration::from_secs(n(value)?.max(1)),
                "--bgsave-secs" => o.bgsave = Duration::from_secs(n(value)?),
                "--rewrite-secs" => o.rewrite = Duration::from_secs(n(value)?),
                "--churn-per-sec" => o.churn_per_sec = n(value)?,
                "--appendonly" => o.appendonly = value == "yes",
                "--seed" => o.seed = n(value)?,
                _ => return Err(other(format!("unknown option {flag}\n{USAGE}"))),
            }
        }
        if o.pipelined > o.clients {
            return Err(other("--pipelined cannot be more than --clients"));
        }
        Ok(o)
    }
}

// ---------------------------------------------------------------- server --

struct Server {
    child: Option<Child>,
    port: u16,
}

impl Server {
    fn start(o: &Options, dir: &Path, log: &Path) -> io::Result<Self> {
        if Conn::connect(o.port).is_ok() {
            return Err(other(format!(
                "something already listens on 127.0.0.1:{}; pick another --port",
                o.port
            )));
        }
        let mut cmd = Command::new(&o.server);
        cmd.arg("--port")
            .arg(o.port.to_string())
            .arg("--dir")
            .arg(dir)
            .arg("--save")
            .arg("")
            .arg("--appendonly")
            .arg(if o.appendonly { "yes" } else { "no" })
            .arg("--logfile")
            .arg(log)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut server = Server {
            child: Some(cmd.spawn()?),
            port: o.port,
        };
        server.wait_ready(Duration::from_secs(600))?;
        Ok(server)
    }

    fn pid(&self) -> u32 {
        self.child.as_ref().map_or(0, Child::id)
    }

    fn exited(&mut self) -> Option<String> {
        let child = self.child.as_mut()?;
        match child.try_wait() {
            Ok(Some(status)) => Some(status.to_string()),
            _ => None,
        }
    }

    fn wait_ready(&mut self, timeout: Duration) -> io::Result<()> {
        let started = Instant::now();
        loop {
            if let Some(status) = self.exited() {
                return Err(other(format!(
                    "the server exited during start-up ({status})"
                )));
            }
            if let Ok(mut c) = Conn::connect(self.port) {
                if matches!(c.call(&[b"PING"]), Ok(Reply::Simple(s)) if s == "PONG") {
                    return Ok(());
                }
            }
            if started.elapsed() > timeout {
                return Err(other("the server did not become ready in time"));
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    /// `SHUTDOWN` (saving) or `SHUTDOWN NOSAVE`, then waits for the exit.
    fn stop(mut self, save: bool) -> io::Result<()> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        if let Ok(mut c) = Conn::connect(self.port) {
            c.set_timeout(Some(Duration::from_secs(600)))?;
            let _ = if save {
                c.call(&[b"SHUTDOWN"])
            } else {
                c.call(&[b"SHUTDOWN", b"NOSAVE"])
            };
        }
        let started = Instant::now();
        while child.try_wait()?.is_none() {
            if started.elapsed() > Duration::from_secs(600) {
                child.kill()?;
                child.wait()?;
                return Err(other("the server did not exit after SHUTDOWN; killed it"));
            }
            thread::sleep(Duration::from_millis(50));
        }
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn connect(o: &Options) -> io::Result<Conn> {
    let mut c = Conn::connect(o.port)?;
    c.set_timeout(Some(Duration::from_secs(60)))?;
    if let Some(pw) = &o.password {
        let r = c.call(&[b"AUTH", pw.as_bytes()])?;
        if r.is_error() {
            return Err(other(format!("AUTH failed: {}", r.short())));
        }
    }
    Ok(c)
}

// ----------------------------------------------------------------- shared --

#[derive(Default)]
struct Shared {
    stop: AtomicBool,
    ops: AtomicU64,
    commands: AtomicU64,
    mismatches: AtomicU64,
    conn_errors: AtomicU64,
    bgsaves: AtomicU64,
    rewrites: AtomicU64,
    admin_errors: AtomicU64,
    churned: AtomicU64,
    churn_errors: AtomicU64,
    /// Latency of single operations, microseconds, since the last report.
    single_us: Mutex<Vec<u32>>,
    /// Latency of whole pipelines, microseconds, since the last report.
    batch_us: Mutex<Vec<u32>>,
    log: Mutex<Option<File>>,
}

impl Shared {
    fn note(&self, record: Value) {
        if let Ok(mut log) = self.log.lock() {
            if let Some(f) = log.as_mut() {
                let _ = writeln!(f, "{record}");
            }
        }
    }
}

fn show(args: &[Vec<u8>]) -> String {
    let words: Vec<String> = args
        .iter()
        .map(|a| {
            let s = String::from_utf8_lossy(a);
            if s.len() > 40 {
                format!("{}...", &s[..40])
            } else {
                s.into_owned()
            }
        })
        .collect();
    words.join(" ")
}

// ---------------------------------------------------------------- workers --

fn send_ops(conn: &mut Conn, ops: &[Op]) -> io::Result<Vec<Vec<Reply>>> {
    let mut buf = Vec::with_capacity(4096);
    for op in ops {
        for cmd in &op.cmds {
            let a: Vec<&[u8]> = cmd.args.iter().map(Vec::as_slice).collect();
            encode(&mut buf, &a);
        }
    }
    conn.send(&buf)?;
    let mut out = Vec::with_capacity(ops.len());
    for op in ops {
        let mut replies = Vec::with_capacity(op.cmds.len());
        for _ in &op.cmds {
            replies.push(conn.read()?);
        }
        out.push(replies);
    }
    Ok(out)
}

fn reset_keys(o: &Options, model: &mut Model, keys: &[Vec<u8>]) -> io::Result<Conn> {
    let mut conn = connect(o)?;
    for key in keys {
        model.forget(key);
        conn.call(&[b"DEL", key])?;
    }
    Ok(conn)
}

fn worker(o: Arc<Options>, shared: Arc<Shared>, id: usize, depth: usize) -> io::Result<Model> {
    let mut model = Model::new(id, o.keys, o.seed);
    let mut conn = connect(&o)?;
    // Start from nothing, whatever an earlier run left.
    let mut buf = Vec::new();
    let mut n = 0;
    for kind in model::KINDS.iter().chain(["v"].iter()) {
        for i in 0..o.keys {
            encode(&mut buf, &[b"DEL", &model.key(kind, i)]);
            n += 1;
        }
    }
    conn.send(&buf)?;
    for _ in 0..n {
        conn.read()?;
    }

    while !shared.stop.load(Relaxed) {
        let ops: Vec<Op> = (0..depth).map(|_| model.next_op()).collect();
        let started = Instant::now();
        match send_ops(&mut conn, &ops) {
            Ok(replies) => {
                let us = started.elapsed().as_micros().min(u32::MAX as u128) as u32;
                let target = if depth == 1 {
                    &shared.single_us
                } else {
                    &shared.batch_us
                };
                if let Ok(mut v) = target.lock() {
                    v.push(us);
                }
                shared.ops.fetch_add(ops.len() as u64, Relaxed);
                let mut bad = Vec::new();
                for (op, got) in ops.iter().zip(&replies) {
                    shared.commands.fetch_add(op.cmds.len() as u64, Relaxed);
                    for (cmd, reply) in op.cmds.iter().zip(got) {
                        if !cmd.expect.matches(reply) {
                            let count = shared.mismatches.fetch_add(1, Relaxed);
                            if count < 200 {
                                shared.note(json!({
                                    "event": "mismatch", "at": unix_now(), "worker": id,
                                    "command": show(&cmd.args),
                                    "expected": format!("{:?}", cmd.expect).chars().take(300).collect::<String>(),
                                    "got": reply.short(),
                                }));
                            }
                            bad.push(op.key.clone());
                            break;
                        }
                    }
                }
                if !bad.is_empty() {
                    conn = reset_keys(&o, &mut model, &bad)?;
                }
            }
            Err(e) => {
                if shared.stop.load(Relaxed) {
                    break;
                }
                shared.conn_errors.fetch_add(1, Relaxed);
                shared.note(json!({"event": "connection-error", "at": unix_now(), "worker": id, "error": e.to_string()}));
                // Whether the in-flight commands ran is unknown: reset their keys.
                let keys: Vec<Vec<u8>> = ops.iter().map(|op| op.key.clone()).collect();
                let mut attempt = 0;
                conn = loop {
                    thread::sleep(Duration::from_secs(1));
                    match reset_keys(&o, &mut model, &keys) {
                        Ok(c) => break c,
                        Err(_) if attempt < 60 && !shared.stop.load(Relaxed) => attempt += 1,
                        Err(e) => return Err(e),
                    }
                };
            }
        }
    }
    Ok(model)
}

// ----------------------------------------------------- admin, churn, report --

fn info_all(conn: &mut Conn) -> io::Result<String> {
    conn.call(&[b"INFO"])?.text()
}

fn admin(o: Arc<Options>, shared: Arc<Shared>) {
    let start = Instant::now();
    let (mut next_save, mut next_rewrite) = (o.bgsave, o.rewrite);
    let mut conn = None;
    while !shared.stop.load(Relaxed) {
        thread::sleep(Duration::from_millis(200));
        let t = start.elapsed();
        let due_save = !o.bgsave.is_zero() && t >= next_save;
        let due_rewrite = o.appendonly && !o.rewrite.is_zero() && t >= next_rewrite;
        if !due_save && !due_rewrite {
            continue;
        }
        if conn.is_none() {
            conn = connect(&o).ok();
        }
        let Some(c) = conn.as_mut() else { continue };
        let (cmd, counter): (&[u8], &AtomicU64) = if due_save {
            next_save += o.bgsave;
            (b"BGSAVE", &shared.bgsaves)
        } else {
            next_rewrite += o.rewrite;
            (b"BGREWRITEAOF", &shared.rewrites)
        };
        match c.call(&[cmd]) {
            Ok(Reply::Simple(_)) => {
                counter.fetch_add(1, Relaxed);
            }
            Ok(Reply::Error(e)) if e.contains("in progress") || e.contains("scheduled") => {
                counter.fetch_add(1, Relaxed);
            }
            Ok(r) => {
                shared.admin_errors.fetch_add(1, Relaxed);
                shared.note(json!({"event": "admin-error", "at": unix_now(), "command": String::from_utf8_lossy(cmd), "reply": r.short()}));
            }
            Err(e) => {
                shared.admin_errors.fetch_add(1, Relaxed);
                shared.note(json!({"event": "admin-error", "at": unix_now(), "command": String::from_utf8_lossy(cmd), "error": e.to_string()}));
                conn = None;
            }
        }
    }
}

fn churn(o: Arc<Options>, shared: Arc<Shared>) {
    if o.churn_per_sec == 0 {
        return;
    }
    let gap = Duration::from_micros(1_000_000 / o.churn_per_sec);
    while !shared.stop.load(Relaxed) {
        let t = Instant::now();
        let result = connect(&o).and_then(|mut c| {
            let r = c.call(&[b"CLIENT", b"SETNAME", b"soak-churn"])?;
            let p = c.call(&[b"PING"])?;
            if r == Reply::Simple("OK".into()) && p == Reply::Simple("PONG".into()) {
                Ok(())
            } else {
                Err(other(format!("unexpected {} / {}", r.short(), p.short())))
            }
        });
        match result {
            Ok(()) => shared.churned.fetch_add(1, Relaxed),
            Err(e) => {
                if shared.churn_errors.fetch_add(1, Relaxed) < 50 {
                    shared.note(
                        json!({"event": "churn-error", "at": unix_now(), "error": e.to_string()}),
                    );
                }
                0
            }
        };
        if let Some(rest) = gap.checked_sub(t.elapsed()) {
            thread::sleep(rest);
        }
    }
}

fn percentiles(mut v: Vec<u32>) -> Value {
    if v.is_empty() {
        return json!(null);
    }
    v.sort_unstable();
    let at =
        |p: f64| v[((p * v.len() as f64).ceil() as usize).clamp(1, v.len()) - 1] as f64 / 1000.0;
    json!({"count": v.len(), "p50_ms": at(0.50), "p99_ms": at(0.99), "p999_ms": at(0.999), "max_ms": at(1.0)})
}

fn num_field(info: &str, key: &str) -> Option<u64> {
    info_field(info, key).and_then(|v| v.parse().ok())
}

/// One report line every interval; returns the records for the summary.
fn reporter(o: Arc<Options>, shared: Arc<Shared>, report_path: PathBuf) -> Vec<Value> {
    let mut records = Vec::new();
    let mut file = File::create(report_path).ok();
    let start = Instant::now();
    let mut last = Instant::now();
    let (mut last_ops, mut last_cmds) = (0, 0);
    let mut conn: Option<Conn> = None;
    while !shared.stop.load(Relaxed) {
        let next = last + o.report;
        while Instant::now() < next && !shared.stop.load(Relaxed) {
            thread::sleep(Duration::from_millis(100));
        }
        let secs = last.elapsed().as_secs_f64();
        last = Instant::now();
        let (ops, cmds) = (shared.ops.load(Relaxed), shared.commands.load(Relaxed));
        let single =
            std::mem::take(&mut *shared.single_us.lock().unwrap_or_else(|e| e.into_inner()));
        let batch = std::mem::take(&mut *shared.batch_us.lock().unwrap_or_else(|e| e.into_inner()));
        if conn.is_none() {
            conn = connect(&o).ok();
        }
        let info = conn.as_mut().and_then(|c| info_all(c).ok());
        if info.is_none() {
            conn = None;
        }
        let info = info.unwrap_or_default();
        let record = json!({
            "t_s": start.elapsed().as_secs(),
            "ops_per_s": ((ops - last_ops) as f64 / secs).round(),
            "commands_per_s": ((cmds - last_cmds) as f64 / secs).round(),
            "single": percentiles(single),
            "pipeline": percentiles(batch),
            "used_memory": num_field(&info, "used_memory"),
            "used_memory_rss": num_field(&info, "used_memory_rss"),
            "connected_clients": num_field(&info, "connected_clients"),
            "keys": info_field(&info, "db0").map(str::to_owned),
            "expired_keys": num_field(&info, "expired_keys"),
            "bgsave_in_progress": num_field(&info, "rdb_bgsave_in_progress"),
            "aof_rewrite_in_progress": num_field(&info, "aof_rewrite_in_progress"),
            "mismatches": shared.mismatches.load(Relaxed),
            "conn_errors": shared.conn_errors.load(Relaxed),
            "info_ok": !info.is_empty(),
        });
        (last_ops, last_cmds) = (ops, cmds);
        println!(
            "[{:>6}s] {:>8} ops/s  p99 {:>7} ms  p99.9 {:>7} ms  max {:>7} ms  mem {:>6} MiB  rss {:>6} MiB  mismatches {}  conn-errors {}",
            record["t_s"],
            record["ops_per_s"],
            record["single"]["p99_ms"],
            record["single"]["p999_ms"],
            record["single"]["max_ms"],
            record["used_memory"].as_u64().map_or(0, |b| b >> 20),
            record["used_memory_rss"].as_u64().map_or(0, |b| b >> 20),
            record["mismatches"],
            record["conn_errors"],
        );
        if let Some(f) = file.as_mut() {
            let _ = writeln!(f, "{record}");
        }
        records.push(record);
    }
    records
}

// ----------------------------------------------------------------- verify --

/// Reads every key of every model back and compares; returns the number of
/// keys that differ.
fn verify(o: &Options, models: &[Model], shared: &Shared, phase: &str) -> io::Result<(u64, u64)> {
    let mut conn = connect(o)?;
    conn.set_timeout(Some(Duration::from_secs(300)))?;
    let (mut checked, mut wrong) = (0u64, 0u64);
    for model in models {
        let checks = model.checks();
        for chunk in checks.chunks(256) {
            let mut buf = Vec::new();
            for (_, cmd) in chunk {
                let a: Vec<&[u8]> = cmd.args.iter().map(Vec::as_slice).collect();
                encode(&mut buf, &a);
            }
            conn.send(&buf)?;
            for (key, cmd) in chunk {
                let got = conn.read()?;
                checked += 1;
                if !model.check_reply(key, cmd, &got) {
                    wrong += 1;
                    if wrong <= 50 {
                        shared.note(json!({
                            "event": "verify-mismatch", "phase": phase,
                            "key": String::from_utf8_lossy(key), "command": show(&cmd.args),
                            "got": got.short(),
                        }));
                    }
                }
            }
        }
    }
    Ok((checked, wrong))
}

fn wait_background_done(o: &Options) -> io::Result<()> {
    let mut conn = connect(o)?;
    let started = Instant::now();
    loop {
        let info = info_all(&mut conn)?;
        let busy = num_field(&info, "rdb_bgsave_in_progress") == Some(1)
            || num_field(&info, "aof_rewrite_in_progress") == Some(1);
        if !busy {
            return Ok(());
        }
        if started.elapsed() > Duration::from_secs(600) {
            return Err(other(
                "a background save or rewrite did not finish in 10 minutes",
            ));
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn median(mut v: Vec<f64>) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    v.sort_by(f64::total_cmp);
    Some(v[v.len() / 2])
}

/// Median of a memory field over the second quarter of the run and over the
/// last quarter: a steady keyspace should not keep growing.
fn memory_trend(records: &[Value], field: &str) -> Value {
    let n = records.len();
    if n < 8 {
        return json!(null);
    }
    let pick = |range: std::ops::Range<usize>| {
        median(
            records[range]
                .iter()
                .filter_map(|r| r[field].as_u64().map(|b| b as f64))
                .collect(),
        )
    };
    match (pick(n / 4..n / 2), pick(n * 3 / 4..n)) {
        (Some(a), Some(b)) => json!({
            "second_quarter_mib": (a / 1048576.0 * 10.0).round() / 10.0,
            "last_quarter_mib": (b / 1048576.0 * 10.0).round() / 10.0,
            "growth_percent": ((b - a) / a * 1000.0).round() / 10.0,
        }),
        _ => json!(null),
    }
}

fn run(o: Options) -> io::Result<bool> {
    let o = Arc::new(o);
    fs::create_dir_all(&o.out)?;
    let data = o.out.join("data");
    fs::create_dir_all(&data)?;
    let log = o.out.join("server.log");
    let shared = Arc::new(Shared::default());
    *shared.log.lock().unwrap_or_else(|e| e.into_inner()) = Some(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(o.out.join("events.jsonl"))?,
    );

    let mut server = if o.external {
        None
    } else {
        Some(Server::start(&o, &data, &log)?)
    };
    let version = {
        let mut c = connect(&o)?;
        info_field(&info_all(&mut c)?, "redis_version").map(str::to_owned)
    };
    println!(
        "rexyra-soak: {} clients ({} pipelined at depth {}), {} keys per kind each, for {:?}; output in {}",
        o.clients,
        o.pipelined,
        o.depth,
        o.keys,
        o.duration,
        o.out.display()
    );
    if let Some(s) = &server {
        println!(
            "server pid {} on port {}, version {:?}",
            s.pid(),
            o.port,
            version
        );
    }

    let started = Instant::now();
    let mut workers = Vec::new();
    for id in 0..o.clients {
        let depth = if id < o.pipelined { o.depth } else { 1 };
        let (o2, s2) = (o.clone(), shared.clone());
        workers.push(thread::spawn(move || worker(o2, s2, id, depth)));
    }
    let (o2, s2) = (o.clone(), shared.clone());
    let admin_t = thread::spawn(move || admin(o2, s2));
    let (o2, s2) = (o.clone(), shared.clone());
    let churn_t = thread::spawn(move || churn(o2, s2));
    let (o2, s2, path) = (o.clone(), shared.clone(), o.out.join("report.jsonl"));
    let report_t = thread::spawn(move || reporter(o2, s2, path));

    let mut crashed = None;
    while started.elapsed() < o.duration {
        thread::sleep(Duration::from_millis(500));
        if let Some(status) = server.as_mut().and_then(Server::exited) {
            crashed = Some(status);
            break;
        }
        if workers.iter().all(|w| w.is_finished()) {
            break;
        }
    }
    shared.stop.store(true, Relaxed);
    let ran_for = started.elapsed();
    let mut models = Vec::new();
    let mut worker_failures = Vec::new();
    for (id, w) in workers.into_iter().enumerate() {
        match w.join() {
            Ok(Ok(m)) => models.push(m),
            Ok(Err(e)) => worker_failures.push(format!("worker {id}: {e}")),
            Err(_) => worker_failures.push(format!("worker {id} panicked")),
        }
    }
    let _ = admin_t.join();
    let _ = churn_t.join();
    let records = report_t.join().unwrap_or_default();

    let mut summary = json!({
        "version": version,
        "options": {
            "duration_s": o.duration.as_secs(), "clients": o.clients, "pipelined": o.pipelined,
            "depth": o.depth, "keys_per_kind": o.keys, "appendonly": o.appendonly,
            "bgsave_every_s": o.bgsave.as_secs(), "rewrite_every_s": o.rewrite.as_secs(),
            "churn_per_sec": o.churn_per_sec, "seed": o.seed, "external": o.external,
            "server": o.server.display().to_string(),
        },
        "ran_for_s": ran_for.as_secs(),
        "operations": shared.ops.load(Relaxed),
        "commands": shared.commands.load(Relaxed),
        "mismatches": shared.mismatches.load(Relaxed),
        "connection_errors": shared.conn_errors.load(Relaxed),
        "bgsaves": shared.bgsaves.load(Relaxed),
        "aof_rewrites": shared.rewrites.load(Relaxed),
        "admin_errors": shared.admin_errors.load(Relaxed),
        "connections_churned": shared.churned.load(Relaxed),
        "churn_errors": shared.churn_errors.load(Relaxed),
        "worker_failures": worker_failures,
        "server_exit_during_run": crashed,
        "memory_used_trend": memory_trend(&records, "used_memory"),
        "memory_rss_trend": memory_trend(&records, "used_memory_rss"),
        "throughput_ops_per_s_median": median(records.iter().filter_map(|r| r["ops_per_s"].as_f64()).collect()),
        "worst_interval_p999_ms": records.iter().filter_map(|r| r["single"]["p999_ms"].as_f64()).fold(0.0, f64::max),
        "worst_single_ms": records.iter().filter_map(|r| r["single"]["max_ms"].as_f64()).fold(0.0, f64::max),
    });

    let mut pass = crashed.is_none()
        && summary["worker_failures"]
            .as_array()
            .is_some_and(Vec::is_empty)
        && summary["mismatches"] == 0
        && summary["connection_errors"] == 0
        && summary["admin_errors"] == 0
        && summary["churn_errors"] == 0;

    if crashed.is_none() {
        wait_background_done(&o)?;
        let (checked, wrong) = verify(&o, &models, &shared, "end")?;
        println!("final check: {checked} keys read back, {wrong} differ");
        summary["final_check"] = json!({"keys": checked, "differ": wrong});
        pass &= wrong == 0;

        if let Some(s) = server.take() {
            let t = Instant::now();
            s.stop(true)?;
            let stop_s = t.elapsed().as_secs_f64();
            let t = Instant::now();
            let s = Server::start(&o, &data, &log)?;
            let load_s = t.elapsed().as_secs_f64();
            let (checked, wrong) = verify(&o, &models, &shared, "after-restart")?;
            println!(
                "after restart: shutdown {stop_s:.1}s, load {load_s:.1}s, {checked} keys read back, {wrong} differ"
            );
            summary["restart_check"] =
                json!({"keys": checked, "differ": wrong, "shutdown_s": stop_s, "load_s": load_s});
            pass &= wrong == 0;
            s.stop(false)?;
        }
    }
    summary["pass"] = json!(pass);
    fs::write(
        o.out.join("summary.json"),
        serde_json::to_string_pretty(&summary).unwrap_or_default(),
    )?;
    println!(
        "{}",
        serde_json::to_string_pretty(&summary).unwrap_or_default()
    );
    println!("{}", if pass { "PASS" } else { "FAIL" });
    Ok(pass)
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.iter().any(|a| a == "-h" || a == "--help") {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    match Options::parse(&argv).and_then(run) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(e) => {
            eprintln!("rexyra-soak: {e}");
            ExitCode::from(2)
        }
    }
}
