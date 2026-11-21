//! Running one scenario: start a fresh server, load, traffic, BGSAVE,
//! checks, reload, and the JSON record of it all.

use crate::load::{self, expect_ok, pattern, set_header, Rng};
use crate::resp::{encode, info_field, Conn, Reply};
use crate::scenario::{self, Kind, Spec, Toucher};
use crate::server::{ensure_port_free, ServerProc};
use crate::stats::{self, rel_ms, Sample, Window};
use serde_json::{json, Value};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const TIMELINE_BUCKET_NS: u64 = 50_000_000;
const LONG: Duration = Duration::from_secs(1800);

pub struct Options {
    pub scenario: String,
    pub runs: u32,
    pub first_run: u32,
    pub port: u16,
    pub server: PathBuf,
    pub data_root: PathBuf,
    pub out: PathBuf,
    pub save_delay_us: Option<u64>,
    pub warmup: Duration,
    pub baseline: Duration,
    pub after: Duration,
    pub clients: usize,
    pub get_percent: u64,
    pub monitor: Duration,
    pub keep_data: bool,
    /// Run the scenario without its toucher connection.
    pub no_toucher: bool,
    pub label: Option<String>,
}

fn other(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

fn num<T: FromStr>(flag: &str, value: &str) -> io::Result<T> {
    value
        .parse()
        .map_err(|_| other(format!("{flag}: cannot parse {value:?}")))
}

impl Options {
    pub fn parse(args: &[String]) -> io::Result<Self> {
        let exe_dir = std::env::current_exe()?
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default();
        let target_dir = exe_dir.parent().map(Path::to_path_buf).unwrap_or_default();
        let mut o = Options {
            scenario: String::new(),
            runs: 3,
            first_run: 1,
            port: 6400,
            server: exe_dir.join("rexyra-server.exe"),
            data_root: target_dir.join("bench-data"),
            out: PathBuf::from("docs/benchmarks/bgsave/raw"),
            save_delay_us: None,
            warmup: Duration::from_secs(1),
            baseline: Duration::from_secs(5),
            after: Duration::from_secs(3),
            clients: 50,
            get_percent: 50,
            monitor: Duration::from_millis(10),
            keep_data: false,
            no_toucher: false,
            label: None,
        };
        let mut it = args.iter();
        while let Some(flag) = it.next() {
            if flag == "--keep-data" {
                o.keep_data = true;
                continue;
            }
            if flag == "--no-toucher" {
                o.no_toucher = true;
                continue;
            }
            let value = it
                .next()
                .ok_or_else(|| other(format!("{flag} needs a value")))?;
            let secs = |v: &str| num::<f64>(flag, v).map(Duration::from_secs_f64);
            match flag.as_str() {
                "--scenario" => o.scenario = value.clone(),
                "--runs" => o.runs = num(flag, value)?,
                "--first-run" => o.first_run = num(flag, value)?,
                "--port" => o.port = num(flag, value)?,
                "--server" => o.server = PathBuf::from(value),
                "--data-root" => o.data_root = PathBuf::from(value),
                "--out" => o.out = PathBuf::from(value),
                "--save-delay-us" => o.save_delay_us = Some(num(flag, value)?),
                "--warmup-secs" => o.warmup = secs(value)?,
                "--baseline-secs" => o.baseline = secs(value)?,
                "--after-secs" => o.after = secs(value)?,
                "--clients" => o.clients = num(flag, value)?,
                "--get-percent" => o.get_percent = num(flag, value)?,
                "--monitor-ms" => o.monitor = Duration::from_millis(num(flag, value)?),
                "--label" => o.label = Some(value.clone()),
                _ => return Err(other(format!("unknown option {flag}"))),
            }
        }
        if o.scenario.is_empty() {
            return Err(other("--scenario is required (see `rexyra-bench list`)"));
        }
        Ok(o)
    }

    fn params(&self) -> Value {
        json!({
            "port": self.port,
            "server": self.server.display().to_string(),
            "save_delay_us": self.save_delay_us.unwrap_or(0),
            "warmup_s": self.warmup.as_secs_f64(),
            "baseline_s": self.baseline.as_secs_f64(),
            "after_s": self.after.as_secs_f64(),
            "clients": self.clients,
            "get_percent": self.get_percent,
            "monitor_interval_ms": self.monitor.as_secs_f64() * 1e3,
            "label": self.label,
            "no_toucher": self.no_toucher,
        })
    }
}

pub fn list() {
    for s in scenario::all() {
        println!("{:<12} {}", s.name, s.about);
    }
}

pub fn run_scenario(o: &Options) -> io::Result<()> {
    let spec = scenario::find(&o.scenario).ok_or_else(|| {
        other(format!(
            "unknown scenario {:?}; see `rexyra-bench list`",
            o.scenario
        ))
    })?;
    if spec.needs_delay && o.save_delay_us.is_none() {
        return Err(other(format!(
            "scenario {} needs --save-delay-us",
            spec.name
        )));
    }
    fs::create_dir_all(&o.out)?;
    fs::create_dir_all(&o.data_root)?;
    for run in o.first_run..o.first_run + o.runs {
        let stem = match &o.label {
            Some(label) => format!("{}-{label}-run{run:02}", spec.name),
            None => format!("{}-run{run:02}", spec.name),
        };
        eprintln!("== {stem}");
        let record = match spec.kind {
            Kind::Traffic => run_traffic(&spec, o, run, &stem)?,
            Kind::Idle => run_idle(&spec, o, run, &stem)?,
        };
        let path = o.out.join(format!("{stem}.json"));
        fs::write(
            &path,
            serde_json::to_vec_pretty(&record).map_err(io::Error::other)?,
        )?;
        eprintln!("   {}", one_line(&record));
        eprintln!("   wrote {}", path.display());
    }
    Ok(())
}

fn one_line(r: &Value) -> String {
    let g = |p: &str| r.pointer(p).and_then(Value::as_f64).unwrap_or(f64::NAN);
    format!(
        "save {:.0} ms, file {:.1} MiB, p99 base/during {:.3}/{:.3} ms, max base/during {:.2}/{:.2} ms, gap during {:.2} ms, peak-before {:.1} MiB, reload ok {}",
        g("/save/duration_ms"),
        g("/save/file_bytes") / 1048576.0,
        g("/windows/baseline/foreground/p99_ms"),
        g("/windows/during/foreground/p99_ms"),
        g("/windows/baseline/foreground/max_ms"),
        g("/windows/during/foreground/max_ms"),
        g("/windows/during/longest_gap/longest_ms"),
        g("/memory/peak_used_minus_before") / 1048576.0,
        r.pointer("/verify/dbsize_match").unwrap_or(&Value::Null),
    )
}

#[derive(Default)]
struct Shared {
    stop: AtomicBool,
    /// BGSAVE acknowledged (ns since origin); 0 = not yet.
    ack_ns: AtomicU64,
    /// First poll sent after the acknowledgement that reported no save in
    /// progress (receive time); 0 = not yet.
    end_ns: AtomicU64,
    /// Receive time of the last poll that reported the save in progress.
    last_busy_ns: AtomicU64,
    /// The controller is waiting for the toucher's gate.
    gate_wanted: AtomicBool,
}

fn now_ns(origin: Instant) -> u64 {
    origin.elapsed().as_nanos() as u64
}

struct MemSample {
    send_ns: u64,
    recv_ns: u64,
    used: u64,
    rss: u64,
    in_progress: bool,
}

fn parse_u64(text: &str, key: &str) -> io::Result<u64> {
    info_field(text, key)
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| other(format!("INFO has no numeric {key}")))
}

/// Polls `INFO memory` and `INFO persistence` (pipelined) every `interval`
/// on its own connection, and notices the end of the save.
fn monitor(
    port: u16,
    interval: Duration,
    shared: Arc<Shared>,
    origin: Instant,
) -> io::Result<Vec<MemSample>> {
    let mut conn = Conn::connect(port)?;
    conn.set_timeout(Some(LONG))?;
    let mut frame = Vec::new();
    encode(&mut frame, &[b"INFO", b"memory"]);
    encode(&mut frame, &[b"INFO", b"persistence"]);
    let interval_ns = interval.as_nanos() as u64;
    let mut out = Vec::with_capacity(4096);
    while !shared.stop.load(SeqCst) {
        let send_ns = now_ns(origin);
        conn.send(&frame)?;
        let memory = conn.read()?.text()?;
        let persistence = conn.read()?.text()?;
        let recv_ns = now_ns(origin);
        let in_progress = info_field(&persistence, "rdb_bgsave_in_progress") == Some("1");
        let ack = shared.ack_ns.load(SeqCst);
        // Only a poll sent after the acknowledgement can speak for this save.
        if ack != 0 && send_ns >= ack {
            if in_progress {
                shared.last_busy_ns.store(recv_ns, SeqCst);
            } else if shared.end_ns.load(SeqCst) == 0 {
                shared.end_ns.store(recv_ns, SeqCst);
            }
        }
        out.push(MemSample {
            send_ns,
            recv_ns,
            used: parse_u64(&memory, "used_memory")?,
            rss: parse_u64(&memory, "used_memory_rss")?,
            in_progress,
        });
        let next = send_ns + interval_ns;
        let now = now_ns(origin);
        if next > now {
            thread::sleep(Duration::from_nanos(next - now));
        }
    }
    Ok(out)
}

struct WorkerOut {
    samples: Vec<Sample>,
    errors: u64,
}

/// One closed-loop foreground client: GET or SET of a random existing
/// `s:<n>` key, or with `fields` HGET or HSET of one of its fields.
#[allow(clippy::too_many_arguments)]
fn worker(
    port: u16,
    keys: u64,
    fields: u64,
    value: Arc<Vec<u8>>,
    get_percent: u64,
    seed: u64,
    shared: Arc<Shared>,
    origin: Instant,
) -> io::Result<WorkerOut> {
    let mut conn = Conn::connect(port)?;
    conn.set_timeout(Some(LONG))?;
    let mut rng = Rng::new(seed);
    let mut frame = Vec::with_capacity(value.len() + 64);
    let mut samples = Vec::with_capacity(1 << 16);
    let mut errors = 0;
    while !shared.stop.load(SeqCst) {
        let key = format!("s:{}", rng.below(keys));
        let is_get = rng.below(100) < get_percent;
        frame.clear();
        if fields > 0 {
            let field = format!("f{}", rng.below(fields));
            if is_get {
                encode(&mut frame, &[b"HGET", key.as_bytes(), field.as_bytes()]);
            } else {
                encode(
                    &mut frame,
                    &[b"HSET", key.as_bytes(), field.as_bytes(), &value],
                );
            }
        } else if is_get {
            encode(&mut frame, &[b"GET", key.as_bytes()]);
        } else {
            encode(&mut frame, &[b"SET", key.as_bytes(), &value]);
        }
        let t0 = Instant::now();
        conn.send(&frame)?;
        let reply = conn.read()?;
        let t1 = Instant::now();
        // A GET of a key that must exist returning nil is an error too.
        if reply.is_error() || (is_get && reply == Reply::Bulk(None)) {
            errors += 1;
        }
        samples.push(Sample {
            end_ns: t1.duration_since(origin).as_nanos() as u64,
            lat_ns: (t1 - t0).as_nanos() as u64,
            target: u32::from(!is_get),
        });
    }
    Ok(WorkerOut { samples, errors })
}

enum TouchOp {
    SetBig {
        header: Vec<u8>,
        value: Arc<Vec<u8>>,
    },
    Rpush {
        key: String,
    },
    Hset {
        key: String,
        fields: u64,
    },
}

impl TouchOp {
    fn name(&self) -> String {
        match self {
            TouchOp::SetBig { header, .. } => String::from_utf8_lossy(header)
                .split("\r\n")
                .nth(4)
                .map(|k| format!("SET {k}"))
                .unwrap_or_default(),
            TouchOp::Rpush { key } => format!("RPUSH {key}"),
            TouchOp::Hset { key, .. } => format!("HSET {key}"),
        }
    }
}

fn touch_ops(spec: &Spec) -> Vec<TouchOp> {
    match spec.toucher {
        Toucher::None => Vec::new(),
        Toucher::BigStrings => {
            // A different pattern from the loaded one; same size.
            let value = Arc::new(pattern(spec.big_size, 0xB16));
            (0..spec.big_keys)
                .map(|i| TouchOp::SetBig {
                    header: set_header(format!("big:{i}").as_bytes(), spec.big_size),
                    value: Arc::clone(&value),
                })
                .collect()
        }
        Toucher::Collections { .. } => (0..spec.lists)
            .map(|i| TouchOp::Rpush {
                key: format!("list:{i}"),
            })
            .chain((0..spec.hashes).map(|i| TouchOp::Hset {
                key: format!("hash:{i}"),
                fields: spec.hash_fields as u64,
            }))
            .collect(),
    }
}

/// The toucher. Each op runs while holding `gate`, so the controller can
/// take a consistent DBSIZE/LLEN reading and issue BGSAVE in between ops.
fn toucher(
    port: u16,
    ops: Vec<TouchOp>,
    pace: Option<Duration>,
    gate: Arc<Mutex<()>>,
    kick: Receiver<()>,
    shared: Arc<Shared>,
    origin: Instant,
) -> io::Result<WorkerOut> {
    let mut conn = Conn::connect(port)?;
    conn.set_timeout(Some(LONG))?;
    let mut rng = Rng::new(0x70C4);
    let mut samples = Vec::with_capacity(1 << 12);
    let mut errors = 0;
    let mut frame = Vec::new();
    'outer: while !shared.stop.load(SeqCst) {
        if let Some(pace) = pace {
            if let Err(RecvTimeoutError::Disconnected) = kick.recv_timeout(pace) {
                thread::sleep(pace);
            }
        }
        for (i, op) in ops.iter().enumerate() {
            if shared.stop.load(SeqCst) {
                break 'outer;
            }
            // `Mutex` is not fair: back-to-back ops would re-take it before
            // the controller ever got it, so stand aside while it asks.
            while shared.gate_wanted.load(SeqCst) {
                thread::sleep(Duration::from_micros(200));
            }
            let _held = gate.lock().unwrap_or_else(PoisonError::into_inner);
            frame.clear();
            match op {
                TouchOp::SetBig { header, .. } => frame.extend_from_slice(header),
                TouchOp::Rpush { key } => encode(&mut frame, &[b"RPUSH", key.as_bytes(), b"t"]),
                TouchOp::Hset { key, fields } => {
                    let field = format!("f{}", rng.below(*fields));
                    encode(
                        &mut frame,
                        &[b"HSET", key.as_bytes(), field.as_bytes(), b"t"],
                    );
                }
            }
            let t0 = Instant::now();
            conn.send(&frame)?;
            if let TouchOp::SetBig { value, .. } = op {
                conn.send(value)?;
                conn.send(b"\r\n")?;
            }
            let reply = conn.read()?;
            let t1 = Instant::now();
            if reply.is_error() {
                errors += 1;
            }
            samples.push(Sample {
                end_ns: t1.duration_since(origin).as_nanos() as u64,
                lat_ns: (t1 - t0).as_nanos() as u64,
                target: i as u32,
            });
        }
    }
    Ok(WorkerOut { samples, errors })
}

fn join<T>(handle: JoinHandle<io::Result<T>>, what: &str) -> io::Result<T> {
    handle
        .join()
        .unwrap_or_else(|_| Err(other(format!("the {what} thread panicked"))))
}

fn prepare_dir(o: &Options, stem: &str) -> io::Result<PathBuf> {
    let dir = o.data_root.join(stem);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn info(conn: &mut Conn, section: &str) -> io::Result<String> {
    conn.call_str(&["INFO", section])?.text()
}

fn exe_info(path: &Path) -> Value {
    let meta = fs::metadata(path).ok();
    json!({
        "path": path.display().to_string(),
        "bytes": meta.as_ref().map(fs::Metadata::len),
        "modified_unix": meta
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs()),
    })
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Starts the server, loads the dataset and sets the save delay.
fn setup(
    spec: &Spec,
    o: &Options,
    dir: &Path,
    log: &Path,
) -> io::Result<(ServerProc, Conn, Value)> {
    ensure_port_free(o.port)?;
    let mut server = ServerProc::start(&o.server, o.port, dir, None, log)?;
    server.wait_ready(Duration::from_secs(60))?;
    let mut control = Conn::connect(o.port)?;
    control.set_timeout(Some(LONG))?;
    let started = Instant::now();
    load::load_dataset(spec, o.port)?;
    let load_s = started.elapsed().as_secs_f64();
    let dbsize = control.call(&[b"DBSIZE"])?.int()?;
    if dbsize != spec.key_count() as i64 {
        return Err(other(format!(
            "DBSIZE {dbsize} after loading, expected {}",
            spec.key_count()
        )));
    }
    let delay = o.save_delay_us.unwrap_or(0).to_string();
    expect_ok(&control.call_str(&["CONFIG", "SET", "rdb-key-save-delay", &delay])?)?;
    let readback = match control.call_str(&["CONFIG", "GET", "rdb-key-save-delay"])? {
        Reply::Array(Some(items)) if items.len() == 2 => items[1].text()?,
        other_reply => return Err(other(format!("CONFIG GET: {}", other_reply.short()))),
    };
    let memory = info(&mut control, "memory")?;
    let dataset = json!({
        "load_seconds": load_s,
        "dbsize": dbsize,
        "used_memory_after_load": parse_u64(&memory, "used_memory")?,
        "used_memory_rss_after_load": parse_u64(&memory, "used_memory_rss")?,
        "rdb_key_save_delay_readback": readback,
        "server_pid": server.pid(),
    });
    // Let the allocator and the OS settle after the load.
    thread::sleep(Duration::from_secs(1));
    Ok((server, control, dataset))
}

fn llens(conn: &mut Conn, spec: &Spec) -> io::Result<Vec<i64>> {
    (0..spec.lists)
        .map(|i| conn.call_str(&["LLEN", &format!("list:{i}")])?.int())
        .collect()
}

/// Waits until the monitor has seen the save end.
fn wait_for_end(
    shared: &Shared,
    monitor: &JoinHandle<io::Result<Vec<MemSample>>>,
) -> io::Result<u64> {
    let deadline = Instant::now() + Duration::from_secs(3600);
    loop {
        let end = shared.end_ns.load(SeqCst);
        if end != 0 {
            return Ok(end);
        }
        if monitor.is_finished() {
            return Err(other(
                "the monitor connection stopped before the save ended",
            ));
        }
        if Instant::now() > deadline {
            return Err(other("the save did not finish within an hour"));
        }
        thread::sleep(Duration::from_millis(2));
    }
}

fn mem_window(samples: &[MemSample], w: Window, zero: u64) -> Value {
    let inside: Vec<&MemSample> = samples.iter().filter(|s| w.contains(s.recv_ns)).collect();
    let mut prev = w.start_ns;
    let mut max_interval = 0;
    for s in &inside {
        max_interval = max_interval.max(s.recv_ns - prev);
        prev = s.recv_ns;
    }
    max_interval = max_interval.max(w.end_ns.saturating_sub(prev));
    let max_used = inside.iter().map(|s| s.used).max();
    json!({
        "samples": inside.len(),
        "max_used": max_used,
        "max_used_at_ms": inside.iter().find(|s| Some(s.used) == max_used).map(|s| rel_ms(s.recv_ns, zero)),
        "min_used": inside.iter().map(|s| s.used).min(),
        "last_used": inside.last().map(|s| s.used),
        "max_rss": inside.iter().map(|s| s.rss).max(),
        "max_sample_interval_ms": stats::ms(max_interval),
    })
}

fn mem_series(samples: &[MemSample], zero: u64) -> Value {
    Value::Array(
        samples
            .iter()
            .map(|s| {
                json!([
                    (rel_ms(s.send_ns, zero) * 1000.0).round() / 1000.0,
                    (rel_ms(s.recv_ns, zero) * 1000.0).round() / 1000.0,
                    s.used,
                    s.rss,
                    u8::from(s.in_progress)
                ])
            })
            .collect(),
    )
}

fn timeline(fg: &[Sample], tc: &[Sample], stop_ns: u64, zero: u64) -> Value {
    let buckets = (stop_ns / TIMELINE_BUCKET_NS + 1) as usize;
    let fill = |samples: &[Sample]| {
        let mut count = vec![0u64; buckets];
        let mut max = vec![0u64; buckets];
        for s in samples {
            let b = ((s.end_ns / TIMELINE_BUCKET_NS) as usize).min(buckets - 1);
            count[b] += 1;
            max[b] = max[b].max(s.lat_ns);
        }
        let max_ms: Vec<f64> = max
            .iter()
            .map(|&m| (stats::ms(m) * 1000.0).round() / 1000.0)
            .collect();
        (count, max_ms)
    };
    let (fg_count, fg_max) = fill(fg);
    let (tc_count, tc_max) = fill(tc);
    json!({
        "bucket_ms": stats::ms(TIMELINE_BUCKET_NS),
        "first_bucket_starts_at_ms": rel_ms(0, zero),
        "foreground_count": fg_count,
        "foreground_max_ms": fg_max,
        "toucher_count": tc_count,
        "toucher_max_ms": tc_max,
    })
}

#[allow(clippy::too_many_lines)]
fn run_traffic(spec: &Spec, o: &Options, run: u32, stem: &str) -> io::Result<Value> {
    let dir = prepare_dir(o, stem)?;
    let log = o.data_root.join(format!("{stem}.server.log"));
    let started_unix = unix_now();
    let (server, mut control, dataset) = setup(spec, o, &dir, &log)?;

    let origin = Instant::now();
    let shared = Arc::new(Shared::default());
    let monitor_handle = {
        let shared = Arc::clone(&shared);
        let (port, interval) = (o.port, o.monitor);
        thread::spawn(move || monitor(port, interval, shared, origin))
    };
    let value = Arc::new(pattern(spec.small_value, 0x5E7));
    let workers: Vec<JoinHandle<io::Result<WorkerOut>>> = (0..o.clients)
        .map(|c| {
            let (shared, value) = (Arc::clone(&shared), Arc::clone(&value));
            let (port, keys, get_percent) = (o.port, spec.small_keys as u64, o.get_percent);
            let fields = spec.small_fields as u64;
            thread::spawn(move || {
                worker(
                    port,
                    keys,
                    fields,
                    value,
                    get_percent,
                    1000 + c as u64,
                    shared,
                    origin,
                )
            })
        })
        .collect();
    let gate = Arc::new(Mutex::new(()));
    let (kick_tx, kick_rx) = mpsc::channel();
    let ops = if o.no_toucher {
        Vec::new()
    } else {
        touch_ops(spec)
    };
    let op_names: Vec<String> = ops.iter().map(TouchOp::name).collect();
    let toucher_handle = if ops.is_empty() {
        None
    } else {
        let pace = match spec.toucher {
            Toucher::Collections { pace_ms } => Some(Duration::from_millis(pace_ms)),
            _ => None,
        };
        let (shared, gate) = (Arc::clone(&shared), Arc::clone(&gate));
        let port = o.port;
        Some(thread::spawn(move || {
            toucher(port, ops, pace, gate, kick_rx, shared, origin)
        }))
    };

    thread::sleep(o.warmup);
    let baseline_start = now_ns(origin);
    thread::sleep(o.baseline);

    // The snapshot instant: no toucher op in flight, nothing creates or
    // deletes keys, so DBSIZE (and LLEN) now is what the file must hold.
    shared.gate_wanted.store(true, SeqCst);
    let held = gate.lock().unwrap_or_else(PoisonError::into_inner);
    let dbsize_at_bgsave = control.call(&[b"DBSIZE"])?.int()?;
    let llen_at_bgsave = llens(&mut control, spec)?;
    let memory_before = info(&mut control, "memory")?;
    let bgsave_sent = now_ns(origin);
    let bgsave_reply = control.call(&[b"BGSAVE"])?;
    let ack = now_ns(origin);
    shared.ack_ns.store(ack, SeqCst);
    shared.gate_wanted.store(false, SeqCst);
    drop(held);
    let _ = kick_tx.send(());
    let bgsave_text = bgsave_reply.text().unwrap_or_else(|_| bgsave_reply.short());
    let started_ok = bgsave_text.contains("Background saving started");

    let end_result = if started_ok {
        wait_for_end(&shared, &monitor_handle)
    } else {
        Err(other(format!("BGSAVE replied {bgsave_text}")))
    };
    if end_result.is_ok() {
        thread::sleep(o.after);
    }
    let stop_ns = now_ns(origin);
    shared.stop.store(true, SeqCst);
    let mut fg = Vec::new();
    let mut fg_errors = 0;
    for w in workers {
        let out = join(w, "worker")?;
        fg.extend(out.samples);
        fg_errors += out.errors;
    }
    let (tc, tc_errors) = match toucher_handle {
        Some(h) => {
            let out = join(h, "toucher")?;
            (out.samples, out.errors)
        }
        None => (Vec::new(), 0),
    };
    let mem = join(monitor_handle, "monitor")?;
    let end = end_result?;
    let last_busy = shared.last_busy_ns.load(SeqCst);

    let persistence = info(&mut control, "persistence")?;
    let status = info_field(&persistence, "rdb_last_bgsave_status")
        .unwrap_or("?")
        .to_owned();
    let lastsave = control.call(&[b"LASTSAVE"])?.int()?;
    let file_bytes = fs::metadata(dir.join("dump.rdb")).map(|m| m.len()).ok();
    drop(control);
    server.shutdown()?;

    // Reload the snapshot in a new process.
    let mut reloaded = ServerProc::start(&o.server, o.port, &dir, None, &log)?;
    let reload_time = reloaded.wait_ready(Duration::from_secs(1800))?;
    let mut check = Conn::connect(o.port)?;
    check.set_timeout(Some(LONG))?;
    let dbsize_reloaded = check.call(&[b"DBSIZE"])?.int()?;
    let llen_reloaded = llens(&mut check, spec)?;
    drop(check);
    reloaded.shutdown()?;

    // Analysis.
    fg.sort_unstable_by_key(|s| s.end_ns);
    let ends: Vec<u64> = fg.iter().map(|s| s.end_ns).collect();
    let windows = [
        (
            "baseline",
            Window {
                start_ns: baseline_start,
                end_ns: ack,
            },
        ),
        (
            "during",
            Window {
                start_ns: ack,
                end_ns: end,
            },
        ),
        (
            "after",
            Window {
                start_ns: end,
                end_ns: stop_ns,
            },
        ),
    ];
    let mut win_json = serde_json::Map::new();
    for (name, w) in windows {
        let lat = |samples: &[Sample], keep: &dyn Fn(&Sample) -> bool| {
            serde_json::to_value(stats::latency(samples, w, keep, ack)).unwrap_or(Value::Null)
        };
        win_json.insert(
            name.to_owned(),
            json!({
                "start_ms": rel_ms(w.start_ns, ack),
                "end_ms": rel_ms(w.end_ns, ack),
                "seconds": w.secs(),
                "foreground": lat(&fg, &|_| true),
                "foreground_get": lat(&fg, &|s| s.target == 0),
                "foreground_set": lat(&fg, &|s| s.target == 1),
                "longest_gap": stats::longest_gap(&ends, w, ack),
                "toucher": lat(&tc, &|_| true),
                "memory": mem_window(&mem, w, ack),
            }),
        );
    }
    let during = windows[1].1;
    let targets: Vec<Value> = if matches!(spec.toucher, Toucher::Collections { .. }) {
        op_names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let mine: Vec<&Sample> = tc.iter().filter(|s| s.target == i as u32).collect();
                let first = mine.iter().find(|s| s.end_ns - s.lat_ns >= ack);
                let in_save: Vec<&&Sample> = mine.iter().filter(|s| during.contains(s.end_ns)).collect();
                json!({
                    "op": name,
                    "first_started_after_ack_latency_ms": first.map(|s| stats::ms(s.lat_ns)),
                    "first_started_after_ack_completed_at_ms": first.map(|s| rel_ms(s.end_ns, ack)),
                    "count_during": in_save.len(),
                    "max_during_ms": in_save.iter().map(|s| s.lat_ns).max().map(stats::ms),
                    "max_baseline_ms": mine.iter().filter(|s| windows[0].1.contains(s.end_ns)).map(|s| s.lat_ns).max().map(stats::ms),
                })
            })
            .collect()
    } else {
        Vec::new()
    };
    let used_before = parse_u64(&memory_before, "used_memory")?;
    let rss_before = parse_u64(&memory_before, "used_memory_rss")?;
    let base_mem = &win_json["baseline"]["memory"];
    let during_mem = &win_json["during"]["memory"];
    let peak = during_mem["max_used"].as_u64();
    let base_max = base_mem["max_used"].as_u64();
    let memory = json!({
        "used_before": used_before,
        "rss_before": rss_before,
        "baseline_max_used": base_max,
        "baseline_max_rss": base_mem["max_rss"],
        "peak_used_during": peak,
        "peak_rss_during": during_mem["max_rss"],
        "peak_used_minus_before": peak.map(|p| p as i64 - used_before as i64),
        "peak_used_minus_baseline_max": peak.zip(base_max).map(|(p, b)| p as i64 - b as i64),
        "after_last_used": win_json["after"]["memory"]["last_used"],
        "max_sample_interval_during_ms": during_mem["max_sample_interval_ms"],
    });

    let record = json!({
        "tool": "rexyra-bench",
        "tool_version": env!("CARGO_PKG_VERSION"),
        "scenario": spec.name,
        "label": o.label,
        "run": run,
        "started_unix": started_unix,
        "spec": spec,
        "params": o.params(),
        "server_exe": exe_info(&o.server),
        "dataset": dataset,
        "time_zero": "all *_ms times are relative to the moment the BGSAVE reply was read",
        "events_ms": {
            "traffic_start": rel_ms(0, ack),
            "baseline_start": rel_ms(baseline_start, ack),
            "bgsave_sent": rel_ms(bgsave_sent, ack),
            "bgsave_ack": 0.0,
            "last_poll_in_progress": if last_busy == 0 { Value::Null } else { json!(rel_ms(last_busy, ack)) },
            "save_end_detected": rel_ms(end, ack),
            "traffic_stop": rel_ms(stop_ns, ack),
        },
        "save": {
            "bgsave_reply": bgsave_text,
            "duration_ms": rel_ms(end, ack),
            "duration_note": "from reading the BGSAVE reply to reading the first INFO reply (to a poll sent after it) with rdb_bgsave_in_progress:0; resolution is the poll interval plus the INFO round trip",
            "last_bgsave_status": status,
            "lastsave_unix": lastsave,
            "file_bytes": file_bytes,
        },
        "memory": memory,
        "windows": win_json,
        "toucher_ops": op_names,
        "toucher_targets": targets,
        "errors": { "foreground": fg_errors, "toucher": tc_errors },
        "verify": {
            "dbsize_at_bgsave": dbsize_at_bgsave,
            "dbsize_reloaded": dbsize_reloaded,
            "dbsize_match": dbsize_at_bgsave == dbsize_reloaded,
            "llen_at_bgsave": llen_at_bgsave,
            "llen_reloaded": llen_reloaded,
            "llen_match": llen_at_bgsave == llen_reloaded,
            "reload_ready_seconds": reload_time.as_secs_f64(),
        },
        "timeline": timeline(&fg, &tc, stop_ns, ack),
        "memory_samples_columns": ["send_ms", "recv_ms", "used_memory", "used_memory_rss", "rdb_bgsave_in_progress"],
        "memory_samples": mem_series(&mem, ack),
    });
    if !o.keep_data {
        fs::remove_dir_all(&dir)?;
    }
    Ok(record)
}

fn run_idle(spec: &Spec, o: &Options, run: u32, stem: &str) -> io::Result<Value> {
    let dir = prepare_dir(o, stem)?;
    let log = o.data_root.join(format!("{stem}.server.log"));
    let started_unix = unix_now();
    let (server, mut control, dataset) = setup(spec, o, &dir, &log)?;

    let origin = Instant::now();
    let shared = Arc::new(Shared::default());
    let monitor_handle = {
        let shared = Arc::clone(&shared);
        let (port, interval) = (o.port, o.monitor);
        thread::spawn(move || monitor(port, interval, shared, origin))
    };
    thread::sleep(Duration::from_secs(1));
    let memory_before = info(&mut control, "memory")?;
    let dbsize_at_bgsave = control.call(&[b"DBSIZE"])?.int()?;
    let bgsave_sent = now_ns(origin);
    let reply = control.call(&[b"BGSAVE"])?;
    let ack = now_ns(origin);
    shared.ack_ns.store(ack, SeqCst);
    let text = reply.text().unwrap_or_else(|_| reply.short());
    let end = if text.contains("Background saving started") {
        wait_for_end(&shared, &monitor_handle)
    } else {
        Err(other(format!("BGSAVE replied {text}")))
    };
    if end.is_ok() {
        thread::sleep(o.after);
    }
    let stop_ns = now_ns(origin);
    shared.stop.store(true, SeqCst);
    let mem = join(monitor_handle, "monitor")?;
    let end = end?;
    let persistence = info(&mut control, "persistence")?;
    let status = info_field(&persistence, "rdb_last_bgsave_status")
        .unwrap_or("?")
        .to_owned();
    let bgsave_bytes = fs::metadata(dir.join("dump.rdb"))?.len();
    // Keep the background-saved file for the reload check; SAVE writes a
    // new dump.rdb.
    fs::rename(dir.join("dump.rdb"), dir.join("bgsave.rdb"))?;
    let save_started = Instant::now();
    let save_reply = control.call(&[b"SAVE"])?;
    let save_ms = save_started.elapsed().as_secs_f64() * 1e3;
    expect_ok(&save_reply)?;
    let save_bytes = fs::metadata(dir.join("dump.rdb"))?.len();
    drop(control);
    server.shutdown()?;

    let mut reloaded = ServerProc::start(&o.server, o.port, &dir, Some("bgsave.rdb"), &log)?;
    let reload_time = reloaded.wait_ready(Duration::from_secs(1800))?;
    let mut check = Conn::connect(o.port)?;
    check.set_timeout(Some(LONG))?;
    let dbsize_reloaded = check.call(&[b"DBSIZE"])?.int()?;
    drop(check);
    reloaded.shutdown()?;

    let base = Window {
        start_ns: 0,
        end_ns: ack,
    };
    let during = Window {
        start_ns: ack,
        end_ns: end,
    };
    let base_mem = mem_window(&mem, base, ack);
    let during_mem = mem_window(&mem, during, ack);
    let after_mem = mem_window(
        &mem,
        Window {
            start_ns: end,
            end_ns: stop_ns,
        },
        ack,
    );
    let used_before = parse_u64(&memory_before, "used_memory")?;
    let peak = during_mem["max_used"].as_u64();
    let record = json!({
        "tool": "rexyra-bench",
        "tool_version": env!("CARGO_PKG_VERSION"),
        "scenario": spec.name,
        "label": o.label,
        "run": run,
        "started_unix": started_unix,
        "spec": spec,
        "params": o.params(),
        "server_exe": exe_info(&o.server),
        "dataset": dataset,
        "time_zero": "all *_ms times are relative to the moment the BGSAVE reply was read",
        "events_ms": {
            "bgsave_sent": rel_ms(bgsave_sent, ack),
            "save_end_detected": rel_ms(end, ack),
            "monitor_stop": rel_ms(stop_ns, ack),
        },
        "save": {
            "bgsave_reply": text,
            "duration_ms": rel_ms(end, ack),
            "duration_note": "from reading the BGSAVE reply to reading the first INFO reply (to a poll sent after it) with rdb_bgsave_in_progress:0; the only traffic is the INFO poll itself",
            "last_bgsave_status": status,
            "file_bytes": bgsave_bytes,
            "blocking_save_ms": save_ms,
            "blocking_save_file_bytes": save_bytes,
        },
        "memory": {
            "used_before": used_before,
            "rss_before": parse_u64(&memory_before, "used_memory_rss")?,
            "baseline_max_used": base_mem["max_used"],
            "peak_used_during": peak,
            "peak_rss_during": during_mem["max_rss"],
            "peak_used_minus_before": peak.map(|p| p as i64 - used_before as i64),
            "max_sample_interval_during_ms": during_mem["max_sample_interval_ms"],
            "after_last_used": after_mem["last_used"],
        },
        "verify": {
            "checked_file": "bgsave.rdb (the BGSAVE output, renamed before SAVE)",
            "dbsize_at_bgsave": dbsize_at_bgsave,
            "dbsize_reloaded": dbsize_reloaded,
            "dbsize_match": dbsize_at_bgsave == dbsize_reloaded,
            "reload_ready_seconds": reload_time.as_secs_f64(),
        },
        "memory_samples_columns": ["send_ms", "recv_ms", "used_memory", "used_memory_rss", "rdb_bgsave_in_progress"],
        "memory_samples": mem_series(&mem, ack),
    });
    if !o.keep_data {
        fs::remove_dir_all(&dir)?;
    }
    Ok(record)
}
