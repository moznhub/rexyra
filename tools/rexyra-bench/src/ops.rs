//! `rexyra-bench ops`: the throughput and latency of one command at a time
//! (`SET`, `GET`, `LPUSH`) on a fresh server, to see what a change to the
//! command path costs. For example, the cost of keyspace notifications:
//!
//! ```text
//! rexyra-bench ops --label off
//! rexyra-bench ops --label on-unheard --notify KEA
//! ```
//!
//! Each run starts its own server (automatic saves off), optionally sets
//! `notify-keyspace-events`, and then, for each command in turn, has
//! `--clients` connections send pipelines of `--pipeline` commands for
//! `--warmup-secs` plus `--seconds`. Only the batches completed after the
//! warm-up count. Latency is per pipeline, from just before it is written to
//! after its last reply is read.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use crate::resp::{encode, Conn, Reply};
use crate::server::ServerProc;

pub struct Options {
    port: u16,
    server: PathBuf,
    data_root: PathBuf,
    clients: usize,
    pipeline: usize,
    warmup: Duration,
    seconds: Duration,
    runs: u32,
    commands: Vec<String>,
    notify: String,
    label: String,
}

fn other(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

impl Options {
    pub fn parse(args: &[String]) -> io::Result<Options> {
        let exe_dir = std::env::current_exe()?
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default();
        let target_dir = exe_dir.parent().map(Path::to_path_buf).unwrap_or_default();
        let mut o = Options {
            port: 6400,
            server: exe_dir.join("rexyra-server.exe"),
            data_root: target_dir.join("bench-data"),
            clients: 50,
            pipeline: 16,
            warmup: Duration::from_secs(1),
            seconds: Duration::from_secs(5),
            runs: 3,
            commands: vec!["SET".into(), "GET".into(), "LPUSH".into()],
            notify: String::new(),
            label: "ops".into(),
        };
        let mut it = args.iter();
        while let Some(flag) = it.next() {
            let value = it
                .next()
                .ok_or_else(|| other(format!("{flag} needs a value")))?;
            let number = |value: &str| -> io::Result<u64> {
                value
                    .parse()
                    .map_err(|_| other(format!("{flag}: cannot parse {value:?}")))
            };
            match flag.as_str() {
                "--port" => o.port = number(value)? as u16,
                "--server" => o.server = PathBuf::from(value),
                "--data-root" => o.data_root = PathBuf::from(value),
                "--clients" => o.clients = number(value)?.max(1) as usize,
                "--pipeline" => o.pipeline = number(value)?.max(1) as usize,
                "--warmup-secs" => o.warmup = Duration::from_secs(number(value)?),
                "--seconds" => o.seconds = Duration::from_secs(number(value)?.max(1)),
                "--runs" => o.runs = number(value)?.max(1) as u32,
                "--commands" => {
                    o.commands = value.split(',').map(|c| c.to_ascii_uppercase()).collect();
                    if let Some(bad) = o
                        .commands
                        .iter()
                        .find(|c| !matches!(c.as_str(), "SET" | "GET" | "LPUSH"))
                    {
                        return Err(other(format!("--commands: {bad} is not SET, GET or LPUSH")));
                    }
                }
                "--notify" => o.notify = value.clone(),
                "--label" => o.label = value.clone(),
                _ => return Err(other(format!("unknown option {flag}"))),
            }
        }
        Ok(o)
    }
}

/// The pipeline one client sends, over and over.
fn batch(command: &str, client: usize, pipeline: usize) -> Vec<u8> {
    let value = [b'v'; 64];
    let mut buf = Vec::new();
    for i in 0..pipeline {
        let key = format!("key:{client}:{i}");
        match command {
            "SET" => encode(&mut buf, &[b"SET", key.as_bytes(), &value]),
            "GET" => encode(&mut buf, &[b"GET", key.as_bytes()]),
            _ => encode(
                &mut buf,
                &[b"LPUSH", format!("list:{client}").as_bytes(), &value[..16]],
            ),
        }
    }
    buf
}

struct Measured {
    ops_per_sec: f64,
    p50_ms: f64,
    p99_ms: f64,
}

fn measure(o: &Options, command: &str) -> io::Result<Measured> {
    let start = Arc::new(Barrier::new(o.clients + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let mut workers = Vec::new();
    for client in 0..o.clients {
        let (start, stop) = (Arc::clone(&start), Arc::clone(&stop));
        let (port, pipeline) = (o.port, o.pipeline);
        let request = batch(command, client, pipeline);
        workers.push(thread::spawn(move || -> io::Result<Vec<(Instant, u64)>> {
            let mut conn = Conn::connect(port)?;
            conn.set_timeout(Some(Duration::from_secs(30)))?;
            start.wait();
            let mut done = Vec::new();
            while !stop.load(Relaxed) {
                let sent = Instant::now();
                conn.send(&request)?;
                for _ in 0..pipeline {
                    if let Reply::Error(e) = conn.read()? {
                        return Err(other(format!("error reply: {e}")));
                    }
                }
                let end = Instant::now();
                done.push((end, (end - sent).as_nanos() as u64));
            }
            Ok(done)
        }));
    }
    start.wait();
    let origin = Instant::now();
    thread::sleep(o.warmup + o.seconds);
    stop.store(true, Relaxed);
    let from = origin + o.warmup;
    let to = from + o.seconds;
    let mut latencies = Vec::new();
    for worker in workers {
        let done = worker
            .join()
            .map_err(|_| other("a client thread panicked"))??;
        latencies.extend(
            done.into_iter()
                .filter(|(end, _)| *end >= from && *end < to)
                .map(|(_, lat)| lat),
        );
    }
    if latencies.is_empty() {
        return Err(other("no batch completed in the measured window"));
    }
    latencies.sort_unstable();
    let rank = |p: f64| {
        latencies[((latencies.len() as f64 * p).ceil() as usize).clamp(1, latencies.len()) - 1]
    };
    Ok(Measured {
        ops_per_sec: (latencies.len() * o.pipeline) as f64 / o.seconds.as_secs_f64(),
        p50_ms: rank(0.50) as f64 / 1e6,
        p99_ms: rank(0.99) as f64 / 1e6,
    })
}

pub fn run(o: &Options) -> io::Result<()> {
    std::fs::create_dir_all(&o.data_root)?;
    println!(
        "label\trun\tcommand\tops/s\tp50 ms\tp99 ms\t({} clients, pipeline {}, {} s, notify-keyspace-events {:?})",
        o.clients,
        o.pipeline,
        o.seconds.as_secs(),
        o.notify
    );
    for run in 1..=o.runs {
        let dir = o.data_root.join(format!("ops-{}-{run}", o.label));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        let log = o
            .data_root
            .join(format!("ops-{}-{run}.server.log", o.label));
        let mut server = ServerProc::start(&o.server, o.port, &dir, None, &log)?;
        server.wait_ready(Duration::from_secs(30))?;
        if !o.notify.is_empty() {
            let mut control = Conn::connect(o.port)?;
            let reply =
                control.call_str(&["CONFIG", "SET", "notify-keyspace-events", &o.notify])?;
            if reply.is_error() {
                return Err(other(format!("CONFIG SET: {}", reply.short())));
            }
        }
        for command in &o.commands {
            let m = measure(o, command)?;
            println!(
                "{}\t{run}\t{command}\t{:.0}\t{:.3}\t{:.3}",
                o.label, m.ops_per_sec, m.p50_ms, m.p99_ms
            );
        }
        server.shutdown()?;
        let _ = std::fs::remove_dir_all(&dir);
    }
    Ok(())
}
