//! rexyra-fanout: a Pub/Sub fan-out benchmark.
//!
//! One publisher sends messages to one channel as fast as the server takes
//! them (pipelined), while N subscribers receive them. Every message carries
//! its sequence number and the time it was sent, so each subscriber checks
//! that it got every message, in order, and measures how long delivery took.
//! Meanwhile one ordinary client sends `GET`s one at a time, to show what the
//! fan-out costs everybody else. Works against any Redis-compatible server.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

const USAGE: &str = "\
usage: rexyra-fanout [--port 6379] [--subscribers 1000] [--seconds 20] [--size 64]
                     [--depth 32] [--rate 0] [--password-env VAR] [--label TEXT] [--out FILE]

--rate is published messages per second, 0 = as fast as the server takes them.";

const CHANNEL: &str = "fanout:bench";
/// Every n-th message's delivery latency is recorded by each subscriber.
const SAMPLE_EVERY: u64 = 16;

struct Options {
    port: u16,
    subscribers: usize,
    seconds: u64,
    size: usize,
    depth: usize,
    rate: u64,
    password: Option<String>,
    label: String,
    out: Option<String>,
}

fn parse() -> io::Result<Options> {
    let mut o = Options {
        port: 6379,
        subscribers: 1000,
        seconds: 20,
        size: 64,
        depth: 32,
        rate: 0,
        password: None,
        label: String::new(),
        out: None,
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut it = args.iter();
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .ok_or_else(|| io::Error::other(format!("{flag} needs a value\n{USAGE}")))?;
        let num = |v: &str| -> io::Result<u64> {
            v.parse()
                .map_err(|_| io::Error::other(format!("{flag}: bad number {v:?}")))
        };
        match flag.as_str() {
            "--port" => o.port = num(value)? as u16,
            "--subscribers" => o.subscribers = num(value)? as usize,
            "--seconds" => o.seconds = num(value)?,
            "--size" => o.size = (num(value)? as usize).max(40),
            "--depth" => o.depth = (num(value)? as usize).max(1),
            "--rate" => o.rate = num(value)?,
            "--password-env" => {
                o.password = Some(std::env::var(value).map_err(|_| {
                    io::Error::other(format!("environment variable {value} is not set"))
                })?)
            }
            "--label" => o.label = value.clone(),
            "--out" => o.out = Some(value.clone()),
            _ => return Err(io::Error::other(format!("unknown option {flag}\n{USAGE}"))),
        }
    }
    Ok(o)
}

fn command(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
    out
}

async fn connect(o: &Options) -> io::Result<BufReader<TcpStream>> {
    let stream = TcpStream::connect(("127.0.0.1", o.port)).await?;
    stream.set_nodelay(true)?;
    let mut conn = BufReader::with_capacity(256 * 1024, stream);
    if let Some(pw) = &o.password {
        conn.get_mut()
            .write_all(&command(&[b"AUTH", pw.as_bytes()]))
            .await?;
        let line = read_line(&mut conn).await?;
        if !line.starts_with(b"+OK") {
            return Err(io::Error::other("AUTH failed"));
        }
    }
    Ok(conn)
}

async fn read_line(conn: &mut BufReader<TcpStream>) -> io::Result<Vec<u8>> {
    let mut line = Vec::with_capacity(64);
    if conn.read_until(b'\n', &mut line).await? == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "connection closed",
        ));
    }
    Ok(line)
}

fn number(line: &[u8]) -> io::Result<i64> {
    std::str::from_utf8(&line[1..line.len().saturating_sub(2)])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| io::Error::other(format!("bad reply {:?}", String::from_utf8_lossy(line))))
}

/// Reads one bulk string (after its `$len` line).
async fn read_bulk(conn: &mut BufReader<TcpStream>) -> io::Result<Vec<u8>> {
    let header = read_line(conn).await?;
    let len = number(&header)? as usize;
    let mut data = vec![0u8; len + 2];
    conn.read_exact(&mut data).await?;
    data.truncate(len);
    Ok(data)
}

fn now_ns(origin: Instant) -> u64 {
    origin.elapsed().as_nanos() as u64
}

#[derive(Default)]
struct SubResult {
    received: u64,
    out_of_order: u64,
    disconnected: bool,
    latencies_ns: Vec<u64>,
}

async fn subscriber(
    o: Arc<Options>,
    origin: Instant,
    ready: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    expected: Arc<AtomicU64>,
) -> SubResult {
    let mut r = SubResult::default();
    let Ok(mut conn) = connect(&o).await else {
        r.disconnected = true;
        return r;
    };
    if conn
        .get_mut()
        .write_all(&command(&[b"SUBSCRIBE", CHANNEL.as_bytes()]))
        .await
        .is_err()
    {
        r.disconnected = true;
        return r;
    }
    // The confirmation: *3, subscribe, channel, count.
    let confirmed = async {
        read_line(&mut conn).await?;
        read_bulk(&mut conn).await?;
        read_bulk(&mut conn).await?;
        read_line(&mut conn).await
    };
    if confirmed.await.is_err() {
        r.disconnected = true;
        return r;
    }
    ready.fetch_add(1, Relaxed);
    let mut last_seq: Option<u64> = None;
    let mut idle_since: Option<Instant> = None;
    loop {
        // Once the publisher has stopped, read until everything it sent has
        // arrived, or nothing more has come for 10 seconds.
        if stop.load(Relaxed) && r.received >= expected.load(Relaxed) {
            break;
        }
        // Wait for data with a timeout, without ever abandoning a message
        // half read: `fill_buf` keeps what it has read in the buffer.
        match tokio::time::timeout(Duration::from_millis(200), conn.fill_buf()).await {
            Err(_) => {
                let idle = *idle_since.get_or_insert_with(Instant::now);
                if stop.load(Relaxed) && idle.elapsed() > Duration::from_secs(10) {
                    break;
                }
                continue;
            }
            Ok(Ok(buf)) if !buf.is_empty() => idle_since = None,
            Ok(_) => {
                r.disconnected = true;
                break;
            }
        }
        let frame = async {
            read_line(&mut conn).await?; // *3
            read_bulk(&mut conn).await?; // message
            read_bulk(&mut conn).await?; // channel
            read_bulk(&mut conn).await // payload
        };
        let Ok(payload) = frame.await else {
            r.disconnected = true;
            break;
        };
        let arrived = now_ns(origin);
        // Payload: "<seq>:<sent ns>:" then padding.
        let text = String::from_utf8_lossy(&payload);
        let mut parts = text.split(':');
        let seq: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
        let sent: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(arrived);
        if last_seq.is_some_and(|last| seq != last + 1) {
            r.out_of_order += 1;
        }
        last_seq = Some(seq);
        r.received += 1;
        if seq.is_multiple_of(SAMPLE_EVERY) {
            r.latencies_ns.push(arrived.saturating_sub(sent));
        }
    }
    r
}

/// Publishes until `seconds` have passed; returns (messages, total receivers
/// reported by the server, the first message's receivers).
async fn publisher(o: Arc<Options>, origin: Instant) -> io::Result<(u64, u64, Vec<u64>)> {
    let mut conn = connect(&o).await?;
    let pad = "x".repeat(o.size);
    let end = Instant::now() + Duration::from_secs(o.seconds);
    let mut seq = 0u64;
    let mut receivers_total = 0u64;
    let mut short = Vec::new();
    let started = Instant::now();
    while Instant::now() < end {
        let mut batch = Vec::with_capacity(o.depth * (o.size + 64));
        for _ in 0..o.depth {
            let mut payload = format!("{seq}:{}:", now_ns(origin));
            payload.push_str(&pad[..o.size.saturating_sub(payload.len())]);
            batch.extend_from_slice(&command(&[
                b"PUBLISH",
                CHANNEL.as_bytes(),
                payload.as_bytes(),
            ]));
            seq += 1;
        }
        conn.get_mut().write_all(&batch).await?;
        for i in 0..o.depth {
            let n = number(&read_line(&mut conn).await?)? as u64;
            receivers_total += n;
            if n < o.subscribers as u64 && short.len() < 20 {
                short.push(seq - o.depth as u64 + i as u64);
            }
        }
        if o.rate > 0 {
            let due = started + Duration::from_secs_f64(seq as f64 / o.rate as f64);
            tokio::time::sleep_until(due.into()).await;
        }
    }
    Ok((seq, receivers_total, short))
}

/// Times `GET` round trips one at a time until `stop`.
async fn probe(o: Arc<Options>, stop: Arc<AtomicBool>) -> io::Result<Vec<u64>> {
    let mut conn = connect(&o).await?;
    let get = command(&[b"GET", b"fanout:probe"]);
    let mut samples = Vec::new();
    while !stop.load(Relaxed) {
        let t = Instant::now();
        conn.get_mut().write_all(&get).await?;
        read_line(&mut conn).await?;
        samples.push(t.elapsed().as_nanos() as u64);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    Ok(samples)
}

/// `used_memory_rss` in bytes, sampled every 200 ms until `stop`.
async fn memory(o: Arc<Options>, stop: Arc<AtomicBool>) -> io::Result<Vec<u64>> {
    let mut conn = connect(&o).await?;
    let info = command(&[b"INFO", b"memory"]);
    let mut samples = Vec::new();
    while !stop.load(Relaxed) {
        conn.get_mut().write_all(&info).await?;
        let text = String::from_utf8_lossy(&read_bulk(&mut conn).await?).into_owned();
        if let Some(rss) = text
            .lines()
            .find_map(|l| l.strip_prefix("used_memory_rss:"))
            .and_then(|v| v.trim().parse().ok())
        {
            samples.push(rss);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(samples)
}

fn percentiles_ms(mut ns: Vec<u64>) -> Value {
    if ns.is_empty() {
        return Value::Null;
    }
    ns.sort_unstable();
    let at = |p: f64| {
        let i = ((p * ns.len() as f64).ceil() as usize).clamp(1, ns.len()) - 1;
        (ns[i] as f64 / 1e4).round() / 100.0
    };
    json!({"samples": ns.len(), "p50": at(0.5), "p99": at(0.99), "p999": at(0.999), "max": at(1.0)})
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let o = Arc::new(parse()?);
    let origin = Instant::now();
    let ready = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let expected = Arc::new(AtomicU64::new(u64::MAX));

    let mut subs = Vec::with_capacity(o.subscribers);
    for _ in 0..o.subscribers {
        subs.push(tokio::spawn(subscriber(
            o.clone(),
            origin,
            ready.clone(),
            stop.clone(),
            expected.clone(),
        )));
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    while ready.load(Relaxed) < o.subscribers as u64 {
        if Instant::now() > deadline {
            return Err(io::Error::other(format!(
                "only {} of {} subscribers connected",
                ready.load(Relaxed),
                o.subscribers
            )));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let probe_stop = Arc::new(AtomicBool::new(false));
    let probe_task = tokio::spawn(probe(o.clone(), probe_stop.clone()));
    let mem_task = tokio::spawn(memory(o.clone(), probe_stop.clone()));
    let started = Instant::now();
    let (published, receivers_total, short) = publisher(o.clone(), origin).await?;
    let publish_secs = started.elapsed().as_secs_f64();
    expected.store(published, Relaxed);
    stop.store(true, Relaxed);

    let mut received = 0u64;
    let mut out_of_order = 0u64;
    let mut disconnected = 0u64;
    let mut incomplete = 0u64;
    let mut latencies = Vec::new();
    for task in subs {
        let r = task.await.unwrap_or_default();
        received += r.received;
        out_of_order += r.out_of_order;
        disconnected += u64::from(r.disconnected);
        incomplete += u64::from(r.received < published);
        latencies.extend(r.latencies_ns);
    }
    let drain_secs = started.elapsed().as_secs_f64() - publish_secs;
    probe_stop.store(true, Relaxed);
    let probe_samples = probe_task.await.map_err(io::Error::other)??;
    let rss = mem_task.await.map_err(io::Error::other)??;

    let summary = json!({
        "label": o.label,
        "subscribers": o.subscribers,
        "message_bytes": o.size,
        "pipeline_depth": o.depth,
        "rate_limit": o.rate,
        "publish_seconds": (publish_secs * 100.0).round() / 100.0,
        "drain_seconds": (drain_secs * 100.0).round() / 100.0,
        "published": published,
        "published_per_sec": (published as f64 / publish_secs).round(),
        "deliveries_expected": published * o.subscribers as u64,
        "deliveries_received": received,
        "deliveries_per_sec": (received as f64 / (publish_secs + drain_secs)).round(),
        "receivers_reported_by_publish": receivers_total,
        "publishes_with_fewer_receivers": short,
        "subscribers_disconnected": disconnected,
        "subscribers_missing_messages": incomplete,
        "out_of_order": out_of_order,
        "delivery_latency_ms": percentiles_ms(latencies),
        "get_latency_ms_meanwhile": percentiles_ms(probe_samples),
        "server_rss_mib_peak": rss.iter().max().map(|b| b >> 20),
        "server_rss_mib_first": rss.first().map(|b| b >> 20),
    });
    let text = serde_json::to_string_pretty(&summary).unwrap_or_default();
    println!("{text}");
    if let Some(path) = &o.out {
        std::fs::write(path, text)?;
    }
    Ok(())
}
