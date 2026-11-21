//! Loading a scenario's dataset, plus the small helpers the traffic shares.

use crate::resp::{encode, write_header, Conn, Reply};
use crate::scenario::Spec;
use std::io;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// xorshift64*: fast, deterministic per seed.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// `len` printable pseudo-random bytes (the RDB writer does not compress,
/// but this keeps the data from being trivially compressible anyway).
pub fn pattern(len: usize, seed: u64) -> Vec<u8> {
    let mut rng = Rng::new(seed);
    (0..len).map(|_| b'!' + (rng.next() % 90) as u8).collect()
}

/// RESP for `SET key <len bytes>` up to the value itself.
pub fn set_header(key: &[u8], len: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(key.len() + 40);
    write_header(&mut buf, b'*', 3);
    write_header(&mut buf, b'$', 3);
    buf.extend_from_slice(b"SET\r\n");
    write_header(&mut buf, b'$', key.len());
    buf.extend_from_slice(key);
    buf.extend_from_slice(b"\r\n");
    write_header(&mut buf, b'$', len);
    buf
}

pub fn expect_ok(reply: &Reply) -> io::Result<()> {
    match reply {
        Reply::Simple(s) if s == "OK" => Ok(()),
        other => Err(io::Error::other(format!(
            "expected OK, got {}",
            other.short()
        ))),
    }
}

fn connect(port: u16) -> io::Result<Conn> {
    let conn = Conn::connect(port)?;
    conn.set_timeout(Some(Duration::from_secs(1800)))?;
    Ok(conn)
}

fn load_strings(
    port: u16,
    prefix: &'static str,
    lo: usize,
    hi: usize,
    size: usize,
    fields: usize,
    seed: u64,
) -> io::Result<()> {
    let mut conn = connect(port)?;
    let value = pattern(size, seed);
    if fields > 0 {
        // Small hashes: one `HSET key f0 v f1 v ...` per key, pipelined.
        let names: Vec<String> = (0..fields).map(|j| format!("f{j}")).collect();
        let mut buf = Vec::new();
        let mut i = lo;
        while i < hi {
            let end = (i + 1000).min(hi);
            buf.clear();
            for k in i..end {
                let key = format!("{prefix}{k}");
                let mut args: Vec<&[u8]> = vec![b"HSET", key.as_bytes()];
                for name in &names {
                    args.push(name.as_bytes());
                    args.push(&value);
                }
                encode(&mut buf, &args);
            }
            conn.send(&buf)?;
            for _ in i..end {
                let added = conn.read()?.int()?;
                if added != fields as i64 {
                    return Err(io::Error::other(format!(
                        "HSET added {added} fields, expected {fields}"
                    )));
                }
            }
            i = end;
        }
    } else if size >= 64 * 1024 {
        for i in lo..hi {
            let key = format!("{prefix}{i}");
            conn.send(&set_header(key.as_bytes(), size))?;
            conn.send(&value)?;
            conn.send(b"\r\n")?;
            expect_ok(&conn.read()?)?;
        }
    } else {
        let mut buf = Vec::new();
        let mut i = lo;
        while i < hi {
            let end = (i + 1000).min(hi);
            buf.clear();
            for k in i..end {
                let key = format!("{prefix}{k}");
                encode(&mut buf, &[b"SET", key.as_bytes(), &value]);
            }
            conn.send(&buf)?;
            for _ in i..end {
                expect_ok(&conn.read()?)?;
            }
            i = end;
        }
    }
    Ok(())
}

fn spawn_strings(
    port: u16,
    prefix: &'static str,
    count: usize,
    size: usize,
    fields: usize,
    conns: usize,
) -> Vec<JoinHandle<io::Result<()>>> {
    let conns = conns.min(count).max(1);
    let chunk = count.div_ceil(conns);
    (0..conns)
        .map(|c| {
            let lo = c * chunk;
            let hi = ((c + 1) * chunk).min(count);
            thread::spawn(move || load_strings(port, prefix, lo, hi, size, fields, c as u64 + 1))
        })
        .collect()
}

/// `RPUSH key e0 e1 ...` in commands of 10,000 elements.
fn load_list(port: u16, key: String, len: usize) -> io::Result<()> {
    let mut conn = connect(port)?;
    let mut j = 0;
    while j < len {
        let end = (j + 10_000).min(len);
        let elems: Vec<String> = (j..end).map(|k| format!("e{k}")).collect();
        let mut args: Vec<&[u8]> = vec![b"RPUSH", key.as_bytes()];
        args.extend(elems.iter().map(|e| e.as_bytes()));
        let n = conn.call(&args)?.int()?;
        if n != end as i64 {
            return Err(io::Error::other(format!(
                "RPUSH {key}: length {n}, expected {end}"
            )));
        }
        j = end;
    }
    Ok(())
}

/// `HSET key f0 v0 f1 v1 ...` in commands of 5,000 fields.
fn load_hash(port: u16, key: String, fields: usize) -> io::Result<()> {
    let mut conn = connect(port)?;
    let mut j = 0;
    while j < fields {
        let end = (j + 5_000).min(fields);
        let pairs: Vec<(String, String)> = (j..end)
            .map(|k| (format!("f{k}"), format!("v{k}")))
            .collect();
        let mut args: Vec<&[u8]> = vec![b"HSET", key.as_bytes()];
        for (f, v) in &pairs {
            args.push(f.as_bytes());
            args.push(v.as_bytes());
        }
        let n = conn.call(&args)?.int()?;
        if n != (end - j) as i64 {
            return Err(io::Error::other(format!(
                "HSET {key}: added {n}, expected {}",
                end - j
            )));
        }
        j = end;
    }
    Ok(())
}

/// Loads everything the scenario needs, in parallel connections.
pub fn load_dataset(spec: &Spec, port: u16) -> io::Result<()> {
    let mut handles = Vec::new();
    if spec.small_keys > 0 {
        handles.extend(spawn_strings(
            port,
            "s:",
            spec.small_keys,
            spec.small_value,
            spec.small_fields,
            8,
        ));
    }
    if spec.big_keys > 0 {
        handles.extend(spawn_strings(
            port,
            "big:",
            spec.big_keys,
            spec.big_size,
            0,
            4,
        ));
    }
    for i in 0..spec.lists {
        let len = spec.list_len;
        handles.push(thread::spawn(move || {
            load_list(port, format!("list:{i}"), len)
        }));
    }
    for i in 0..spec.hashes {
        let fields = spec.hash_fields;
        handles.push(thread::spawn(move || {
            load_hash(port, format!("hash:{i}"), fields)
        }));
    }
    let mut first_error = None;
    for handle in handles {
        let outcome = handle
            .join()
            .unwrap_or_else(|_| Err(io::Error::other("a loader thread panicked")));
        if let Err(e) = outcome {
            first_error.get_or_insert(e);
        }
    }
    first_error.map_or(Ok(()), Err)
}
