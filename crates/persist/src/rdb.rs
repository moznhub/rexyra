//! The RDB snapshot format.
//!
//! A file is a header (`REDIS` and a four-digit version), a sequence of
//! records each introduced by a one-byte opcode or value type, and an
//! end-of-file marker followed by a CRC-64 of everything before it.
//!
//! **Writing** uses the plain, long-standing encodings (a list is a count
//! followed by its elements, and so on). Every Redis version can load them,
//! and converts them to its compact in-memory forms as it does. A hash with
//! field TTLs is written in the form Redis 7.4 introduced for it (type 24,
//! a hash with metadata), which needs RDB version 12: see
//! [`Writer::with_version`] and [`needs_version`].
//!
//! **Reading** also understands the compact encodings Redis 7 writes
//! (listpack, intset, quicklist, and Redis 7.4's listpack of hash fields
//! with TTLs) and LZF-compressed strings, so a `dump.rdb` produced by Redis
//! loads here. Types this server does not have (streams, modules) are
//! reported as unsupported. Hash fields whose TTL has passed are read like
//! the others; dropping them is up to the caller, who knows the time
//! ([`Hash::drop_expired`]).

use std::collections::VecDeque;
use std::fmt;
use std::io::{self, Write};

use bytes::Bytes;
use rexyra_engine::{Hash, Set, Value, ZSet};

use crate::{crc64, lzf};

/// The RDB version written. Redis 7.0 and later read it.
pub const VERSION: u16 = 11;
/// The RDB version written when there are hash field TTLs, which only
/// Redis 7.4 and later read.
pub const VERSION_FIELD_TTLS: u16 = 12;
/// The newest RDB version this reader accepts (Redis 7.4).
const MAX_VERSION: u16 = 12;

const OP_FUNCTION: u8 = 0xF5;
const OP_FREQ: u8 = 0xF8;
const OP_IDLE: u8 = 0xF9;
const OP_AUX: u8 = 0xFA;
const OP_RESIZE_DB: u8 = 0xFB;
const OP_EXPIRE_MS: u8 = 0xFC;
const OP_EXPIRE_SECONDS: u8 = 0xFD;
const OP_SELECT_DB: u8 = 0xFE;
const OP_EOF: u8 = 0xFF;

/// The longest string a value may hold, Redis's `proto-max-bulk-len`. A
/// compressed string that claims to expand to more is refused before any
/// memory is set aside for it.
const MAX_STRING_LEN: usize = 512 * 1024 * 1024;
/// The most elements a collection has room reserved for before they are
/// read. The count comes from the file or payload; past this the collection
/// grows as its elements actually arrive.
const MAX_RESERVED: usize = 1024;

const TYPE_STRING: u8 = 0;
const TYPE_LIST: u8 = 1;
const TYPE_SET: u8 = 2;
const TYPE_ZSET: u8 = 3;
const TYPE_HASH: u8 = 4;
const TYPE_ZSET_2: u8 = 5;
const TYPE_SET_INTSET: u8 = 11;
const TYPE_HASH_LISTPACK: u8 = 16;
const TYPE_ZSET_LISTPACK: u8 = 17;
const TYPE_LIST_QUICKLIST_2: u8 = 18;
const TYPE_SET_LISTPACK: u8 = 20;
/// A hash with field TTLs: the earliest TTL, then each field with its TTL
/// relative to that one.
const TYPE_HASH_METADATA: u8 = 24;
/// A hash with field TTLs as a listpack of field, value, TTL triplets.
const TYPE_HASH_LISTPACK_EX: u8 = 25;

#[derive(Debug)]
pub enum RdbError {
    Io(io::Error),
    /// The data does not start with `REDIS`.
    NotRdb,
    UnsupportedVersion(u16),
    /// The data ends in the middle of a record.
    Truncated,
    BadChecksum,
    /// Valid RDB, but something this server cannot represent.
    Unsupported(String),
    Corrupt(&'static str),
}

impl fmt::Display for RdbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RdbError::Io(error) => write!(f, "{error}"),
            RdbError::NotRdb => f.write_str("not an RDB file (wrong signature)"),
            RdbError::UnsupportedVersion(version) => {
                write!(f, "RDB format version {version} is not supported")
            }
            RdbError::Truncated => f.write_str("unexpected end of RDB data"),
            RdbError::BadChecksum => f.write_str("wrong RDB checksum"),
            RdbError::Unsupported(what) => write!(f, "unsupported RDB content: {what}"),
            RdbError::Corrupt(what) => write!(f, "corrupt RDB data: {what}"),
        }
    }
}

impl std::error::Error for RdbError {}

impl From<io::Error> for RdbError {
    fn from(error: io::Error) -> Self {
        RdbError::Io(error)
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

fn put_length(out: &mut Vec<u8>, length: u64) {
    match length {
        0..=0x3F => out.push(length as u8),
        0x40..=0x3FFF => out.extend_from_slice(&[0x40 | (length >> 8) as u8, length as u8]),
        0x4000..=0xFFFF_FFFF => {
            out.push(0x80);
            out.extend_from_slice(&(length as u32).to_be_bytes());
        }
        _ => {
            out.push(0x81);
            out.extend_from_slice(&length.to_be_bytes());
        }
    }
}

fn put_string(out: &mut Vec<u8>, bytes: &[u8]) {
    put_length(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// Appends the type byte and encoded body of `value`.
fn put_value(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::String(text) => {
            out.push(TYPE_STRING);
            put_string(out, text);
        }
        Value::List(list) => {
            out.push(TYPE_LIST);
            put_length(out, list.len() as u64);
            for element in list {
                put_string(out, element);
            }
        }
        Value::Set(set) => {
            out.push(TYPE_SET);
            put_length(out, set.len() as u64);
            set.for_each(|member| put_string(out, member));
        }
        Value::Hash(hash) if hash.is_compact() && hash.next_field_expiry().is_some() => {
            // Type, then (once the key is spliced in after the type byte)
            // the earliest TTL and the listpack.
            out.push(TYPE_HASH_LISTPACK_EX);
            put_listpack_ex(out, hash);
        }
        Value::Hash(hash) => {
            // Type, then (once the key is spliced in after the type byte)
            // the earliest TTL if any, the count and the fields.
            let min = put_hash_header(out, hash);
            put_length(out, hash.len() as u64);
            for (field, value) in hash.iter_stored() {
                put_hash_field(out, hash, min, field, value);
            }
        }
        Value::ZSet(zset) => {
            out.push(TYPE_ZSET_2);
            put_length(out, zset.len() as u64);
            for (member, score) in zset.iter() {
                put_string(out, member);
                out.extend_from_slice(&score.to_le_bytes());
            }
        }
    }
}

/// Appends a small hash with field TTLs in the form Redis 7.4 keeps it in
/// memory and writes it (type 25): the earliest TTL, then a listpack of
/// field, value and TTL (zero for none) for each field, in the hash's own
/// order, which is the one Redis requires: fields with a TTL first, by
/// time.
fn put_listpack_ex(out: &mut Vec<u8>, hash: &Hash) {
    let min = hash.next_field_expiry().unwrap_or(0);
    out.extend_from_slice(&min.to_le_bytes());
    let mut entries = Vec::new();
    let mut count = 0usize;
    for (field, value) in hash.iter_stored() {
        put_listpack_value(&mut entries, field);
        put_listpack_value(&mut entries, value);
        let at = hash.field_expire_at(field).unwrap_or(0);
        put_listpack_integer(&mut entries, at as i64);
        count += 3;
    }
    // Six bytes of header (total size, element count), the entries, and
    // the end marker.
    let total = 6 + entries.len() + 1;
    let mut blob = Vec::with_capacity(total);
    blob.extend_from_slice(&(total as u32).to_le_bytes());
    blob.extend_from_slice(&(count.min(usize::from(u16::MAX)) as u16).to_le_bytes());
    blob.extend_from_slice(&entries);
    blob.push(0xFF);
    put_string(out, &blob);
}

/// Appends one listpack entry: an integer if `text` is one written the
/// usual way (as Redis stores it), otherwise a string.
fn put_listpack_value(out: &mut Vec<u8>, text: &[u8]) {
    match canonical_integer(text) {
        Some(number) => put_listpack_integer(out, number),
        None => put_listpack_string(out, text),
    }
}

/// `text` as a 64-bit integer, if it is one in its shortest decimal form:
/// no sign but a minus, no leading zero, no "-0".
fn canonical_integer(text: &[u8]) -> Option<i64> {
    if text.is_empty() || text.len() > 20 {
        return None;
    }
    let number: i64 = std::str::from_utf8(text).ok()?.parse().ok()?;
    (number.to_string().as_bytes() == text).then_some(number)
}

/// Appends one listpack entry holding a string, followed by its length.
fn put_listpack_string(out: &mut Vec<u8>, text: &[u8]) {
    let start = out.len();
    match text.len() {
        len @ 0..=63 => out.push(0x80 | len as u8),
        len @ 64..=4095 => out.extend_from_slice(&[0xE0 | (len >> 8) as u8, len as u8]),
        len => {
            out.push(0xF0);
            out.extend_from_slice(&(len as u32).to_le_bytes());
        }
    }
    out.extend_from_slice(text);
    put_listpack_backlen(out, out.len() - start);
}

/// Appends one listpack entry holding an integer, in the smallest of the
/// integer encodings it fits.
fn put_listpack_integer(out: &mut Vec<u8>, number: i64) {
    let start = out.len();
    match number {
        0..=127 => out.push(number as u8),
        -4096..=4095 => {
            let raw = (number as u16) & 0x1FFF;
            out.extend_from_slice(&[0xC0 | (raw >> 8) as u8, raw as u8]);
        }
        -32_768..=32_767 => {
            out.push(0xF1);
            out.extend_from_slice(&(number as i16).to_le_bytes());
        }
        -8_388_608..=8_388_607 => {
            out.push(0xF2);
            out.extend_from_slice(&(number as i32).to_le_bytes()[..3]);
        }
        -2_147_483_648..=2_147_483_647 => {
            out.push(0xF3);
            out.extend_from_slice(&(number as i32).to_le_bytes());
        }
        _ => {
            out.push(0xF4);
            out.extend_from_slice(&number.to_le_bytes());
        }
    }
    put_listpack_backlen(out, out.len() - start);
}

/// The length of the entry just written, in the form that lets a listpack
/// be walked backwards: seven bits per byte, the first byte the most
/// significant, every byte but the first with the top bit set.
fn put_listpack_backlen(out: &mut Vec<u8>, length: usize) {
    let bytes = match length {
        0..=127 => 1,
        128..=16_382 => 2,
        16_383..=2_097_150 => 3,
        2_097_151..=268_435_454 => 4,
        _ => 5,
    };
    for index in (0..bytes).rev() {
        let group = ((length >> (7 * index)) & 0x7F) as u8;
        out.push(if index == bytes - 1 {
            group
        } else {
            group | 0x80
        });
    }
}

/// Appends the type byte of a hash and, if it has field TTLs, the earliest
/// of them, which in the file follows the key. Returns that time, which the
/// fields' TTLs are written relative to.
fn put_hash_header(out: &mut Vec<u8>, hash: &Hash) -> Option<u64> {
    let min = hash.next_field_expiry();
    match min {
        Some(min) => {
            out.push(TYPE_HASH_METADATA);
            out.extend_from_slice(&min.to_le_bytes());
        }
        None => out.push(TYPE_HASH),
    }
    min
}

/// Appends one field of a hash; `min` is what [`put_hash_header`] returned.
fn put_hash_field(out: &mut Vec<u8>, hash: &Hash, min: Option<u64>, field: &[u8], value: &[u8]) {
    if let Some(min) = min {
        // Zero for none, otherwise one more than the distance from the
        // earliest.
        let ttl = hash.field_expire_at(field).map_or(0, |at| at - min + 1);
        put_length(out, ttl);
    }
    put_string(out, field);
    put_string(out, value);
}

/// The RDB version a value needs: [`VERSION_FIELD_TTLS`] for a hash with
/// field TTLs, [`VERSION`] for everything else.
pub fn needs_version(value: &Value) -> u16 {
    match value {
        Value::Hash(hash) if hash.next_field_expiry().is_some() => VERSION_FIELD_TTLS,
        _ => VERSION,
    }
}

/// Strings at least this long are written without being copied first.
const STREAMED_STRING_BYTES: usize = 64 * 1024;

/// Writes an RDB file. Records must be written in the order the methods are
/// listed: auxiliary fields, functions, then each database's entries.
pub struct Writer<W: Write> {
    out: W,
    crc: u64,
    scratch: Vec<u8>,
}

impl<W: Write> Writer<W> {
    pub fn new(out: W) -> io::Result<Self> {
        Self::with_version(out, VERSION)
    }

    /// A writer whose file says it is in RDB format `version`:
    /// [`VERSION_FIELD_TTLS`] if any hash written may have field TTLs.
    pub fn with_version(out: W, version: u16) -> io::Result<Self> {
        let mut writer = Writer {
            out,
            crc: 0,
            scratch: Vec::new(),
        };
        writer.scratch = format!("REDIS{version:04}").into_bytes();
        writer.flush_scratch()?;
        Ok(writer)
    }

    fn flush_scratch(&mut self) -> io::Result<()> {
        self.crc = crc64::update(self.crc, &self.scratch);
        self.out.write_all(&self.scratch)?;
        self.scratch.clear();
        Ok(())
    }

    /// A piece of metadata about the file, such as the server version.
    pub fn aux(&mut self, name: &str, value: &str) -> io::Result<()> {
        self.scratch.push(OP_AUX);
        put_string(&mut self.scratch, name.as_bytes());
        put_string(&mut self.scratch, value.as_bytes());
        self.flush_scratch()
    }

    /// The source code of a function library.
    pub fn function(&mut self, code: &[u8]) -> io::Result<()> {
        self.scratch.push(OP_FUNCTION);
        put_string(&mut self.scratch, code);
        self.flush_scratch()
    }

    /// Starts the entries of database `index`, which holds `keys` keys of
    /// which `expiring` have a TTL.
    pub fn select_db(&mut self, index: usize, keys: usize, expiring: usize) -> io::Result<()> {
        self.scratch.push(OP_SELECT_DB);
        put_length(&mut self.scratch, index as u64);
        self.scratch.push(OP_RESIZE_DB);
        put_length(&mut self.scratch, keys as u64);
        put_length(&mut self.scratch, expiring as u64);
        self.flush_scratch()
    }

    /// One key of the current database. `expire_at` is in milliseconds since
    /// the Unix epoch.
    pub fn entry(&mut self, key: &[u8], value: &Value, expire_at: Option<u64>) -> io::Result<()> {
        match value {
            // A long string goes out as it is, after its header, rather
            // than by way of a second copy of it in memory.
            Value::String(text) if text.len() >= STREAMED_STRING_BYTES => {
                if let Some(at) = expire_at {
                    self.scratch.push(OP_EXPIRE_MS);
                    self.scratch.extend_from_slice(&at.to_le_bytes());
                }
                self.scratch.push(TYPE_STRING);
                put_string(&mut self.scratch, key);
                put_length(&mut self.scratch, text.len() as u64);
                self.flush_scratch()?;
                self.encoded(text)
            }
            _ => {
                encode_entry(&mut self.scratch, key, value, expire_at);
                self.flush_scratch()
            }
        }
    }

    /// One or more keys of the current database, already in the form
    /// [`encode_entry`] produces.
    pub fn encoded(&mut self, entries: &[u8]) -> io::Result<()> {
        self.crc = crc64::update(self.crc, entries);
        self.out.write_all(entries)
    }

    /// Writes the end marker and checksum and returns the underlying writer.
    pub fn finish(mut self) -> io::Result<W> {
        self.scratch.push(OP_EOF);
        self.flush_scratch()?;
        self.out.write_all(&self.crc.to_le_bytes())?;
        Ok(self.out)
    }
}

/// Appends one key to `out` in the form [`Writer::entry`] writes it, for
/// [`Writer::encoded`] to write later.
pub fn encode_entry(out: &mut Vec<u8>, key: &[u8], value: &Value, expire_at: Option<u64>) {
    if let Some(at) = expire_at {
        out.push(OP_EXPIRE_MS);
        out.extend_from_slice(&at.to_le_bytes());
    }
    // The type comes first, then the key, then the body.
    let start = out.len();
    put_value(out, value);
    let mut key_bytes = Vec::with_capacity(key.len() + 9);
    put_string(&mut key_bytes, key);
    out.splice(start + 1..start + 1, key_bytes);
}

/// How far a piecewise encoding of one entry has got; see [`encode_some`].
#[derive(Debug, Default)]
pub struct Progress {
    begun: bool,
    /// The next element of a list or sorted set, or the place in the table
    /// of a hash or set.
    next: u64,
    /// The time a hash's field TTLs are written relative to.
    min: Option<u64>,
}

/// Buckets of a hash or set looked at between checks of how much has been
/// encoded.
const PART_BUCKETS: usize = 64;

/// Appends the next part of one key to `out`, for a value too large to
/// encode in one go: the first call writes what precedes the elements, and
/// each call writes elements until about `max_elements` of them or
/// `max_bytes` bytes have been added. Returns `true` once the entry is
/// complete.
///
/// The value must not change between calls. The parts, joined in order, are
/// one entry for [`Writer::encoded`]: the same key and value as
/// [`encode_entry`] writes, though a hash's or set's elements may come in
/// another order.
pub fn encode_some(
    out: &mut Vec<u8>,
    key: &[u8],
    value: &Value,
    expire_at: Option<u64>,
    progress: &mut Progress,
    max_elements: usize,
    max_bytes: usize,
) -> bool {
    if !progress.begun {
        progress.begun = true;
        if let Some(at) = expire_at {
            out.push(OP_EXPIRE_MS);
            out.extend_from_slice(&at.to_le_bytes());
        }
        let (kind, len) = match value {
            Value::String(text) => {
                out.push(TYPE_STRING);
                put_string(out, key);
                put_string(out, text);
                return true;
            }
            Value::List(list) => (TYPE_LIST, list.len()),
            Value::Set(set) => (TYPE_SET, set.len()),
            Value::Hash(hash) if hash.is_compact() => {
                // Small: written whole.
                let start = out.len();
                put_value(out, value);
                let mut key_bytes = Vec::with_capacity(key.len() + 9);
                put_string(&mut key_bytes, key);
                out.splice(start + 1..start + 1, key_bytes);
                return true;
            }
            Value::Hash(hash) => {
                let mut header = Vec::with_capacity(9);
                let min = put_hash_header(&mut header, hash);
                out.push(header[0]);
                put_string(out, key);
                out.extend_from_slice(&header[1..]);
                put_length(out, hash.len() as u64);
                progress.min = min;
                return encode_some(out, key, value, None, progress, max_elements, max_bytes);
            }
            Value::ZSet(zset) => (TYPE_ZSET_2, zset.len()),
        };
        out.push(kind);
        put_string(out, key);
        put_length(out, len as u64);
    }
    let limit = out.len().saturating_add(max_bytes);
    let mut added = 0;
    match value {
        Value::String(_) => true,
        Value::List(list) => {
            let mut index = progress.next as usize;
            while index < list.len() && added < max_elements && out.len() < limit {
                put_string(out, &list[index]);
                index += 1;
                added += 1;
            }
            progress.next = index as u64;
            index >= list.len()
        }
        Value::ZSet(zset) => {
            let mut index = progress.next as usize;
            let end = index.saturating_add(max_elements).min(zset.len());
            for (member, score) in zset.range(index..end, false) {
                put_string(out, member);
                out.extend_from_slice(&score.to_le_bytes());
                index += 1;
                if out.len() >= limit {
                    break;
                }
            }
            progress.next = index as u64;
            index >= zset.len()
        }
        // A few buckets of the table at a time, in the order they lie in
        // memory.
        Value::Hash(hash) => loop {
            let min = progress.min;
            let next = hash.visit_from(progress.next, PART_BUCKETS, |field, value| {
                put_hash_field(out, hash, min, field, value);
                added += 1;
            });
            match next {
                None => break true,
                Some(next) => progress.next = next,
            }
            if added >= max_elements || out.len() >= limit {
                break false;
            }
        },
        Value::Set(set) => loop {
            let next = set.visit_from(progress.next, PART_BUCKETS, |member| {
                put_string(out, member);
                added += 1;
            });
            match next {
                None => break true,
                Some(next) => progress.next = next,
            }
            if added >= max_elements || out.len() >= limit {
                break false;
            }
        },
    }
}

/// Appends the version and checksum that end a `DUMP`-style payload.
fn seal(mut payload: Vec<u8>, version: u16) -> Vec<u8> {
    payload.extend_from_slice(&version.to_le_bytes());
    let crc = crc64::update(0, &payload);
    payload.extend_from_slice(&crc.to_le_bytes());
    payload
}

/// Checks the version and checksum of a `DUMP`-style payload and returns
/// its body.
fn unseal(payload: &[u8]) -> Result<&[u8], RdbError> {
    let Some(split) = payload.len().checked_sub(10) else {
        return Err(RdbError::Truncated);
    };
    let (body, footer) = payload.split_at(split);
    let version = u16::from_le_bytes([footer[0], footer[1]]);
    if version > MAX_VERSION {
        return Err(RdbError::UnsupportedVersion(version));
    }
    let stored = u64::from_le_bytes(footer[2..].try_into().expect("eight bytes"));
    if stored != crc64::update(0, &payload[..split + 2]) {
        return Err(RdbError::BadChecksum);
    }
    Ok(body)
}

/// The payload `DUMP` returns for `value`.
pub fn dump_value(value: &Value) -> Vec<u8> {
    let mut payload = Vec::new();
    put_value(&mut payload, value);
    seal(payload, needs_version(value))
}

/// Decodes a payload produced by `DUMP`, here or on Redis.
pub fn restore_value(payload: &[u8]) -> Result<Value, RdbError> {
    let mut reader = Reader::new(unseal(payload)?);
    let kind = reader.byte()?;
    let value = reader.value(kind)?;
    if reader.at != reader.bytes.len() {
        return Err(RdbError::Corrupt("data after the value"));
    }
    Ok(value)
}

/// The payload `FUNCTION DUMP` returns for the given library sources.
pub fn dump_functions<'a>(libraries: impl Iterator<Item = &'a [u8]>) -> Vec<u8> {
    let mut payload = Vec::new();
    for code in libraries {
        payload.push(OP_FUNCTION);
        put_string(&mut payload, code);
    }
    seal(payload, VERSION)
}

/// Decodes a `FUNCTION DUMP` payload into library sources.
pub fn restore_functions(payload: &[u8]) -> Result<Vec<Bytes>, RdbError> {
    let mut reader = Reader::new(unseal(payload)?);
    let mut libraries = Vec::new();
    while reader.at < reader.bytes.len() {
        match reader.byte()? {
            OP_FUNCTION => libraries.push(reader.string()?),
            _ => return Err(RdbError::Corrupt("not a function library")),
        }
    }
    Ok(libraries)
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// One record of an RDB file.
#[derive(Debug, Clone, PartialEq)]
pub enum Item {
    Aux {
        name: Bytes,
        value: Bytes,
    },
    /// The source code of a function library.
    Function(Bytes),
    Entry {
        db: usize,
        key: Bytes,
        value: Value,
        /// Milliseconds since the Unix epoch.
        expire_at: Option<u64>,
    },
}

/// A length, or one of the special string encodings that share its prefix.
enum Length {
    Plain(u64),
    Int(i64),
    Lzf,
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Reader { bytes, at: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], RdbError> {
        let end = self
            .at
            .checked_add(count)
            .filter(|&end| end <= self.bytes.len())
            .ok_or(RdbError::Truncated)?;
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }

    fn byte(&mut self) -> Result<u8, RdbError> {
        Ok(self.take(1)?[0])
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], RdbError> {
        Ok(self.take(N)?.try_into().expect("take returned N bytes"))
    }

    fn encoded_length(&mut self) -> Result<Length, RdbError> {
        let first = self.byte()?;
        Ok(match first >> 6 {
            0 => Length::Plain(u64::from(first & 0x3F)),
            1 => Length::Plain((u64::from(first & 0x3F) << 8) | u64::from(self.byte()?)),
            2 => match first {
                0x80 => Length::Plain(u64::from(u32::from_be_bytes(self.array()?))),
                0x81 => Length::Plain(u64::from_be_bytes(self.array()?)),
                _ => return Err(RdbError::Corrupt("unknown length encoding")),
            },
            _ => match first & 0x3F {
                0 => Length::Int(i64::from(self.byte()? as i8)),
                1 => Length::Int(i64::from(i16::from_le_bytes(self.array()?))),
                2 => Length::Int(i64::from(i32::from_le_bytes(self.array()?))),
                3 => Length::Lzf,
                _ => return Err(RdbError::Corrupt("unknown string encoding")),
            },
        })
    }

    fn length(&mut self) -> Result<usize, RdbError> {
        match self.encoded_length()? {
            Length::Plain(length) => {
                usize::try_from(length).map_err(|_| RdbError::Corrupt("length too large"))
            }
            _ => Err(RdbError::Corrupt("expected a length")),
        }
    }

    fn string(&mut self) -> Result<Bytes, RdbError> {
        Ok(match self.encoded_length()? {
            Length::Plain(length) => {
                let length =
                    usize::try_from(length).map_err(|_| RdbError::Corrupt("length too large"))?;
                Bytes::copy_from_slice(self.take(length)?)
            }
            Length::Int(number) => Bytes::from(number.to_string()),
            Length::Lzf => {
                let compressed = self.length()?;
                let expanded = self.length()?;
                if expanded > MAX_STRING_LEN {
                    return Err(RdbError::Corrupt("compressed string too long"));
                }
                let data = self.take(compressed)?;
                Bytes::from(
                    lzf::decompress(data, expanded)
                        .ok_or(RdbError::Corrupt("bad LZF-compressed string"))?,
                )
            }
        })
    }

    /// A count of elements, checked against what is left so that a corrupt
    /// count cannot ask for an absurd allocation.
    fn count(&mut self, bytes_per_element: usize) -> Result<usize, RdbError> {
        let count = self.length()?;
        let room = (self.bytes.len() - self.at) / bytes_per_element.max(1);
        if count > room {
            return Err(RdbError::Truncated);
        }
        Ok(count)
    }

    fn score(&mut self, binary: bool) -> Result<f64, RdbError> {
        let score = if binary {
            f64::from_le_bytes(self.array()?)
        } else {
            // The old text form: a length byte, with three reserved values.
            match self.byte()? {
                255 => f64::NEG_INFINITY,
                254 => f64::INFINITY,
                253 => f64::NAN,
                length => std::str::from_utf8(self.take(usize::from(length))?)
                    .ok()
                    .and_then(|text| text.parse().ok())
                    .ok_or(RdbError::Corrupt("bad score"))?,
            }
        };
        if score.is_nan() {
            return Err(RdbError::Corrupt("score is not a number"));
        }
        Ok(score)
    }

    fn value(&mut self, kind: u8) -> Result<Value, RdbError> {
        Ok(match kind {
            TYPE_STRING => Value::String(self.string()?),
            TYPE_LIST => {
                let count = self.count(1)?;
                let mut list = VecDeque::with_capacity(count.min(MAX_RESERVED));
                for _ in 0..count {
                    list.push_back(self.string()?);
                }
                Value::List(list)
            }
            TYPE_SET => {
                let mut set = Set::new();
                for _ in 0..self.count(1)? {
                    set.insert(&self.string()?);
                }
                Value::Set(set)
            }
            TYPE_ZSET | TYPE_ZSET_2 => {
                let mut zset = ZSet::new();
                for _ in 0..self.count(2)? {
                    let member = self.string()?;
                    zset.insert(&member, self.score(kind == TYPE_ZSET_2)?);
                }
                Value::ZSet(zset)
            }
            TYPE_HASH => {
                let mut hash = Hash::new();
                for _ in 0..self.count(2)? {
                    let field = self.string()?;
                    hash.insert(&field, self.string()?);
                }
                Value::Hash(hash)
            }
            TYPE_HASH_METADATA => {
                let min = u64::from_le_bytes(self.array()?);
                let count = self.count(3)?;
                let mut fields = Vec::with_capacity(count.min(MAX_RESERVED));
                for _ in 0..count {
                    let ttl = self.length()? as u64;
                    let at = match ttl {
                        0 => None,
                        ttl => Some(
                            min.checked_add(ttl - 1)
                                .ok_or(RdbError::Corrupt("bad field TTL"))?,
                        ),
                    };
                    let field = self.string()?;
                    fields.push((field, self.string()?, at));
                }
                Value::Hash(Hash::from_fields(fields, true))
            }
            TYPE_HASH_LISTPACK_EX => {
                // The earliest TTL, only a hint.
                self.array::<8>()?;
                let items = listpack(&self.string()?)?;
                if items.len() % 3 != 0 {
                    return Err(RdbError::Corrupt("hash with an incomplete field"));
                }
                let mut fields = Vec::with_capacity(items.len() / 3);
                let mut items = items.into_iter();
                while let (Some(field), Some(value), Some(ttl)) =
                    (items.next(), items.next(), items.next())
                {
                    let at = std::str::from_utf8(&ttl)
                        .ok()
                        .and_then(|text| text.parse::<u64>().ok())
                        .ok_or(RdbError::Corrupt("bad field TTL"))?;
                    fields.push((field, value, (at != 0).then_some(at)));
                }
                Value::Hash(Hash::from_fields(fields, true))
            }
            TYPE_SET_INTSET => {
                let mut set = Set::new();
                for member in intset(&self.string()?)? {
                    set.insert(member.to_string().as_bytes());
                }
                Value::Set(set)
            }
            TYPE_SET_LISTPACK => {
                let mut set = Set::new();
                for member in listpack(&self.string()?)? {
                    set.insert(&member);
                }
                Value::Set(set)
            }
            TYPE_HASH_LISTPACK => {
                let mut hash = Hash::new();
                let (pairs, odd) = pairs(listpack(&self.string()?)?);
                if odd {
                    return Err(RdbError::Corrupt("hash with a field but no value"));
                }
                for (field, value) in pairs {
                    hash.insert(&field, value);
                }
                Value::Hash(hash)
            }
            TYPE_ZSET_LISTPACK => {
                let mut zset = ZSet::new();
                let (pairs, odd) = pairs(listpack(&self.string()?)?);
                if odd {
                    return Err(RdbError::Corrupt("sorted set with a member but no score"));
                }
                for (member, score) in pairs {
                    let score: f64 = std::str::from_utf8(&score)
                        .ok()
                        .and_then(|text| text.parse().ok())
                        .filter(|score: &f64| !score.is_nan())
                        .ok_or(RdbError::Corrupt("bad score"))?;
                    zset.insert(&member, score);
                }
                Value::ZSet(zset)
            }
            TYPE_LIST_QUICKLIST_2 => {
                let mut list = VecDeque::new();
                for _ in 0..self.count(2)? {
                    // A node is either one large element or a listpack.
                    match self.length()? {
                        1 => list.push_back(self.string()?),
                        2 => list.extend(listpack(&self.string()?)?),
                        _ => return Err(RdbError::Corrupt("unknown list node container")),
                    }
                }
                Value::List(list)
            }
            15 | 19 | 21 => return Err(RdbError::Unsupported("a stream".to_owned())),
            6 | 7 => return Err(RdbError::Unsupported("a module value".to_owned())),
            22 | 23 => {
                return Err(RdbError::Unsupported(
                    "a hash with field TTLs in the form of a Redis 7.4 release candidate"
                        .to_owned(),
                ))
            }
            9 | 10 | 12 | 13 | 14 => {
                return Err(RdbError::Unsupported(format!(
                    "value type {kind}, written by Redis older than 7.0"
                )))
            }
            other => return Err(RdbError::Unsupported(format!("value type {other}"))),
        })
    }
}

fn pairs(items: Vec<Bytes>) -> (Vec<(Bytes, Bytes)>, bool) {
    let odd = items.len() % 2 == 1;
    let mut items = items.into_iter();
    let mut pairs = Vec::new();
    while let (Some(first), Some(second)) = (items.next(), items.next()) {
        pairs.push((first, second));
    }
    (pairs, odd)
}

/// Decodes Redis's "intset": a sorted array of integers of one width.
fn intset(blob: &[u8]) -> Result<Vec<i64>, RdbError> {
    let bad = RdbError::Corrupt("bad intset");
    let (Some(width), Some(count)) = (blob.get(0..4), blob.get(4..8)) else {
        return Err(bad);
    };
    let width = u32::from_le_bytes(width.try_into().expect("four bytes")) as usize;
    let count = u32::from_le_bytes(count.try_into().expect("four bytes")) as usize;
    let body = &blob[8..];
    if !matches!(width, 2 | 4 | 8)
        || body.len() / width != count
        || !body.len().is_multiple_of(width)
    {
        return Err(bad);
    }
    Ok(body
        .chunks(width)
        .map(|chunk| match width {
            2 => i64::from(i16::from_le_bytes(chunk.try_into().expect("two bytes"))),
            4 => i64::from(i32::from_le_bytes(chunk.try_into().expect("four bytes"))),
            _ => i64::from_le_bytes(chunk.try_into().expect("eight bytes")),
        })
        .collect())
}

/// Decodes Redis's "listpack": a compact sequence of strings and integers.
/// Integers are returned as their decimal text.
fn listpack(blob: &[u8]) -> Result<Vec<Bytes>, RdbError> {
    let bad = || RdbError::Corrupt("bad listpack");
    // Six header bytes (total size, element count), entries, then 0xFF.
    let mut at = 6;
    let mut items = Vec::new();
    loop {
        let &first = blob.get(at).ok_or_else(bad)?;
        if first == 0xFF {
            return Ok(items);
        }
        let need = |count: usize| blob.get(at + 1..at + 1 + count).ok_or_else(bad);
        let integer = |value: i64, size: usize| (Bytes::from(value.to_string()), size);
        let (item, size) = if first & 0x80 == 0 {
            integer(i64::from(first), 1)
        } else if first & 0xC0 == 0x80 {
            let length = usize::from(first & 0x3F);
            (Bytes::copy_from_slice(need(length)?), 1 + length)
        } else if first & 0xE0 == 0xC0 {
            // A 13-bit signed integer.
            let raw = (i64::from(first & 0x1F) << 8) | i64::from(need(1)?[0]);
            integer(if raw >= 1 << 12 { raw - (1 << 13) } else { raw }, 2)
        } else if first & 0xF0 == 0xE0 {
            let length = (usize::from(first & 0x0F) << 8) | usize::from(need(1)?[0]);
            let text = blob.get(at + 2..at + 2 + length).ok_or_else(bad)?;
            (Bytes::copy_from_slice(text), 2 + length)
        } else {
            match first {
                0xF0 => {
                    let length =
                        u32::from_le_bytes(need(4)?.try_into().expect("four bytes")) as usize;
                    let text = blob
                        .get(at + 5..)
                        .and_then(|rest| rest.get(..length))
                        .ok_or_else(bad)?;
                    (Bytes::copy_from_slice(text), 5 + length)
                }
                0xF1 => integer(
                    i64::from(i16::from_le_bytes(need(2)?.try_into().expect("two bytes"))),
                    3,
                ),
                0xF2 => {
                    let bytes = need(3)?;
                    let raw = i64::from(bytes[0])
                        | (i64::from(bytes[1]) << 8)
                        | (i64::from(bytes[2]) << 16);
                    integer(if raw >= 1 << 23 { raw - (1 << 24) } else { raw }, 4)
                }
                0xF3 => integer(
                    i64::from(i32::from_le_bytes(need(4)?.try_into().expect("four bytes"))),
                    5,
                ),
                0xF4 => integer(
                    i64::from_le_bytes(need(8)?.try_into().expect("eight bytes")),
                    9,
                ),
                _ => return Err(bad()),
            }
        };
        items.push(item);
        // Each entry is followed by its own length, in one to five bytes,
        // so the structure can be walked backwards.
        let back_length = match size {
            0..=127 => 1,
            128..=16_382 => 2,
            16_383..=2_097_150 => 3,
            2_097_151..=268_435_454 => 4,
            _ => 5,
        };
        at += size + back_length;
    }
}

/// Reads a whole RDB file, calling `visit` for each record in file order.
/// Statistics Redis stores per key (access frequency, idle time) are
/// skipped.
pub fn read(data: &[u8], mut visit: impl FnMut(Item)) -> Result<(), RdbError> {
    if !data.starts_with(b"REDIS") {
        return Err(RdbError::NotRdb);
    }
    let version = data
        .get(5..9)
        .and_then(|digits| std::str::from_utf8(digits).ok())
        .and_then(|digits| digits.parse::<u16>().ok())
        .ok_or(RdbError::NotRdb)?;
    if !(1..=MAX_VERSION).contains(&version) {
        return Err(RdbError::UnsupportedVersion(version));
    }

    let mut reader = Reader::new(data);
    reader.at = 9;
    let mut db = 0;
    let mut expire_at = None;
    loop {
        match reader.byte()? {
            OP_EOF => break,
            OP_SELECT_DB => db = reader.length()?,
            OP_RESIZE_DB => {
                // Table sizes, only a hint.
                reader.length()?;
                reader.length()?;
            }
            OP_AUX => {
                let name = reader.string()?;
                let value = reader.string()?;
                visit(Item::Aux { name, value });
            }
            OP_FUNCTION => visit(Item::Function(reader.string()?)),
            OP_EXPIRE_MS => expire_at = Some(u64::from_le_bytes(reader.array()?)),
            OP_EXPIRE_SECONDS => {
                expire_at = Some(u64::from(u32::from_le_bytes(reader.array()?)) * 1000);
            }
            OP_FREQ => {
                reader.byte()?;
            }
            OP_IDLE => {
                reader.length()?;
            }
            0xF6 | 0xF7 => {
                return Err(RdbError::Unsupported(
                    "module data or pre-release functions".to_owned(),
                ))
            }
            kind => {
                let key = reader.string()?;
                let value = reader.value(kind)?;
                visit(Item::Entry {
                    db,
                    key,
                    value,
                    expire_at: expire_at.take(),
                });
            }
        }
    }
    // From version 5 the file ends with a checksum; zero means "not kept".
    if version >= 5 {
        let checked = reader.at;
        let stored = u64::from_le_bytes(reader.array()?);
        if stored != 0 && stored != crc64::update(0, &data[..checked]) {
            return Err(RdbError::BadChecksum);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn b(text: &str) -> Bytes {
        Bytes::copy_from_slice(text.as_bytes())
    }

    fn sample_values() -> Vec<(&'static str, Value, Option<u64>)> {
        let mut hash = Hash::new();
        hash.insert(b"field", b("value"));
        hash.insert(b"n", b("42"));
        let mut set = Set::new();
        for member in ["a", "b", "17"] {
            set.insert(member.as_bytes());
        }
        let mut ints = Set::new();
        for member in ["-5", "3", "70000"] {
            ints.insert(member.as_bytes());
        }
        let mut zset = ZSet::new();
        zset.insert(b"low", -1.5);
        zset.insert(b"high", 1e20);
        zset.insert(b"inf", f64::INFINITY);
        vec![
            ("str", Value::String(b("hello")), None),
            ("empty", Value::String(Bytes::new()), None),
            (
                "binary",
                Value::String(Bytes::from(vec![0u8, 255, 10, 13])),
                None,
            ),
            ("big", Value::String(Bytes::from(vec![b'x'; 70_000])), None),
            ("ttl", Value::String(b("soon")), Some(1_900_000_000_123)),
            (
                "list",
                Value::List(VecDeque::from([b("one"), b("2"), b("")])),
                None,
            ),
            ("hash", Value::Hash(hash), Some(1_900_000_000_000)),
            ("set", Value::Set(set), None),
            ("ints", Value::Set(ints), None),
            ("zset", Value::ZSet(zset), None),
        ]
    }

    fn write_sample() -> Vec<u8> {
        let mut writer = Writer::new(Vec::new()).unwrap();
        writer.aux("redis-ver", "7.2.0").unwrap();
        writer.function(b"#!lua name=lib\n...").unwrap();
        let values = sample_values();
        writer.select_db(0, values.len(), 2).unwrap();
        for (key, value, expire_at) in &values {
            writer.entry(key.as_bytes(), value, *expire_at).unwrap();
        }
        writer.select_db(3, 1, 0).unwrap();
        writer
            .entry(b"other-db", &Value::String(b("three")), None)
            .unwrap();
        writer.finish().unwrap()
    }

    fn read_all(data: &[u8]) -> Result<Vec<Item>, RdbError> {
        let mut items = Vec::new();
        read(data, |item| items.push(item))?;
        Ok(items)
    }

    #[test]
    fn what_is_written_reads_back() {
        let data = write_sample();
        assert!(data.starts_with(b"REDIS0011"));
        let items = read_all(&data).unwrap();
        let mut expected = vec![
            Item::Aux {
                name: b("redis-ver"),
                value: b("7.2.0"),
            },
            Item::Function(b("#!lua name=lib\n...")),
        ];
        expected.extend(
            sample_values()
                .into_iter()
                .map(|(key, value, expire_at)| Item::Entry {
                    db: 0,
                    key: b(key),
                    value,
                    expire_at,
                }),
        );
        expected.push(Item::Entry {
            db: 3,
            key: b("other-db"),
            value: Value::String(b("three")),
            expire_at: None,
        });
        assert_eq!(items, expected);
    }

    #[test]
    fn damage_is_detected() {
        let data = write_sample();
        // Any single flipped bit breaks the checksum (or the structure).
        for position in [9, 40, data.len() / 2, data.len() - 9, data.len() - 1] {
            let mut damaged = data.clone();
            damaged[position] ^= 0x04;
            assert!(read_all(&damaged).is_err(), "byte {position}");
        }
        // So does a file cut short anywhere.
        for length in [0, 4, 9, 20, data.len() / 2, data.len() - 8, data.len() - 1] {
            assert!(read_all(&data[..length]).is_err(), "{length} bytes");
        }
        assert!(matches!(read_all(b"NOTRDB0011"), Err(RdbError::NotRdb)));
        assert!(matches!(
            read_all(b"REDIS9999\xFF"),
            Err(RdbError::UnsupportedVersion(9999))
        ));
    }

    #[test]
    fn a_zero_checksum_means_unchecked() {
        let mut data = write_sample();
        let end = data.len();
        data[end - 8..].fill(0);
        assert!(read_all(&data).is_ok());
    }

    #[test]
    fn dump_payloads() {
        for (name, value, _) in sample_values() {
            let payload = dump_value(&value);
            assert_eq!(restore_value(&payload).unwrap(), value, "{name}");
            let mut damaged = payload.clone();
            damaged[0] ^= 1;
            assert!(restore_value(&damaged).is_err(), "{name}");
        }
        assert!(matches!(restore_value(b"short"), Err(RdbError::Truncated)));
    }

    #[test]
    fn a_payload_cannot_ask_for_more_memory_than_it_holds() {
        // A compressed string of one byte that claims to expand to 2^64 - 1.
        let mut body = vec![TYPE_STRING, 0xC3, 0x01, 0x81];
        body.extend_from_slice(&[0xFF; 8]);
        body.push(b'A');
        assert!(matches!(
            restore_value(&seal(body, 11)),
            Err(RdbError::Corrupt(_))
        ));
        // And one that claims more than a string may hold.
        let mut body = vec![TYPE_STRING, 0xC3, 0x01, 0x81];
        body.extend_from_slice(&(1u64 << 30).to_be_bytes());
        body.push(b'A');
        assert!(matches!(
            restore_value(&seal(body, 11)),
            Err(RdbError::Corrupt(_))
        ));
    }

    fn ttl_hash() -> Hash {
        let mut hash = Hash::new();
        for (field, value) in [("a", "1"), ("b", "2"), ("c", "3")] {
            hash.insert(field.as_bytes(), b(value));
        }
        hash.set_field_expire(b"b", 1_800_000_000_500);
        hash.set_field_expire(b"a", 1_800_000_000_000);
        hash
    }

    #[test]
    fn hash_field_ttls_need_the_newer_format() {
        let hash = Value::Hash(ttl_hash());
        assert_eq!(needs_version(&hash), VERSION_FIELD_TTLS);
        assert_eq!(needs_version(&Value::Hash(Hash::new())), VERSION);
        // A hash whose field TTLs have all been removed is a plain hash again.
        let Value::Hash(mut persisted) = hash.clone() else {
            unreachable!()
        };
        persisted.persist_field(b"a");
        persisted.persist_field(b"b");
        assert_eq!(needs_version(&Value::Hash(persisted.clone())), VERSION);
        assert_eq!(dump_value(&Value::Hash(persisted))[0], TYPE_HASH);

        let payload = dump_value(&hash);
        assert_eq!(payload[0], TYPE_HASH_LISTPACK_EX);
        assert_eq!(&payload[payload.len() - 10..][..2], &12u16.to_le_bytes());
        assert_eq!(restore_value(&payload).unwrap(), hash);

        let mut writer = Writer::with_version(Vec::new(), VERSION_FIELD_TTLS).unwrap();
        writer.select_db(0, 1, 0).unwrap();
        writer.entry(b"h", &hash, Some(1_900_000_000_000)).unwrap();
        let data = writer.finish().unwrap();
        assert!(data.starts_with(b"REDIS0012"));
        assert_eq!(
            read_all(&data).unwrap(),
            vec![Item::Entry {
                db: 0,
                key: b("h"),
                value: hash,
                expire_at: Some(1_900_000_000_000),
            }]
        );
    }

    fn hex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn a_small_hash_with_field_ttls_dumps_exactly_as_redis_does() {
        let mut hash = Hash::new();
        for (field, value) in [("a", "1"), ("b", "2"), ("c", "3")] {
            hash.insert(field.as_bytes(), b(value));
        }
        hash.set_field_expire(b"a", 0x01a1_ff00_f03c);
        // What Redis 7.4 replied to DUMP for the same hash.
        assert_eq!(
            dump_value(&Value::Hash(hash.clone())),
            hex("193cf000ffa1010000242400000009008161020101f43cf000ffa1010000098162020201000181630203010001ff0c007619cd1c50a5a18e")
        );
        // A large table goes in the other form.
        let mut big = hash;
        for n in 0..600 {
            big.insert(format!("f{n}").as_bytes(), b("v"));
        }
        let payload = dump_value(&Value::Hash(big.clone()));
        assert_eq!(payload[0], TYPE_HASH_METADATA);
        assert_eq!(restore_value(&payload).unwrap(), Value::Hash(big));
    }

    #[test]
    fn listpack_integers_round_trip() {
        for number in [
            0i64,
            1,
            127,
            128,
            -1,
            4095,
            -4096,
            4096,
            32_767,
            -32_768,
            32_768,
            8_388_607,
            -8_388_608,
            8_388_608,
            i32::MAX as i64,
            i32::MIN as i64,
            i64::MAX,
            i64::MIN,
        ] {
            let mut entries = Vec::new();
            put_listpack_value(&mut entries, number.to_string().as_bytes());
            put_listpack_value(&mut entries, b"007");
            let mut blob = ((entries.len() + 7) as u32).to_le_bytes().to_vec();
            blob.extend_from_slice(&2u16.to_le_bytes());
            blob.extend_from_slice(&entries);
            blob.push(0xFF);
            assert_eq!(
                listpack(&blob).unwrap(),
                [b(&number.to_string()), b("007")],
                "{number}"
            );
        }
        assert_eq!(canonical_integer(b"-0"), None);
        assert_eq!(canonical_integer(b"+1"), None);
        assert_eq!(canonical_integer(b"12"), Some(12));
    }

    /// `DUMP` payloads taken from Redis 7.4.
    #[test]
    fn hashes_with_field_ttls_from_redis() {
        // A listpack with TTLs (`listpackex`): a with a TTL, b and c without.
        let Value::Hash(hash) = restore_value(&hex(
            "193cf000ffa1010000242400000009008161020101f43cf000ffa1010000098162020201000181630203010001ff0c007619cd1c50a5a18e",
        ))
        .unwrap() else {
            panic!("not a hash")
        };
        assert_eq!(hash.len(), 3);
        assert_eq!(hash.field_expire_at(b"a"), Some(0x01a1_ff00_f03c));
        assert_eq!(hash.field_expire_at(b"b"), None);
        assert_eq!(hash.get(b"c", 0), Some(&b("3")));

        // The same form after every TTL went: still `listpackex`.
        let Value::Hash(hash) = restore_value(&hex(
            "190000000000000000151500000006008161020101000181620202010001ff0c00401cf997f61b5652",
        ))
        .unwrap() else {
            panic!("not a hash")
        };
        assert!(hash.has_field_ttls());
        assert_eq!(hash.next_field_expiry(), None);
        assert_eq!(hash.get(b"b", 0), Some(&b("2")));

        // A hash table with TTLs: b and c with, an LZF-compressed long1
        // without.
        let Value::Hash(hash) = restore_value(&hex(
            "1800505c18a301000003010162c00241f50163c00300056c6f6e6731c309404a017878e03d000178780c004c6c63100fb531c7",
        ))
        .unwrap() else {
            panic!("not a hash")
        };
        assert_eq!(hash.field_expire_at(b"b"), Some(1_800_000_000_000));
        assert_eq!(hash.field_expire_at(b"c"), Some(1_800_000_000_500));
        assert_eq!(hash.field_expire_at(b"long1"), None);
        assert_eq!(hash.get(b"long1", 0), Some(&b(&"x".repeat(74))));
        assert_eq!(hash.get(b"c", 0), Some(&b("3")));
    }

    #[test]
    fn function_payloads() {
        let libraries = [&b"#!lua name=a\n1"[..], &b"#!lua name=b\n2"[..]];
        let payload = dump_functions(libraries.iter().copied());
        assert_eq!(
            restore_functions(&payload).unwrap(),
            [b("#!lua name=a\n1"), b("#!lua name=b\n2")]
        );
        assert_eq!(
            restore_functions(&dump_functions(std::iter::empty())).unwrap(),
            Vec::<Bytes>::new()
        );
        assert!(restore_functions(&dump_value(&Value::String(b("x")))).is_err());
    }

    #[test]
    fn special_string_encodings() {
        // Integers stored in one, two and four bytes, and an LZF string.
        for (encoded, expected) in [
            (&[0xC0, 0x7B][..], "123"),
            (&[0xC0, 0xFF][..], "-1"),
            (&[0xC1, 0x39, 0x30][..], "12345"),
            (&[0xC2, 0x00, 0x00, 0x00, 0x80][..], "-2147483648"),
            (&[0xC3, 5, 10, 0, b'a', 0xE0, 0, 0][..], "aaaaaaaaaa"),
        ] {
            let mut reader = Reader::new(encoded);
            assert_eq!(reader.string().unwrap(), b(expected));
            assert_eq!(reader.at, encoded.len());
        }
    }

    #[test]
    fn length_encodings() {
        for length in [0u64, 63, 64, 16_383, 16_384, 0xFFFF_FFFF, 0x1_0000_0000] {
            let mut out = Vec::new();
            put_length(&mut out, length);
            let mut reader = Reader::new(&out);
            assert!(matches!(reader.encoded_length().unwrap(), Length::Plain(n) if n == length));
            assert_eq!(reader.at, out.len());
        }
    }

    /// Builds a listpack the way Redis lays one out.
    fn build_listpack(entries: &[Vec<u8>]) -> Vec<u8> {
        let mut body = Vec::new();
        for entry in entries {
            body.extend_from_slice(entry);
            // The back-length of a short entry is its size in one byte.
            assert!(entry.len() <= 127);
            body.push(entry.len() as u8);
        }
        let mut blob = ((body.len() + 7) as u32).to_le_bytes().to_vec();
        blob.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        blob.extend_from_slice(&body);
        blob.push(0xFF);
        blob
    }

    /// Encodes `value` in steps of `step` elements and reads it back.
    fn in_parts(value: &Value, expire_at: Option<u64>, step: usize, max_bytes: usize) -> usize {
        let mut encoded = Vec::new();
        let mut progress = Progress::default();
        let mut calls = 0;
        loop {
            calls += 1;
            assert!(calls < 1_000_000, "never finished");
            let before = encoded.len();
            let done = encode_some(
                &mut encoded,
                b"key",
                value,
                expire_at,
                &mut progress,
                step,
                max_bytes,
            );
            if done {
                break;
            }
            // Every call that does not finish makes progress.
            assert!(encoded.len() > before, "a call added nothing");
        }
        let mut writer = Writer::new(Vec::new()).unwrap();
        writer
            .select_db(2, 1, usize::from(expire_at.is_some()))
            .unwrap();
        writer.encoded(&encoded).unwrap();
        let items = read_all(&writer.finish().unwrap()).unwrap();
        assert_eq!(
            items,
            vec![Item::Entry {
                db: 2,
                key: b("key"),
                value: value.clone(),
                expire_at
            }]
        );
        calls
    }

    #[test]
    fn a_value_encoded_in_parts_reads_back_the_same() {
        let text = |n: usize| Bytes::from(format!("element-{n}"));
        // Past the sizes at which hashes, sets and sorted sets leave their
        // compact forms, and small ones too.
        for count in [0, 1, 7, 600, 5_000] {
            let mut hash = Hash::new();
            let mut set = Set::new();
            let mut ints = Set::new();
            let mut zset = ZSet::new();
            for n in 0..count {
                hash.insert(&text(n), text(n * 2));
                set.insert(&text(n));
                ints.insert(n.to_string().as_bytes());
                zset.insert(&text(n), n as f64 / 3.0);
            }
            // Every third field with a TTL of its own, some the same.
            let mut with_ttls = hash.clone();
            for n in (0..count).step_by(3) {
                with_ttls.set_field_expire(&text(n), 1_900_000_000_000 + (n as u64 % 7));
            }
            let values = [
                Value::List((0..count).map(text).collect()),
                Value::Hash(hash),
                Value::Hash(with_ttls),
                Value::Set(set),
                Value::Set(ints),
                Value::ZSet(zset),
            ];
            for value in &values {
                for step in [1, 3, 64, 100_000] {
                    let calls = in_parts(value, Some(77), step, usize::MAX);
                    if count == 5_000 && step == 64 {
                        // In pieces, not all at once.
                        assert!(calls > 20, "{calls} calls");
                    }
                }
                // Limited by bytes instead of elements.
                let calls = in_parts(value, None, usize::MAX, 256);
                if count == 5_000 {
                    assert!(calls > 20, "{calls} calls");
                }
            }
        }
        // A string is always written whole.
        assert_eq!(in_parts(&Value::String(b("hello")), None, 1, 1), 1);
    }

    #[test]
    fn a_hash_being_rehashed_is_still_encoded_once_each() {
        // Grown one at a time, a table is usually caught half-way between
        // two sizes; a scan then has two tables to cover.
        for count in [513, 700, 1_025, 2_049, 3_000] {
            let mut hash = Hash::new();
            for n in 0..count {
                hash.insert(format!("f{n}").as_bytes(), Bytes::from(n.to_string()));
            }
            in_parts(&Value::Hash(hash), None, 5, usize::MAX);
        }
    }

    #[test]
    fn long_strings_are_written_exactly_like_short_ones() {
        for len in [
            STREAMED_STRING_BYTES - 1,
            STREAMED_STRING_BYTES,
            STREAMED_STRING_BYTES + 10,
        ] {
            let value = Value::String(Bytes::from(vec![b'x'; len]));
            let mut writer = Writer::new(Vec::new()).unwrap();
            writer.select_db(0, 2, 1).unwrap();
            writer.entry(b"with-ttl", &value, Some(123)).unwrap();
            writer.entry(b"without", &value, None).unwrap();
            let written = writer.finish().unwrap();

            // The same file put together from separately encoded entries,
            // which never takes the short cut for long strings.
            let mut writer = Writer::new(Vec::new()).unwrap();
            writer.select_db(0, 2, 1).unwrap();
            let mut encoded = Vec::new();
            encode_entry(&mut encoded, b"with-ttl", &value, Some(123));
            encode_entry(&mut encoded, b"without", &value, None);
            writer.encoded(&encoded).unwrap();
            assert!(written == writer.finish().unwrap(), "length {len}");
        }
    }

    #[test]
    fn listpack_entries() {
        let mut long = vec![0xE0, 100];
        long.extend_from_slice(&[b'z'; 100]);
        let blob = build_listpack(&[
            vec![5],                               // small unsigned
            vec![0x80 | 3, b'a', b'b', b'c'],      // short string
            vec![0xC0 | 0x1F, 0xFF],               // 13-bit: -1
            vec![0xC0 | 0x03, 0xE8],               // 13-bit: 1000
            long,                                  // 12-bit length string
            vec![0xF1, 0x00, 0x80],                // 16-bit: -32768
            vec![0xF2, 0xFF, 0xFF, 0x7F],          // 24-bit: 8388607
            vec![0xF3, 0x00, 0x00, 0x00, 0x80],    // 32-bit: i32::MIN
            vec![0xF4, 1, 0, 0, 0, 0, 0, 0, 0x80], // 64-bit
            vec![0xF0, 2, 0, 0, 0, b'h', b'i'],    // 32-bit length string
        ]);
        let expected: Vec<Bytes> = [
            "5".to_owned(),
            "abc".to_owned(),
            "-1".to_owned(),
            "1000".to_owned(),
            "z".repeat(100),
            "-32768".to_owned(),
            "8388607".to_owned(),
            i32::MIN.to_string(),
            (i64::MIN + 1).to_string(),
            "hi".to_owned(),
        ]
        .iter()
        .map(|text| b(text))
        .collect();
        assert_eq!(listpack(&blob).unwrap(), expected);

        assert!(listpack(&blob[..blob.len() - 1]).is_err());
        assert!(listpack(&[]).is_err());
    }

    #[test]
    fn compact_value_types() {
        let pack = |items: &[&str]| {
            build_listpack(
                &items
                    .iter()
                    .map(|item| {
                        let mut entry = vec![0x80 | item.len() as u8];
                        entry.extend_from_slice(item.as_bytes());
                        entry
                    })
                    .collect::<Vec<_>>(),
            )
        };
        let blob = |kind: u8, pack: &[u8]| {
            let mut data = Vec::new();
            put_string(&mut data, pack);
            Reader::new(&data).value(kind)
        };

        let Value::Hash(hash) = blob(TYPE_HASH_LISTPACK, &pack(&["f", "v", "g", "w"])).unwrap()
        else {
            panic!("not a hash")
        };
        assert_eq!(hash.get(b"g", 0), Some(&b("w")));
        assert_eq!(hash.len(), 2);
        assert!(blob(TYPE_HASH_LISTPACK, &pack(&["f"])).is_err());

        let Value::ZSet(zset) = blob(TYPE_ZSET_LISTPACK, &pack(&["a", "1.5", "b", "inf"])).unwrap()
        else {
            panic!("not a sorted set")
        };
        assert_eq!(zset.score(b"a"), Some(1.5));
        assert_eq!(zset.score(b"b"), Some(f64::INFINITY));
        assert!(blob(TYPE_ZSET_LISTPACK, &pack(&["a", "nan"])).is_err());

        let Value::Set(set) = blob(TYPE_SET_LISTPACK, &pack(&["x", "y"])).unwrap() else {
            panic!("not a set")
        };
        assert!(set.contains(b"x") && set.contains(b"y"));

        // An intset of 16-bit integers.
        let mut ints = 2u32.to_le_bytes().to_vec();
        ints.extend_from_slice(&3u32.to_le_bytes());
        for n in [-7i16, 0, 300] {
            ints.extend_from_slice(&n.to_le_bytes());
        }
        let Value::Set(set) = blob(TYPE_SET_INTSET, &ints).unwrap() else {
            panic!("not a set")
        };
        assert_eq!(set.len(), 3);
        assert!(set.contains(b"-7") && set.contains(b"300"));
        assert!(blob(TYPE_SET_INTSET, &ints[..ints.len() - 1]).is_err());

        // A quicklist of one packed node and one plain node.
        let mut data = Vec::new();
        put_length(&mut data, 2);
        put_length(&mut data, 2);
        put_string(&mut data, &pack(&["a", "b"]));
        put_length(&mut data, 1);
        put_string(&mut data, b"a large element");
        assert_eq!(
            Reader::new(&data).value(TYPE_LIST_QUICKLIST_2).unwrap(),
            Value::List(VecDeque::from([b("a"), b("b"), b("a large element")]))
        );
    }

    #[test]
    fn unsupported_types_say_what_they_are() {
        for (kind, word) in [
            (15u8, "stream"),
            (7, "module"),
            (22, "release candidate"),
            (13, "older"),
        ] {
            let error = Reader::new(&[0]).value(kind).unwrap_err().to_string();
            assert!(error.contains(word), "{kind}: {error}");
        }
    }

    #[test]
    fn a_huge_count_does_not_allocate() {
        // A list claiming four billion elements in a few bytes.
        let data = [0x80, 0xFF, 0xFF, 0xFF, 0xFF, 1, b'a'];
        assert!(matches!(
            Reader::new(&data).value(TYPE_LIST),
            Err(RdbError::Truncated)
        ));
    }

    proptest! {
        #[test]
        fn arbitrary_bytes_never_panic(data in prop::collection::vec(any::<u8>(), 0..300)) {
            let _ = read_all(&data);
            let _ = restore_value(&data);
            let _ = restore_functions(&data);
            let _ = listpack(&data);
            let _ = intset(&data);
            let mut with_header = b"REDIS0011".to_vec();
            with_header.extend_from_slice(&data);
            let _ = read_all(&with_header);
        }

        #[test]
        fn strings_and_lists_round_trip(
            items in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..40), 0..30),
            expire_at in prop::option::of(any::<u64>()),
        ) {
            let list = Value::List(items.iter().map(|item| Bytes::from(item.clone())).collect());
            let mut writer = Writer::new(Vec::new()).unwrap();
            writer.select_db(1, 1, 0).unwrap();
            writer.entry(b"key", &list, expire_at).unwrap();
            let data = writer.finish().unwrap();
            let items = read_all(&data).unwrap();
            prop_assert_eq!(
                items,
                vec![Item::Entry { db: 1, key: b("key"), value: list.clone(), expire_at }]
            );
            prop_assert_eq!(restore_value(&dump_value(&list)).unwrap(), list);
        }
    }
}
