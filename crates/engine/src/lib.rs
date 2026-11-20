//! The keyspace.
//!
//! Nothing here is thread-safe on purpose: the whole keyspace is owned by the
//! server's single execution thread, so there are no locks or atomics.
//!
//! Time never comes from the system clock inside this crate. Every operation
//! that depends on expiry takes `now` (milliseconds since the Unix epoch) from
//! the caller, which keeps the engine deterministic and testable.

mod db;
mod dict;
mod glob;
mod hash;
mod set;
mod zset;

pub use db::{Candidate, Db, Entry, SetTtl, SnapshotSink, Value};
pub use dict::Dict;
pub use glob::glob_match;
pub use hash::Hash;
pub use set::{Set, INTSET_MAX_ENTRIES};
pub use zset::{LexBound, ScoreBound, ZSet};

use bytes::Bytes;
use std::cell::Cell;
use std::hash::{BuildHasher, RandomState};

/// Sets and sorted sets with more elements than this leave their compact
/// encoding (Redis's `set-` and `zset-max-listpack-entries` default).
pub const COMPACT_MAX_ENTRIES: usize = 128;
/// Hashes with more fields than this leave their compact encoding (Redis's
/// `hash-max-listpack-entries` default).
pub const HASH_COMPACT_MAX_ENTRIES: usize = 512;
/// Collections holding an element longer than this leave their compact
/// encoding (Redis's `*-max-listpack-value` default).
pub const COMPACT_MAX_VALUE: usize = 64;

/// A fast pseudo-random number for sampling (`SRANDMEMBER`, `SPOP`,
/// `HRANDFIELD`, ...). Not cryptographic. Seeded per thread from the
/// operating system's randomness via the standard library's hash keys.
pub fn random_u64() -> u64 {
    thread_local! {
        static STATE: Cell<u64> = Cell::new(RandomState::new().hash_one(0u64) | 1);
    }
    STATE.with(|state| {
        // xorshift64
        let mut x = state.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        state.set(x);
        x
    })
}

/// The fixed set of logical databases selected with `SELECT`.
pub struct Keyspace {
    dbs: Vec<Db>,
}

impl Keyspace {
    pub fn new(databases: usize) -> Self {
        Self {
            dbs: (0..databases.max(1)).map(|_| Db::new()).collect(),
        }
    }

    /// Number of databases.
    pub fn len(&self) -> usize {
        self.dbs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dbs.is_empty()
    }

    /// # Panics
    /// If `index` is out of range. `SELECT` validates the index before it is
    /// ever stored on a client.
    pub fn db(&mut self, index: usize) -> &mut Db {
        &mut self.dbs[index]
    }

    /// Starts a point-in-time snapshot of every database; see
    /// [`Db::begin_snapshot`]. `sink_for` makes the sink for the database
    /// with the given index.
    pub fn begin_snapshot(&mut self, now: u64, mut sink_for: impl FnMut(usize) -> SnapshotSink) {
        for (index, db) in self.dbs.iter_mut().enumerate() {
            db.begin_snapshot(now, sink_for(index));
        }
    }

    /// Does up to `buckets` buckets' worth of snapshot work. Returns `true`
    /// once every database has been walked completely.
    pub fn snapshot_step(&mut self, buckets: usize) -> bool {
        for db in &mut self.dbs {
            if !db.snapshot_step(buckets) {
                return false;
            }
        }
        true
    }

    pub fn end_snapshot(&mut self) {
        for db in &mut self.dbs {
            db.end_snapshot();
        }
    }

    /// See [`Db::set_lfu`]; applies to every database.
    pub fn set_lfu(&mut self, log_factor: u32, decay_minutes: u32) {
        for db in &mut self.dbs {
            db.set_lfu(log_factor, decay_minutes);
        }
    }

    /// Empties every database and returns the contents for the caller to
    /// drop wherever it likes; see [`Db::take_all`].
    pub fn take_all(&mut self) -> Vec<Box<dyn std::any::Any + Send>> {
        self.dbs.iter_mut().map(Db::take_all).collect()
    }

    /// See [`Db::track_expired`]; applies to every database.
    pub fn track_expired(&mut self, on: bool) {
        for db in &mut self.dbs {
            db.track_expired(on);
        }
    }

    /// See [`Db::take_expired`]: `(database index, key)` pairs.
    pub fn take_expired(&mut self) -> Vec<(usize, Bytes)> {
        let mut expired = Vec::new();
        for (index, db) in self.dbs.iter_mut().enumerate() {
            expired.extend(db.take_expired().into_iter().map(|key| (index, key)));
        }
        expired
    }

    /// See [`Db::settle_watches`]; applies to every database.
    pub fn settle_watches(&mut self) {
        for db in &mut self.dbs {
            db.settle_watches();
        }
    }

    /// See [`Db::track_writes`]; applies to every database.
    pub fn track_writes(&mut self, on: bool) {
        for db in &mut self.dbs {
            db.track_writes(on);
        }
    }

    /// See [`Db::take_written`]: `(database index, key)` pairs.
    pub fn take_written(&mut self) -> Vec<(usize, Bytes)> {
        let mut written = Vec::new();
        for (index, db) in self.dbs.iter_mut().enumerate() {
            written.extend(db.take_written().into_iter().map(|key| (index, key)));
        }
        written
    }

    pub fn flush_all(&mut self) {
        for db in &mut self.dbs {
            db.clear();
        }
    }

    /// Background maintenance, called periodically between commands: removes
    /// keys whose TTL has passed and advances any in-progress rehash.
    ///
    /// `max_expired` bounds the work done per database so one call never
    /// stalls command execution for long.
    pub fn cron(&mut self, now: u64, max_expired: usize) -> usize {
        let mut expired = 0;
        for db in &mut self.dbs {
            expired += db.active_expire(now, max_expired);
            db.rehash_steps(64);
        }
        expired
    }
}
