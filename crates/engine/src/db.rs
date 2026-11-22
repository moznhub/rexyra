use std::cell::Cell;
use std::collections::{BTreeSet, HashMap, VecDeque};

use bytes::Bytes;

use crate::dict::{Dict, Pos};
use crate::hash::Hash;
use crate::set::Set;
use crate::zset::ZSet;

/// Something that happened to a key as a side effect of looking it up or
/// writing it, recorded (when asked for) for keyspace notifications and the
/// append-only file; see [`Db::take_notes`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Note {
    /// The key was deleted because its TTL had passed.
    Expired,
    /// A read found no such key (Redis's `keymiss`).
    Missed,
    /// The key was created (Redis's `new`).
    Created,
    /// These fields of the hash at the key were deleted because their TTL
    /// had passed; `emptied`: they were its last, and the hash went too.
    FieldsExpired { fields: Vec<Bytes>, emptied: bool },
    /// The key changed, for client-side caching (recorded only while
    /// [`Db::track_changes`] is on): written, deleted, expired, evicted, or
    /// handed out for writing and not reported [`Db::unchanged`].
    /// `by_server`: the server did it on its own account (expiry, eviction)
    /// rather than as what a command asked for.
    Changed { by_server: bool },
}

/// A stored value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(Bytes),
    List(VecDeque<Bytes>),
    Hash(Hash),
    Set(Set),
    ZSet(ZSet),
}

impl Value {
    /// The name `TYPE` reports.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::List(_) => "list",
            Value::Hash(_) => "hash",
            Value::Set(_) => "set",
            Value::ZSet(_) => "zset",
        }
    }

    /// Whether this is a collection with no elements left. Redis never keeps
    /// such a key: removing the last element removes the key. (An empty
    /// string is a perfectly good value and is not "empty" in this sense.)
    pub fn is_empty_collection(&self) -> bool {
        match self {
            Value::String(_) => false,
            Value::List(list) => list.is_empty(),
            Value::Hash(hash) => hash.is_empty(),
            Value::Set(set) => set.is_empty(),
            Value::ZSet(zset) => zset.is_empty(),
        }
    }
}

/// A value together with its optional expiry.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub value: Value,
    /// Absolute expiry in milliseconds since the Unix epoch.
    expire_at: Option<u64>,
    /// The snapshot (see [`Db::begin_snapshot`]) this entry has been saved
    /// in, or was created during. A `Cell` so that the scan, which only
    /// reads the table, can mark entries as it saves them.
    epoch: Cell<u32>,
    /// When the key was last looked up, in seconds since the Unix epoch.
    accessed: Cell<u32>,
    /// A logarithmic count of how often the key is looked up, which fades
    /// while it is not. See [`Db::set_lfu`].
    frequency: Cell<u8>,
}

/// The frequency a new key starts with, so it is not evicted before it has
/// had a chance to be used.
const INITIAL_FREQUENCY: u8 = 5;

impl Entry {
    fn new(value: Value, expire_at: Option<u64>, epoch: u32, now: u64) -> Entry {
        Entry {
            value,
            expire_at,
            epoch: Cell::new(epoch),
            accessed: Cell::new((now / 1000) as u32),
            frequency: Cell::new(INITIAL_FREQUENCY),
        }
    }

    /// The access frequency as of `seconds`, lowered by one for every
    /// `decay_minutes` the key has gone unused.
    fn decayed_frequency(&self, seconds: u32, decay_minutes: u32) -> u8 {
        let frequency = self.frequency.get();
        if decay_minutes == 0 {
            return frequency;
        }
        let periods = seconds.saturating_sub(self.accessed.get()) / 60 / decay_minutes;
        frequency.saturating_sub(periods.min(255) as u8)
    }
}

/// A key that could be evicted, with what the eviction policies go by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub key: Bytes,
    /// Seconds since the key was last looked up.
    pub idle_seconds: u64,
    pub frequency: u8,
    pub expire_at: Option<u64>,
}

/// Receives the entries of a snapshot: key, value and expiry.
///
/// The last argument says whether the entry has to be taken now, because it
/// is about to change or go. If it is `false` (the entry was merely reached
/// by the walk) the sink may return `false` to leave it for later; it must
/// then come back for it with [`Db::unsaved`] and [`Db::mark_saved`] before
/// the snapshot ends. Should the entry be about to change in the meantime,
/// the sink is called again with `true`.
pub type SnapshotSink = Box<dyn FnMut(&Bytes, &Value, Option<u64>, bool) -> bool + Send>;

/// A point-in-time snapshot in progress.
struct Snapshot {
    /// The instant the snapshot describes.
    started_at: u64,
    sink: SnapshotSink,
    /// Where the scan of the table has got to.
    cursor: u64,
    scanned: bool,
}

impl Snapshot {
    /// Hands `entry` to the sink unless this snapshot already has it (or it
    /// was created after the snapshot began). With `now` false the sink may
    /// leave it for later.
    fn save(&mut self, epoch: u32, key: &Bytes, entry: &Entry, now: bool) {
        if entry.epoch.get() == epoch {
            return;
        }
        // An entry that had already expired when the snapshot began is not
        // part of it.
        if entry.is_expired(self.started_at) || (self.sink)(key, &entry.value, entry.expire_at, now)
        {
            entry.epoch.set(epoch);
        }
    }
}

impl Entry {
    pub fn expire_at(&self) -> Option<u64> {
        self.expire_at
    }

    pub fn is_expired(&self, now: u64) -> bool {
        self.expire_at.is_some_and(|at| at <= now)
    }
}

/// What happens to a key's TTL when its value is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetTtl {
    /// Leave any existing TTL untouched (`INCR`, `APPEND`, `SET ... KEEPTTL`).
    Keep,
    /// Remove any TTL (plain `SET`).
    Clear,
    /// Expire at this absolute time in milliseconds.
    At(u64),
}

/// One logical database.
///
/// Expiry is enforced two ways, as in Redis:
///
/// * **lazily** — every access checks the key's TTL first and deletes the key
///   if it has passed, so an expired key is never observable;
/// * **actively** — [`Db::active_expire`] removes expired keys that nobody is
///   reading, so they do not hold memory forever.
///
/// Unlike Redis, which samples random keys and may leave expired ones around
/// for a while, active expiry here is exact: an index ordered by expiry time
/// always knows which key is due next.
///
/// Hash fields with a TTL of their own are reclaimed the same way, through
/// a second index: of the hashes that have field TTLs, each under a time no
/// later than its earliest one. A TTL that goes away (`HPERSIST`, `HSET`)
/// leaves the hash where it is; the reclaimer finds out when it gets there
/// and moves the hash to its next field TTL, or out of the index (Redis's
/// `subexpiry` count in `INFO keyspace` behaves the same).
#[derive(Default)]
pub struct Db {
    dict: Dict<Entry>,
    /// Every key with a TTL, ordered by expiry time. Kept in step with the
    /// `expire_at` field of the corresponding entry.
    expires: BTreeSet<(u64, Bytes)>,
    /// Hashes with field TTLs, each under the time its
    /// [`Hash::indexed_at`] says, which is never later than its earliest
    /// field TTL.
    field_index: BTreeSet<(u64, Bytes)>,
    /// Fields deleted because their TTL passed, ever.
    expired_fields: u64,
    /// Whether to record written keys in `written`.
    track_writes: bool,
    /// Keys created or overwritten since the last [`Db::take_written`].
    written: Vec<Bytes>,
    /// Keys some client is watching (`WATCH`), whether or not they exist.
    watched: HashMap<Bytes, Watched>,
    /// Watched keys handed out for writing since the last
    /// [`Db::settle_watches`].
    pending: Vec<Bytes>,
    /// Whether to record expired keys, lookups that find nothing, and
    /// created keys in `notes`.
    track_expired: bool,
    note_misses: bool,
    note_created: bool,
    /// What happened to keys since the last [`Db::take_notes`], in order.
    notes: Vec<(Note, Bytes)>,
    /// Whether to note changed keys ([`Note::Changed`]).
    track_changes: bool,
    /// Keys handed out for writing since the last [`Db::take_notes`], noted
    /// as changed then unless [`Db::unchanged`] takes them back first.
    maybe_changed: Vec<Bytes>,
    /// How quickly the access-frequency counter saturates (higher: slower)
    /// and after how many unused minutes it drops by one.
    lfu_log_factor: u32,
    lfu_decay_minutes: u32,
    /// Number of the latest snapshot. Entries tagged with it need no saving.
    epoch: u32,
    snapshot: Option<Snapshot>,
}

#[derive(Default)]
struct Watched {
    /// How many watches are registered on the key.
    watchers: u32,
    /// Incremented every time the key changes.
    version: u64,
    /// The value was handed out for writing; unless [`Db::unchanged`] says
    /// otherwise, that counts as a change at the next
    /// [`Db::settle_watches`].
    maybe_changed: bool,
}

impl Db {
    pub fn new() -> Self {
        Self {
            lfu_log_factor: 10,
            lfu_decay_minutes: 1,
            ..Self::default()
        }
    }

    /// Tunes the access-frequency counter; see Redis's `lfu-log-factor` and
    /// `lfu-decay-time`.
    pub fn set_lfu(&mut self, log_factor: u32, decay_minutes: u32) {
        self.lfu_log_factor = log_factor;
        self.lfu_decay_minutes = decay_minutes;
    }

    /// Records a lookup of the entry at `pos`: now is its last access, and
    /// its frequency counter may go up. The counter is logarithmic: the
    /// higher it already is, the less likely one more access raises it.
    fn mark_access(&self, pos: Pos, now: u64) {
        let entry = self.dict.at(pos).1;
        let seconds = (now / 1000) as u32;
        let mut frequency = entry.decayed_frequency(seconds, self.lfu_decay_minutes);
        if frequency < u8::MAX {
            let above_initial = f64::from(frequency.saturating_sub(INITIAL_FREQUENCY));
            let chance = 1.0 / (above_initial * f64::from(self.lfu_log_factor) + 1.0);
            if (crate::random_u64() >> 11) as f64 / (1u64 << 53) as f64 <= chance {
                frequency += 1;
            }
        }
        entry.frequency.set(frequency);
        entry.accessed.set(seconds);
    }

    fn candidate(&self, key: &Bytes, entry: &Entry, now: u64) -> Candidate {
        let seconds = (now / 1000) as u32;
        Candidate {
            key: key.clone(),
            idle_seconds: u64::from(seconds.saturating_sub(entry.accessed.get())),
            frequency: entry.decayed_frequency(seconds, self.lfu_decay_minutes),
            expire_at: entry.expire_at,
        }
    }

    /// How long ago `key` was last looked up and how often it is, without
    /// counting as a lookup itself. `None` if the key does not exist.
    pub fn access_info(&self, key: &[u8], now: u64) -> Option<Candidate> {
        let pos = self.dict.find(key)?;
        let (key, entry) = self.dict.at(pos);
        (!entry.is_expired(now)).then(|| self.candidate(key, entry, now))
    }

    /// A key picked at random, as a candidate for eviction. With
    /// `volatile_only`, one of the keys that have a TTL.
    pub fn sample(&self, volatile_only: bool, now: u64) -> Option<Candidate> {
        if !volatile_only {
            let (key, entry) = self
                .dict
                .random_entry(crate::random_u64(), crate::random_u64())?;
            return Some(self.candidate(key, entry, now));
        }
        // Keys with a TTL are indexed by expiry time: pick a random time in
        // the range they span and take a key that expires next to it, on
        // either side, so that the first and the last can be picked too.
        let (first, _) = self.expires.first()?;
        let (last, _) = self.expires.last()?;
        let at = first + crate::random_u64() % (last - first + 1);
        let after = crate::random_u64() & 1 == 0;
        let (_, key) = if after {
            self.expires.range((at, Bytes::new())..).next()
        } else {
            self.expires
                .range(..(at.saturating_add(1), Bytes::new()))
                .next_back()
        }
        .or_else(|| self.expires.first())?;
        let entry = self.dict.get(key)?;
        Some(self.candidate(key, entry, now))
    }

    /// The key whose TTL runs out first.
    pub fn soonest_to_expire(&self, now: u64) -> Option<Candidate> {
        let (_, key) = self.expires.first()?;
        let entry = self.dict.get(key)?;
        Some(self.candidate(key, entry, now))
    }

    /// Empties the database like [`Db::clear`], but hands its contents back
    /// instead of freeing them, so that the caller can drop them on another
    /// thread: freeing millions of keys takes a while.
    pub fn take_all(&mut self) -> Box<dyn std::any::Any + Send> {
        self.before_clear();
        let dict = std::mem::take(&mut self.dict);
        let expires = std::mem::take(&mut self.expires);
        let field_index = std::mem::take(&mut self.field_index);
        Box::new((dict, expires, field_index))
    }

    /// Turns recording of created and overwritten keys on or off. The server
    /// turns it on while clients are blocked waiting for a key to appear, so
    /// the bookkeeping costs nothing the rest of the time.
    pub fn track_writes(&mut self, on: bool) {
        self.track_writes = on;
        if !on {
            self.written.clear();
        }
    }

    /// The keys created or overwritten since the last call, in order.
    /// Changes inside an existing collection are not recorded.
    pub fn take_written(&mut self) -> Vec<Bytes> {
        std::mem::take(&mut self.written)
    }

    /// Starts a point-in-time snapshot of this database as of `now`.
    ///
    /// Every entry that exists at this moment reaches `sink` exactly once,
    /// in the state it has now, however the database changes afterwards:
    ///
    /// * [`Db::snapshot_step`] walks the table a little at a time, saving
    ///   entries it has not saved yet;
    /// * every method that changes or removes an entry saves it first, if
    ///   the walk has not reached it.
    ///
    /// Entries created after this call are not part of the snapshot. Call
    /// [`Db::end_snapshot`] when the walk is complete, or to abandon it.
    pub fn begin_snapshot(&mut self, now: u64, sink: SnapshotSink) {
        self.epoch = self.epoch.wrapping_add(1);
        self.snapshot = Some(Snapshot {
            started_at: now,
            sink,
            cursor: 0,
            scanned: self.dict.is_empty(),
        });
    }

    /// Saves the entries in the next `buckets` buckets of the table. Returns
    /// `true` once the whole table has been walked (or no snapshot is
    /// running).
    pub fn snapshot_step(&mut self, buckets: usize) -> bool {
        let Db {
            dict,
            snapshot: Some(snapshot),
            epoch,
            ..
        } = self
        else {
            return true;
        };
        for _ in 0..buckets {
            if snapshot.scanned {
                break;
            }
            let cursor = snapshot.cursor;
            snapshot.cursor = dict.scan(cursor, |key, entry| {
                snapshot.save(*epoch, key, entry, false)
            });
            snapshot.scanned = snapshot.cursor == 0;
        }
        snapshot.scanned
    }

    pub fn end_snapshot(&mut self) {
        self.snapshot = None;
    }

    /// The entry at `key` if the running snapshot still has to save it:
    /// one its sink left for later. `None` if there is no such entry or no
    /// snapshot.
    pub fn unsaved(&self, key: &[u8]) -> Option<(&Value, Option<u64>)> {
        let snapshot = self.snapshot.as_ref()?;
        let entry = self.dict.get(key)?;
        (entry.epoch.get() != self.epoch && !entry.is_expired(snapshot.started_at))
            .then_some((&entry.value, entry.expire_at))
    }

    /// Records that the running snapshot has saved the entry at `key`.
    pub fn mark_saved(&mut self, key: &[u8]) {
        if self.snapshot.is_some() {
            if let Some(entry) = self.dict.get(key) {
                entry.epoch.set(self.epoch);
            }
        }
    }

    pub fn is_snapshotting(&self) -> bool {
        self.snapshot.is_some()
    }

    /// Saves the entry at `pos` to the running snapshot, if it still needs
    /// it. Called before anything changes or removes an entry.
    fn preserve(&mut self, pos: Pos) {
        if let Some(snapshot) = &mut self.snapshot {
            let (key, entry) = self.dict.at(pos);
            snapshot.save(self.epoch, key, entry, true);
        }
    }

    fn preserve_key(&mut self, key: &[u8]) {
        if self.snapshot.is_some() {
            if let Some(pos) = self.dict.find(key) {
                self.preserve(pos);
            }
        }
    }

    /// Starts watching `key` and returns its current version. The key has
    /// changed since if [`Db::watch_version`] returns anything else.
    ///
    /// Creation, replacement, deletion, expiry and a change of TTL count as
    /// changes. So does handing out the value for writing ([`Db::get_mut`],
    /// [`Db::get_or_insert_with`]), unless the caller reports through
    /// [`Db::unchanged`] that it modified nothing. Each call must be paired
    /// with one [`Db::unwatch`].
    pub fn watch(&mut self, key: &[u8], now: u64) -> u64 {
        self.settle_watches();
        // Collect an already expired key first, so that its removal later is
        // not mistaken for a change.
        self.live(key, now);
        let watched = self.watched.entry(Bytes::copy_from_slice(key)).or_default();
        watched.watchers += 1;
        watched.version
    }

    pub fn unwatch(&mut self, key: &[u8]) {
        if let Some(watched) = self.watched.get_mut(key) {
            watched.watchers -= 1;
            if watched.watchers == 0 {
                self.watched.remove(key);
            }
        }
    }

    /// The current version of a watched key; `None` if nobody watches it.
    pub fn watch_version(&mut self, key: &[u8], now: u64) -> Option<u64> {
        self.settle_watches();
        // A key whose TTL has passed has changed, even if nothing has
        // touched it yet.
        self.live(key, now);
        self.watched.get(key).map(|watched| watched.version)
    }

    /// Exchanges the keys of two databases, with their TTLs and any snapshot
    /// in progress, leaving each database's watchers and settings in place.
    /// A watched key present in either database counts as changed in both.
    pub(crate) fn swap_contents(a: &mut Db, b: &mut Db) {
        std::mem::swap(&mut a.dict, &mut b.dict);
        std::mem::swap(&mut a.expires, &mut b.expires);
        std::mem::swap(&mut a.field_index, &mut b.field_index);
        std::mem::swap(&mut a.epoch, &mut b.epoch);
        std::mem::swap(&mut a.snapshot, &mut b.snapshot);
        Db::touch_swapped(a, b);
        Db::touch_swapped(b, a);
    }

    fn touch_swapped(this: &mut Db, other: &Db) {
        this.settle_watches();
        for (key, watched) in &mut this.watched {
            if this.dict.find(key).is_some() || other.dict.find(key).is_some() {
                watched.version += 1;
            }
        }
    }

    pub(crate) fn is_tracking_writes(&self) -> bool {
        self.track_writes
    }

    /// Records that `key` may have changed, for the benefit of watchers
    /// and of client-side caching.
    fn touch(&mut self, key: &[u8]) {
        self.touch_as(key, false);
    }

    /// [`Db::touch`], saying whether the server changed the key on its own
    /// account (expiry, eviction).
    fn touch_as(&mut self, key: &[u8], by_server: bool) {
        if !self.watched.is_empty() {
            if let Some(watched) = self.watched.get_mut(key) {
                watched.version += 1;
            }
        }
        if self.track_changes {
            self.notes
                .push((Note::Changed { by_server }, Bytes::copy_from_slice(key)));
        }
    }

    /// Records that `key`'s value was handed out for writing.
    fn touch_later(&mut self, key: &[u8]) {
        if !self.watched.is_empty() {
            if let Some(watched) = self.watched.get_mut(key) {
                if !watched.maybe_changed {
                    watched.maybe_changed = true;
                    self.pending.push(Bytes::copy_from_slice(key));
                }
            }
        }
        if self.track_changes && !self.maybe_changed.iter().any(|kept| kept == key) {
            self.maybe_changed.push(Bytes::copy_from_slice(key));
        }
    }

    /// Reports that the command now running obtained `key` for writing but
    /// did not modify it (`SREM` of a member that is not there, say), so
    /// watchers need not be disturbed, nor clients that cache it. Must come
    /// before the command raises any keyspace notification, which notes
    /// the keys handed out so far as changed.
    pub fn unchanged(&mut self, key: &[u8]) {
        if !self.watched.is_empty() {
            if let Some(watched) = self.watched.get_mut(key) {
                watched.maybe_changed = false;
            }
        }
        if !self.maybe_changed.is_empty() {
            self.maybe_changed.retain(|kept| kept != key);
        }
    }

    /// Turns recording of changed keys ([`Note::Changed`]) on or off, for
    /// client-side caching. Off (the default), a change costs one test of
    /// this flag.
    pub fn track_changes(&mut self, on: bool) {
        self.track_changes = on;
        if !on {
            self.maybe_changed.clear();
            self.notes
                .retain(|(note, _)| !matches!(note, Note::Changed { .. }));
        }
    }

    /// Turns the write accesses since the last call into changes. Call after
    /// every command.
    pub fn settle_watches(&mut self) {
        for key in self.pending.drain(..) {
            if let Some(watched) = self.watched.get_mut(&key) {
                if std::mem::take(&mut watched.maybe_changed) {
                    watched.version += 1;
                }
            }
        }
    }

    /// Turns recording of expired keys on or off. The append-only file needs
    /// to know when a key is deleted by its TTL, because on replay nothing
    /// expires by itself: the log says when each key went. So do keyspace
    /// notifications.
    pub fn track_expired(&mut self, on: bool) {
        self.track_expired = on;
        if !on {
            self.notes.retain(|(note, _)| *note != Note::Expired);
        }
    }

    /// Turns recording of lookups that find no key on or off. The commands
    /// layer turns it on around the lookups Redis counts as reads.
    pub fn note_misses(&mut self, on: bool) {
        self.note_misses = on;
    }

    /// Turns recording of created keys on or off.
    pub fn note_created(&mut self, on: bool) {
        self.note_created = on;
        if !on {
            self.notes.retain(|(note, _)| *note != Note::Created);
        }
    }

    /// What happened to keys since the last call, in order. Keys handed
    /// out for writing since then count as changed now.
    pub fn take_notes(&mut self) -> Vec<(Note, Bytes)> {
        for key in self.maybe_changed.drain(..) {
            self.notes.push((Note::Changed { by_server: false }, key));
        }
        std::mem::take(&mut self.notes)
    }

    /// Whether anything has been noted since the last [`Db::take_notes`].
    pub fn has_notes(&self) -> bool {
        !self.notes.is_empty() || !self.maybe_changed.is_empty()
    }

    fn note(&mut self, note: Note, key: &[u8]) {
        self.notes.push((note, Bytes::copy_from_slice(key)));
    }

    fn note_written(&mut self, key: &[u8]) {
        if self.track_writes {
            self.written.push(Bytes::copy_from_slice(key));
        }
    }

    /// Number of keys, including any that have expired but have not been
    /// collected yet (the same approximation `DBSIZE` makes in Redis).
    pub fn len(&self) -> usize {
        self.dict.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dict.is_empty()
    }

    /// Number of keys that have a TTL.
    pub fn volatile_len(&self) -> usize {
        self.expires.len()
    }

    /// Number of hashes with field TTLs (`subexpiry` in `INFO keyspace`).
    /// As in Redis, a hash whose field TTLs have all been removed is
    /// counted until the reclaimer gets to it.
    pub fn volatile_hashes(&self) -> usize {
        self.field_index.len()
    }

    /// Number of hash fields deleted because their TTL passed, since the
    /// database was created or [`Db::reset_expired_fields`].
    pub fn expired_fields(&self) -> u64 {
        self.expired_fields
    }

    pub fn reset_expired_fields(&mut self) {
        self.expired_fields = 0;
    }

    /// What has to happen before every key goes at once: a running snapshot
    /// saves them, and watchers of keys that exist are told.
    fn before_clear(&mut self) {
        if let Some(snapshot) = &mut self.snapshot {
            for (key, entry) in self.dict.iter() {
                snapshot.save(self.epoch, key, entry, true);
            }
        }
        for (key, watched) in &mut self.watched {
            if self.dict.get(key).is_some() {
                watched.version += 1;
            }
        }
    }

    pub fn clear(&mut self) {
        self.before_clear();
        self.dict.clear();
        self.expires.clear();
        self.field_index.clear();
    }

    /// Locates a live key, deleting it first if its TTL has passed.
    fn live(&mut self, key: &[u8], now: u64) -> Option<Pos> {
        self.dict.rehash_step(1);
        let Some(pos) = self.dict.find(key) else {
            if self.note_misses {
                self.note(Note::Missed, key);
            }
            return None;
        };
        if self.dict.at(pos).1.is_expired(now) {
            if self.track_expired {
                self.note(Note::Expired, key);
            }
            self.remove_entry(key, true);
            if self.note_misses {
                self.note(Note::Missed, key);
            }
            return None;
        }
        self.mark_access(pos, now);
        Some(pos)
    }

    fn remove_entry(&mut self, key: &[u8], by_server: bool) -> Option<Entry> {
        self.preserve_key(key);
        let (key, entry) = self.dict.remove(key)?;
        self.touch_as(&key, by_server);
        unindex_fields(&mut self.field_index, &key, &entry.value);
        if let Some(at) = entry.expire_at {
            self.expires.remove(&(at, key));
        }
        Some(entry)
    }

    pub fn get(&mut self, key: &[u8], now: u64) -> Option<&Value> {
        let pos = self.live(key, now)?;
        Some(&self.dict.at(pos).1.value)
    }

    pub fn get_mut(&mut self, key: &[u8], now: u64) -> Option<&mut Value> {
        let pos = self.live(key, now)?;
        self.touch_later(key);
        self.preserve(pos);
        Some(&mut self.dict.at_mut(pos).1.value)
    }

    pub fn exists(&mut self, key: &[u8], now: u64) -> bool {
        self.live(key, now).is_some()
    }

    /// The key's expiry: `None` if the key does not exist, `Some(None)` if it
    /// exists without a TTL, `Some(Some(at))` otherwise.
    pub fn expire_at(&mut self, key: &[u8], now: u64) -> Option<Option<u64>> {
        let pos = self.live(key, now)?;
        Some(self.dict.at(pos).1.expire_at)
    }

    /// Creates or overwrites `key`.
    ///
    /// The key is copied into its own allocation: callers usually hold a
    /// slice of a network buffer, and storing that slice would keep the whole
    /// buffer alive for as long as the key exists.
    pub fn set(&mut self, key: &[u8], mut value: Value, ttl: SetTtl, now: u64) {
        self.note_written(key);
        // An expired key goes first; then the change is the new value's.
        let pos = self.live(key, now);
        self.touch(key);
        match pos {
            Some(pos) => {
                self.preserve(pos);
                let (stored_key, entry) = self.dict.at_mut(pos);
                unindex_fields(&mut self.field_index, stored_key, &entry.value);
                index_fields(&mut self.field_index, stored_key, &mut value);
                entry.value = value;
                let new_expiry = match ttl {
                    SetTtl::Keep => return,
                    SetTtl::Clear => None,
                    SetTtl::At(at) => Some(at),
                };
                let old_expiry = std::mem::replace(&mut entry.expire_at, new_expiry);
                if old_expiry != new_expiry {
                    let stored_key = stored_key.clone();
                    self.reindex(stored_key, old_expiry, new_expiry);
                }
            }
            None => {
                if self.note_created {
                    self.note(Note::Created, key);
                }
                let key = Bytes::copy_from_slice(key);
                let expire_at = match ttl {
                    SetTtl::At(at) => Some(at),
                    SetTtl::Keep | SetTtl::Clear => None,
                };
                if let Some(at) = expire_at {
                    self.expires.insert((at, key.clone()));
                }
                index_fields(&mut self.field_index, &key, &mut value);
                let entry = Entry::new(value, expire_at, self.epoch, now);
                self.dict.insert(key, entry);
            }
        }
    }

    fn reindex(&mut self, key: Bytes, old: Option<u64>, new: Option<u64>) {
        match (old, new) {
            (Some(old), Some(new)) => {
                self.expires.remove(&(old, key.clone()));
                self.expires.insert((new, key));
            }
            (Some(old), None) => {
                self.expires.remove(&(old, key));
            }
            (None, Some(new)) => {
                self.expires.insert((new, key));
            }
            (None, None) => {}
        }
    }

    /// The value at `key` without the side effects of [`Db::get`]: an
    /// expired key reads as missing but is not deleted. Because this only
    /// needs shared access, several values can be borrowed at once (set
    /// algebra reads many keys together).
    pub fn peek(&self, key: &[u8], now: u64) -> Option<&Value> {
        self.dict
            .get(key)
            .filter(|entry| !entry.is_expired(now))
            .map(|entry| &entry.value)
    }

    /// The value at `key`, created with `make` if the key does not exist.
    /// An existing value is returned as is, whatever its type, with its TTL
    /// untouched.
    pub fn get_or_insert_with(
        &mut self,
        key: &[u8],
        now: u64,
        make: impl FnOnce() -> Value,
    ) -> &mut Value {
        let pos = match self.live(key, now) {
            Some(pos) => {
                self.touch_later(key);
                self.preserve(pos);
                pos
            }
            None => {
                self.touch(key);
                self.note_written(key);
                let key = Bytes::copy_from_slice(key);
                if self.note_created {
                    self.notes.push((Note::Created, key.clone()));
                }
                let mut value = make();
                index_fields(&mut self.field_index, &key, &mut value);
                let entry = Entry::new(value, None, self.epoch, now);
                self.dict.insert(key.clone(), entry);
                self.dict
                    .find(&key)
                    .expect("the key was inserted on the line above")
            }
        };
        &mut self.dict.at_mut(pos).1.value
    }

    /// Deletes `key` if it holds a collection that has become empty. Every
    /// command that removes elements calls this afterwards. Returns whether
    /// it deleted the key.
    pub fn remove_if_empty(&mut self, key: &[u8], now: u64) -> bool {
        if self.get(key, now).is_some_and(Value::is_empty_collection) {
            self.remove_entry(key, false);
            return true;
        }
        false
    }

    /// Sets (`Some`) or removes (`None`) the TTL of an existing key.
    /// Returns `false` if the key does not exist.
    pub fn set_expire(&mut self, key: &[u8], expire_at: Option<u64>, now: u64) -> bool {
        let Some(pos) = self.live(key, now) else {
            return false;
        };
        self.preserve(pos);
        let (stored_key, entry) = self.dict.at_mut(pos);
        let old = std::mem::replace(&mut entry.expire_at, expire_at);
        let had_or_has_ttl = old.is_some() || expire_at.is_some();
        if old != expire_at {
            let stored_key = stored_key.clone();
            self.reindex(stored_key, old, expire_at);
        }
        // Setting a TTL counts as a change even if it is the same TTL.
        if had_or_has_ttl {
            self.touch(key);
        }
        true
    }

    /// Removes a key, returning its value and expiry if it was live.
    pub fn take(&mut self, key: &[u8], now: u64) -> Option<(Value, Option<u64>)> {
        self.live(key, now)?;
        self.remove_entry(key, false)
            .map(|entry| (entry.value, entry.expire_at))
    }

    /// Removes a key to free memory (eviction). Returns whether a live key
    /// was removed.
    pub fn evict(&mut self, key: &[u8], now: u64) -> bool {
        self.live(key, now).is_some() && self.remove_entry(key, true).is_some()
    }

    /// Removes a key. Returns whether a live key was removed.
    pub fn remove(&mut self, key: &[u8], now: u64) -> bool {
        self.take(key, now).is_some()
    }

    /// Cursor iteration for `SCAN`; see [`Dict::scan`] for the guarantees.
    /// Expired keys are passed through — check [`Entry::is_expired`].
    pub fn scan(&self, cursor: u64, visit: impl FnMut(&Bytes, &Entry)) -> u64 {
        self.dict.scan(cursor, visit)
    }

    /// Every entry, including expired ones not yet collected.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &Entry)> {
        self.dict.iter()
    }

    /// A random live key.
    pub fn random_key(&mut self, now: u64) -> Option<Bytes> {
        // Retry when the pick turns out to be expired (which also deletes
        // it). Bounded so a database full of expired keys cannot spin.
        for _ in 0..100 {
            let key = self.dict.random_key()?.clone();
            if self.exists(&key, now) {
                return Some(key);
            }
        }
        None
    }

    /// Deletes up to `max` keys whose TTL has passed. Returns how many.
    pub fn active_expire(&mut self, now: u64, max: usize) -> usize {
        let mut removed = 0;
        while removed < max {
            match self.expires.first() {
                Some((at, _)) if *at <= now => {}
                _ => break,
            }
            if let Some((_, key)) = self.expires.pop_first() {
                self.preserve_key(&key);
                if self.track_expired {
                    self.notes.push((Note::Expired, key.clone()));
                }
                // After the expiry is noted: Redis announces `expired`
                // before it invalidates cached copies.
                self.touch_as(&key, true);
                if let Some((_, entry)) = self.dict.remove(&key) {
                    unindex_fields(&mut self.field_index, &key, &entry.value);
                }
                removed += 1;
            }
        }
        removed
    }

    /// Records that field TTLs of the hash at `key` were set, so that the
    /// reclaimer knows when to come for them. Call after setting any.
    pub fn index_field_ttls(&mut self, key: &[u8]) {
        let Some(pos) = self.dict.find(key) else {
            return;
        };
        let (stored_key, entry) = self.dict.at_mut(pos);
        let Value::Hash(hash) = &mut entry.value else {
            return;
        };
        let Some(next) = hash.next_field_expiry() else {
            return;
        };
        let Some(ttls) = hash.ttls_mut() else {
            return;
        };
        match ttls.indexed_at {
            Some(at) if at <= next => {}
            old => {
                if let Some(old) = old {
                    self.field_index.remove(&(old, stored_key.clone()));
                }
                self.field_index.insert((next, stored_key.clone()));
                ttls.indexed_at = Some(next);
            }
        }
    }

    /// Deletes fields of the hash at `key` whose TTL has passed: those
    /// among `fields`, or every one if `None`. A command does this before
    /// it looks such a field up, as Redis does, and it counts as a change
    /// of the key. The key goes too if they were its last fields. Returns
    /// how many fields were deleted.
    pub fn expire_fields(&mut self, key: &[u8], fields: Option<&[Bytes]>, now: u64) -> usize {
        let Some(pos) = self.live(key, now) else {
            return 0;
        };
        let Value::Hash(hash) = &self.dict.at(pos).1.value else {
            return 0;
        };
        if !hash.has_expired_fields(now) {
            return 0;
        }
        let due: Option<Vec<Bytes>> = fields.map(|fields| {
            let mut due: Vec<Bytes> = Vec::new();
            for field in fields {
                if hash.is_field_expired(field, now) && !due.contains(field) {
                    due.push(field.clone());
                }
            }
            due
        });
        if due.as_ref().is_some_and(Vec::is_empty) {
            return 0;
        }
        self.preserve(pos);
        let Value::Hash(hash) = &mut self.dict.at_mut(pos).1.value else {
            return 0;
        };
        let removed = match due {
            Some(due) => {
                for field in &due {
                    hash.remove(field);
                }
                due
            }
            None => hash.remove_expired(now, usize::MAX),
        };
        let emptied = hash.is_empty();
        let count = removed.len();
        self.after_fields_expired(key, count, emptied, removed);
        self.touch_as(key, true);
        count
    }

    /// Deletes up to `max` hash fields whose TTL has passed, earliest
    /// first, and hashes left empty by that. Returns how many fields.
    pub fn active_expire_fields(&mut self, now: u64, max: usize) -> usize {
        let mut removed = 0;
        while removed < max {
            match self.field_index.first() {
                Some((at, _)) if *at <= now => {}
                _ => break,
            }
            let Some((at, key)) = self.field_index.pop_first() else {
                break;
            };
            let Some(pos) = self.dict.find(&key) else {
                continue;
            };
            let entry = self.dict.at(pos).1;
            match &entry.value {
                // An entry left behind by a value that has since been
                // replaced or moved under another time.
                Value::Hash(hash) if hash.indexed_at() == Some(at) => {}
                _ => continue,
            }
            if entry.is_expired(now) {
                // The whole key is going; key expiry sees to it.
                if let Value::Hash(hash) = &mut self.dict.at_mut(pos).1.value {
                    if let Some(ttls) = hash.ttls_mut() {
                        ttls.indexed_at = None;
                    }
                }
                continue;
            }
            self.preserve(pos);
            let (stored_key, entry) = self.dict.at_mut(pos);
            let Value::Hash(hash) = &mut entry.value else {
                continue;
            };
            let fields = hash.remove_expired(now, max - removed);
            removed += fields.len();
            let next = hash.next_field_expiry();
            let emptied = hash.is_empty();
            if let Some(ttls) = hash.ttls_mut() {
                ttls.indexed_at = next;
            }
            if let Some(next) = next {
                self.field_index.insert((next, stored_key.clone()));
            }
            if !fields.is_empty() {
                let count = fields.len();
                self.after_fields_expired(&key, count, emptied, fields);
                // A change of the key, for watchers and cached copies, as
                // in Redis, which announces `hexpired` first.
                self.touch_as(&key, true);
            }
        }
        removed
    }

    /// Bookkeeping after fields of the hash at `key` were deleted by
    /// expiry: counted, recorded, and the key deleted if it was left empty.
    fn after_fields_expired(
        &mut self,
        key: &[u8],
        count: usize,
        emptied: bool,
        fields: Vec<Bytes>,
    ) {
        self.expired_fields += count as u64;
        if self.track_expired && count > 0 {
            self.notes.push((
                Note::FieldsExpired { fields, emptied },
                Bytes::copy_from_slice(key),
            ));
        }
        if emptied {
            self.remove_entry(key, true);
        }
    }

    /// Advances an in-progress incremental rehash; see [`Dict::rehash_step`].
    pub fn rehash_steps(&mut self, buckets: usize) {
        self.dict.rehash_step(buckets);
    }
}

/// Puts a value about to be stored at `key` into the index of hashes with
/// field TTLs, if it is one. A value may be a copy of another key's, so
/// whatever it says about the index is reset first.
fn index_fields(index: &mut BTreeSet<(u64, Bytes)>, key: &Bytes, value: &mut Value) {
    let Value::Hash(hash) = value else {
        return;
    };
    let next = hash.next_field_expiry();
    let Some(ttls) = hash.ttls_mut() else {
        return;
    };
    ttls.indexed_at = next;
    if let Some(next) = next {
        index.insert((next, key.clone()));
    }
}

/// Takes a value stored at `key`, which is about to go, out of the index of
/// hashes with field TTLs.
fn unindex_fields(index: &mut BTreeSet<(u64, Bytes)>, key: &Bytes, value: &Value) {
    if let Value::Hash(hash) = value {
        if let Some(at) = hash.indexed_at() {
            index.remove(&(at, key.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Hash;
    use proptest::prelude::*;
    use std::collections::HashMap;

    fn s(text: &str) -> Value {
        Value::String(Bytes::copy_from_slice(text.as_bytes()))
    }

    /// The index and the per-entry field must always describe the same TTLs.
    fn assert_consistent(db: &Db) {
        let from_entries: BTreeSet<(u64, Bytes)> = db
            .iter()
            .filter_map(|(k, e)| e.expire_at.map(|at| (at, k.clone())))
            .collect();
        assert_eq!(db.expires, from_entries);
    }

    #[test]
    fn set_get_remove() {
        let mut db = Db::new();
        assert_eq!(db.get(b"a", 0), None);
        db.set(b"a", s("1"), SetTtl::Clear, 0);
        assert_eq!(db.get(b"a", 0), Some(&s("1")));
        assert!(db.exists(b"a", 0));
        assert_eq!(db.len(), 1);
        assert!(db.remove(b"a", 0));
        assert!(!db.remove(b"a", 0));
        assert!(db.is_empty());
    }

    #[test]
    fn lazy_expiry_hides_and_deletes() {
        let mut db = Db::new();
        db.set(b"a", s("1"), SetTtl::At(100), 0);
        assert_eq!(db.get(b"a", 99), Some(&s("1")));
        assert_eq!(db.len(), 1);
        // The TTL instant itself counts as expired.
        assert_eq!(db.get(b"a", 100), None);
        assert_eq!(db.len(), 0);
        assert_eq!(db.volatile_len(), 0);
    }

    #[test]
    fn ttl_policies_on_overwrite() {
        let mut db = Db::new();
        db.set(b"a", s("1"), SetTtl::At(100), 0);

        db.set(b"a", s("2"), SetTtl::Keep, 0);
        assert_eq!(db.expire_at(b"a", 0), Some(Some(100)));

        db.set(b"a", s("3"), SetTtl::At(200), 0);
        assert_eq!(db.expire_at(b"a", 0), Some(Some(200)));
        assert_consistent(&db);

        db.set(b"a", s("4"), SetTtl::Clear, 0);
        assert_eq!(db.expire_at(b"a", 0), Some(None));
        assert_eq!(db.volatile_len(), 0);

        // `Keep` on a key that does not exist creates it without a TTL.
        db.set(b"b", s("1"), SetTtl::Keep, 0);
        assert_eq!(db.expire_at(b"b", 0), Some(None));
        assert_eq!(db.expire_at(b"missing", 0), None);
    }

    #[test]
    fn overwriting_an_expired_key_does_not_inherit_its_ttl() {
        let mut db = Db::new();
        db.set(b"a", s("old"), SetTtl::At(100), 0);
        db.set(b"a", s("new"), SetTtl::Keep, 150);
        assert_eq!(db.expire_at(b"a", 150), Some(None));
        assert_eq!(db.get(b"a", 1_000_000), Some(&s("new")));
        assert_consistent(&db);
    }

    #[test]
    fn set_expire_and_persist() {
        let mut db = Db::new();
        assert!(!db.set_expire(b"a", Some(100), 0));
        db.set(b"a", s("1"), SetTtl::Clear, 0);
        assert!(db.set_expire(b"a", Some(100), 0));
        assert_eq!(db.expire_at(b"a", 0), Some(Some(100)));
        assert!(db.set_expire(b"a", None, 0));
        assert_eq!(db.expire_at(b"a", 0), Some(None));
        assert_eq!(db.get(b"a", 1_000), Some(&s("1")));
        assert_consistent(&db);
    }

    #[test]
    fn written_keys_are_recorded_only_while_tracking() {
        let list = || Value::List(VecDeque::from([Bytes::from_static(b"x")]));
        let mut db = Db::new();
        db.set(b"before", s("1"), SetTtl::Clear, 0);
        assert!(db.take_written().is_empty());

        db.track_writes(true);
        db.set(b"a", s("1"), SetTtl::Clear, 0);
        db.set(b"a", s("2"), SetTtl::Keep, 0); // an overwrite counts too
        db.get_or_insert_with(b"l", 0, list);
        db.get_or_insert_with(b"l", 0, list); // already there: not a write
        db.get_mut(b"l", 0);
        db.remove(b"a", 0);
        assert_eq!(db.take_written(), vec![&b"a"[..], &b"a"[..], &b"l"[..]]);
        assert!(db.take_written().is_empty());

        db.set(b"b", s("1"), SetTtl::Clear, 0);
        db.track_writes(false);
        assert!(db.take_written().is_empty());
        db.set(b"c", s("1"), SetTtl::Clear, 0);
        assert!(db.take_written().is_empty());
    }

    #[test]
    fn watched_keys_change_version_on_every_kind_of_write() {
        let list = || Value::List(VecDeque::new());
        let mut db = Db::new();
        db.set(b"a", s("1"), SetTtl::Clear, 0);
        let mut version = db.watch(b"a", 0);
        let mut changed = |db: &mut Db, now: u64| {
            let current = db.watch_version(b"a", now).unwrap();
            let changed = current != version;
            version = current;
            changed
        };

        // Reads and writes to other keys leave it alone.
        db.get(b"a", 0);
        db.peek(b"a", 0);
        db.exists(b"a", 0);
        db.set(b"b", s("1"), SetTtl::Clear, 0);
        db.remove(b"missing", 0);
        assert!(!changed(&mut db, 0));

        db.set(b"a", s("1"), SetTtl::Keep, 0);
        assert!(changed(&mut db, 0));
        db.get_mut(b"a", 0);
        assert!(changed(&mut db, 0));
        // ... unless the writer says it changed nothing.
        db.get_mut(b"a", 0);
        db.unchanged(b"a");
        assert!(!changed(&mut db, 0));
        db.get_or_insert_with(b"a", 0, list);
        db.unchanged(b"a");
        db.set_expire(b"a", None, 0); // no TTL before or after
        assert!(!changed(&mut db, 0));
        db.get_or_insert_with(b"a", 0, list);
        assert!(changed(&mut db, 0));
        db.set_expire(b"a", Some(100), 0);
        assert!(changed(&mut db, 0));
        // Expiry, noticed lazily by the version check itself.
        assert!(!changed(&mut db, 99));
        assert!(changed(&mut db, 100));
        // Creation of a missing key.
        db.get_or_insert_with(b"a", 100, list);
        assert!(changed(&mut db, 100));
        db.remove(b"a", 100);
        assert!(changed(&mut db, 100));
        db.set(b"a", s("1"), SetTtl::At(200), 100);
        assert!(changed(&mut db, 100));
        db.active_expire(200, 10);
        assert!(changed(&mut db, 200));
        // Clearing changes only keys that exist.
        db.clear();
        assert!(!changed(&mut db, 200));
        db.set(b"a", s("1"), SetTtl::Clear, 200);
        assert!(changed(&mut db, 200));
        db.clear();
        assert!(changed(&mut db, 200));
    }

    #[test]
    fn swapping_contents_changes_watched_keys_present_on_either_side() {
        let mut a = Db::new();
        let mut b = Db::new();
        a.set(b"only-a", s("1"), SetTtl::At(500), 0);
        b.set(b"only-b", s("2"), SetTtl::Clear, 0);
        let before: Vec<u64> = ["only-a", "only-b", "nowhere"]
            .iter()
            .map(|key| a.watch(key.as_bytes(), 0))
            .collect();
        let watched_b = b.watch(b"only-a", 0);

        Db::swap_contents(&mut a, &mut b);

        assert!(a.get(b"only-b", 0).is_some());
        assert!(a.get(b"only-a", 0).is_none());
        assert_eq!(
            b.expire_at(b"only-a", 0),
            Some(Some(500)),
            "the TTL moves too"
        );
        assert_consistent(&a);
        assert_consistent(&b);
        assert_ne!(a.watch_version(b"only-a", 0), Some(before[0]));
        assert_ne!(a.watch_version(b"only-b", 0), Some(before[1]));
        assert_eq!(
            a.watch_version(b"nowhere", 0),
            Some(before[2]),
            "a key in neither database is not disturbed"
        );
        assert_ne!(b.watch_version(b"only-a", 0), Some(watched_b));
    }

    #[test]
    fn watches_are_counted() {
        let mut db = Db::new();
        assert_eq!(db.watch_version(b"a", 0), None);
        db.watch(b"a", 0);
        db.watch(b"a", 0);
        db.unwatch(b"a");
        assert!(db.watch_version(b"a", 0).is_some());
        db.unwatch(b"a");
        assert_eq!(db.watch_version(b"a", 0), None);
        db.unwatch(b"a"); // harmless
    }

    #[test]
    fn a_key_already_expired_when_watched_does_not_count_as_changed() {
        let mut db = Db::new();
        db.set(b"a", s("1"), SetTtl::At(100), 0);
        let version = db.watch(b"a", 150);
        assert_eq!(db.watch_version(b"a", 200), Some(version));
    }

    type Saved = std::sync::Arc<std::sync::Mutex<Vec<(Bytes, Value, Option<u64>)>>>;

    fn collecting_sink() -> (Saved, SnapshotSink) {
        let saved = Saved::default();
        let shared = saved.clone();
        let sink: SnapshotSink = Box::new(move |key, value, expire_at, _now| {
            shared
                .lock()
                .unwrap()
                .push((Bytes::copy_from_slice(key), value.clone(), expire_at));
            true
        });
        (saved, sink)
    }

    fn sorted(saved: &Saved) -> Vec<(Bytes, Value, Option<u64>)> {
        let mut entries = saved.lock().unwrap().clone();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    #[test]
    fn a_snapshot_is_the_database_at_the_moment_it_began() {
        let mut db = Db::new();
        for key in ["changed", "removed", "expires", "untouched", "retimed"] {
            db.set(key.as_bytes(), s(key), SetTtl::Clear, 0);
        }
        db.set(b"already-expired", s("x"), SetTtl::At(50), 0);
        let (saved, sink) = collecting_sink();
        db.begin_snapshot(100, sink);

        // Everything a client can do before the walk gets there.
        db.set(b"changed", s("new"), SetTtl::Clear, 100);
        db.remove(b"removed", 100);
        db.set_expire(b"expires", Some(150), 100);
        db.get(b"expires", 200); // lazily expired and deleted
        db.set_expire(b"retimed", Some(9_999), 100);
        db.set(b"created-later", s("x"), SetTtl::Clear, 100);
        if let Some(Value::String(text)) = db.get_mut(b"created-later", 100) {
            *text = Bytes::from_static(b"y");
        }
        while !db.snapshot_step(1) {}
        db.end_snapshot();

        let expected: Vec<(Bytes, Value, Option<u64>)> =
            ["changed", "expires", "removed", "retimed", "untouched"]
                .iter()
                .map(|key| (Bytes::copy_from_slice(key.as_bytes()), s(key), None))
                .collect();
        assert_eq!(sorted(&saved), expected);
        // The database itself carries on with the new state.
        assert_eq!(db.get(b"changed", 200), Some(&s("new")));
        assert!(!db.is_snapshotting());
    }

    #[test]
    fn clearing_the_database_during_a_snapshot_saves_it_first() {
        let mut db = Db::new();
        for n in 0..100 {
            db.set(format!("k{n}").as_bytes(), s("v"), SetTtl::Clear, 0);
        }
        let (saved, sink) = collecting_sink();
        db.begin_snapshot(0, sink);
        db.snapshot_step(3);
        db.clear();
        assert!(db.snapshot_step(1));
        assert_eq!(saved.lock().unwrap().len(), 100);
    }

    #[test]
    fn a_sink_may_leave_an_entry_for_later() {
        let mut db = Db::new();
        db.set(b"plain", s("1"), SetTtl::Clear, 0);
        db.set(b"big", s("left for later"), SetTtl::At(900), 0);
        db.set(b"big-written", s("left, then written to"), SetTtl::Clear, 0);
        db.set(b"gone", s("expired before the snapshot"), SetTtl::At(50), 0);
        // Nothing is unsaved without a snapshot.
        assert_eq!(db.unsaved(b"big"), None);

        // A sink that leaves anything called "big..." alone unless it has to
        // take it.
        let taken = Saved::default();
        let shared = taken.clone();
        let sink: SnapshotSink = Box::new(move |key, value, expire_at, now| {
            if key.starts_with(b"big") && !now {
                return false;
            }
            let entry = (key.clone(), value.clone(), expire_at);
            shared.lock().unwrap().push(entry);
            true
        });
        db.begin_snapshot(100, sink);
        while !db.snapshot_step(1) {}
        // The walk is over; the two are still to be saved, as they were.
        assert_eq!(sorted(&taken).len(), 1);
        assert_eq!(db.unsaved(b"big"), Some((&s("left for later"), Some(900))));
        assert!(db.unsaved(b"big-written").is_some());
        assert_eq!(db.unsaved(b"plain"), None);
        assert_eq!(db.unsaved(b"gone"), None);
        assert_eq!(db.unsaved(b"missing"), None);

        // A write to one makes the sink take it first, as it was.
        db.set(b"big-written", s("new"), SetTtl::Clear, 200);
        assert_eq!(db.unsaved(b"big-written"), None);
        // One created now is not part of the snapshot.
        db.set(b"big-new", s("x"), SetTtl::Clear, 200);
        assert_eq!(db.unsaved(b"big-new"), None);
        // The other is saved by whoever comes back for it, and stays saved.
        db.mark_saved(b"big");
        assert_eq!(db.unsaved(b"big"), None);
        db.set(b"big", s("changed afterwards"), SetTtl::Clear, 300);
        db.end_snapshot();
        assert_eq!(
            sorted(&taken),
            vec![
                (
                    Bytes::from_static(b"big-written"),
                    s("left, then written to"),
                    None
                ),
                (Bytes::from_static(b"plain"), s("1"), None),
            ]
        );

        // Nothing carries over: the next snapshot has it all to save again.
        let (saved, sink) = collecting_sink();
        db.begin_snapshot(400, sink);
        assert!(db.unsaved(b"big").is_some());
        while !db.snapshot_step(4) {}
        db.end_snapshot();
        assert_eq!(sorted(&saved).len(), 4);
    }

    #[test]
    fn consecutive_snapshots_each_save_everything() {
        let mut db = Db::new();
        db.set(b"a", s("1"), SetTtl::Clear, 0);
        for round in 0..3 {
            let (saved, sink) = collecting_sink();
            db.begin_snapshot(0, sink);
            // Created during this snapshot; part of the next one.
            db.set(format!("new{round}").as_bytes(), s("x"), SetTtl::Clear, 0);
            while !db.snapshot_step(1) {}
            db.end_snapshot();
            assert_eq!(saved.lock().unwrap().len(), 1 + round, "round {round}");
        }
        // With no snapshot running there is nothing to step.
        assert!(db.snapshot_step(1));
    }

    fn expired(notes: Vec<(Note, Bytes)>) -> Vec<Bytes> {
        notes
            .into_iter()
            .map(|(note, key)| {
                assert_eq!(note, Note::Expired);
                key
            })
            .collect()
    }

    fn hash(fields: &[(&str, Option<u64>)]) -> Value {
        let mut hash = Hash::new();
        for (field, at) in fields {
            hash.insert(field.as_bytes(), Bytes::from_static(b"v"));
            if let Some(at) = at {
                hash.set_field_expire(field.as_bytes(), *at);
            }
        }
        Value::Hash(hash)
    }

    fn fields_of(db: &mut Db, key: &str, now: u64) -> Vec<String> {
        match db.get(key.as_bytes(), now) {
            Some(Value::Hash(hash)) => hash
                .iter(now)
                .map(|(field, _)| String::from_utf8(field.to_vec()).unwrap())
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Every hash with a field TTL is in the field index, under a time no
    /// later than its earliest one; nothing else is, but for entries the
    /// reclaimer will find stale.
    fn assert_fields_indexed(db: &Db) {
        for (key, entry) in db.iter() {
            if let Value::Hash(hash) = &entry.value {
                match (hash.next_field_expiry(), hash.indexed_at()) {
                    (Some(next), Some(at)) => {
                        assert!(at <= next, "{key:?} indexed late");
                        assert!(db.field_index.contains(&(at, key.clone())), "{key:?}");
                    }
                    (Some(_), None) => panic!("{key:?} has field TTLs but is not indexed"),
                    (None, Some(at)) => {
                        assert!(db.field_index.contains(&(at, key.clone())), "{key:?}");
                    }
                    (None, None) => {}
                }
            }
        }
        for (at, key) in &db.field_index {
            let indexed = matches!(
                db.dict.get(key),
                Some(Entry { value: Value::Hash(hash), .. }) if hash.indexed_at() == Some(*at)
            );
            assert!(indexed, "stray entry for {key:?}");
        }
    }

    #[test]
    fn hash_fields_are_reclaimed_in_time_order_a_bounded_number_at_a_time() {
        let mut db = Db::new();
        db.track_expired(true);
        db.set(
            b"h",
            hash(&[
                ("a", Some(30)),
                ("b", Some(10)),
                ("c", None),
                ("d", Some(20)),
            ]),
            SetTtl::Clear,
            0,
        );
        db.set(b"whole", hash(&[("x", Some(15))]), SetTtl::Clear, 0);
        db.set(b"plain", hash(&[("x", None)]), SetTtl::Clear, 0);
        assert_eq!(db.volatile_hashes(), 2);
        assert_fields_indexed(&db);

        assert_eq!(db.active_expire_fields(9, 100), 0);
        // Two at most: the hash due first gives up both of its fields due
        // (b, then d), and `whole` waits for the next call.
        assert_eq!(db.active_expire_fields(25, 2), 2);
        assert_eq!(fields_of(&mut db, "h", 0), ["a", "c"]);
        assert!(db.exists(b"whole", 0));
        assert_eq!(db.active_expire_fields(25, 2), 1);
        assert!(!db.exists(b"whole", 25), "its last field went");
        assert_eq!(db.active_expire_fields(100, 100), 1);
        assert_eq!(fields_of(&mut db, "h", 0), ["c"]);
        assert_eq!(db.volatile_hashes(), 0);
        assert_eq!(db.expired_fields(), 4);
        assert_fields_indexed(&db);
        let field = |name: &'static str| Bytes::from_static(name.as_bytes());
        let expired = |fields: Vec<Bytes>, emptied: bool| Note::FieldsExpired { fields, emptied };
        assert_eq!(
            db.take_notes(),
            vec![
                (expired(vec![field("b"), field("d")], false), field("h")),
                (expired(vec![field("x")], true), field("whole")),
                (expired(vec![field("a")], false), field("h")),
            ]
        );
    }

    #[test]
    fn a_lookup_deletes_the_expired_fields_it_names() {
        let mut db = Db::new();
        db.track_expired(true);
        db.set(
            b"h",
            hash(&[("a", Some(10)), ("b", Some(10)), ("c", Some(50))]),
            SetTtl::Clear,
            0,
        );
        let names = |list: &[&'static str]| -> Vec<Bytes> {
            list.iter()
                .map(|name| Bytes::from_static(name.as_bytes()))
                .collect()
        };
        assert_eq!(db.expire_fields(b"h", Some(&names(&["a", "c"])), 5), 0);
        assert_eq!(
            db.expire_fields(b"h", Some(&names(&["a", "a", "c", "nope"])), 20),
            1
        );
        assert_eq!(db.expire_fields(b"missing", None, 20), 0);
        let Some(Value::Hash(hash)) = db.get(b"h", 20) else {
            panic!("gone")
        };
        assert_eq!(hash.len(), 2, "b is still stored");
        assert_eq!(db.expire_fields(b"h", None, 60), 2);
        assert!(!db.exists(b"h", 60));
        assert_eq!(db.expired_fields(), 3);
        assert_fields_indexed(&db);
        assert_eq!(db.take_notes().len(), 2);
    }

    #[test]
    fn the_field_index_follows_values_that_move_or_go() {
        let mut db = Db::new();
        db.set(b"h", hash(&[("a", Some(100))]), SetTtl::Clear, 0);
        // A copy is indexed under its own key.
        let copy = db.get(b"h", 0).unwrap().clone();
        db.set(b"copy", copy, SetTtl::Clear, 0);
        // A rename.
        let (value, _) = db.take(b"h", 0).unwrap();
        db.set(b"renamed", value, SetTtl::Clear, 0);
        assert_eq!(db.volatile_hashes(), 2);
        assert_fields_indexed(&db);
        // Overwritten by another type, deleted, or expired as a whole key.
        db.set(b"copy", s("x"), SetTtl::Clear, 0);
        db.set(b"gone", hash(&[("a", Some(100))]), SetTtl::At(50), 0);
        db.set(b"deleted", hash(&[("a", Some(100))]), SetTtl::Clear, 0);
        db.remove(b"deleted", 0);
        assert_eq!(db.volatile_hashes(), 2);
        assert_eq!(db.active_expire(60, 10), 1);
        assert_eq!(db.volatile_hashes(), 1);
        assert_fields_indexed(&db);
        let mut other = Db::new();
        Db::swap_contents(&mut db, &mut other);
        assert_eq!((db.volatile_hashes(), other.volatile_hashes()), (0, 1));
        assert_fields_indexed(&other);
        assert_eq!(other.active_expire_fields(100, 10), 1);
        assert!(!other.exists(b"renamed", 100));
        other.set(b"again", hash(&[("a", Some(200))]), SetTtl::Clear, 100);
        other.clear();
        assert_eq!(other.volatile_hashes(), 0);
    }

    #[test]
    fn field_expiry_is_a_change_for_watchers_and_saved_first_by_a_snapshot() {
        let mut db = Db::new();
        db.set(
            b"h",
            hash(&[("a", Some(10)), ("b", None)]),
            SetTtl::Clear,
            0,
        );
        let version = db.watch(b"h", 0);
        let (saved, sink) = collecting_sink();
        db.begin_snapshot(5, sink);
        db.active_expire_fields(20, 10);
        assert_ne!(db.watch_version(b"h", 20), Some(version));
        while !db.snapshot_step(1) {}
        db.end_snapshot();
        let saved = sorted(&saved);
        assert_eq!(saved.len(), 1);
        let Value::Hash(hash) = &saved[0].1 else {
            panic!("not a hash")
        };
        assert_eq!(hash.field_expire_at(b"a"), Some(10), "as it was");
    }

    #[test]
    fn expired_keys_are_recorded_only_while_tracking() {
        let mut db = Db::new();
        db.set(b"a", s("1"), SetTtl::At(10), 0);
        db.get(b"a", 20);
        assert!(db.take_notes().is_empty());

        db.track_expired(true);
        db.set(b"lazy", s("1"), SetTtl::At(30), 20);
        db.set(b"active", s("1"), SetTtl::At(40), 20);
        db.set(b"deleted", s("1"), SetTtl::At(40), 20);
        db.set(b"kept", s("1"), SetTtl::Clear, 20);
        db.remove(b"deleted", 25); // an ordinary delete is not an expiry
        db.get(b"lazy", 35);
        db.get(b"lazy", 36); // already gone: recorded once
        db.active_expire(50, 10);
        assert_eq!(expired(db.take_notes()), vec![&b"lazy"[..], &b"active"[..]]);
        assert!(db.take_notes().is_empty());
        db.track_expired(false);
        db.set(b"b", s("1"), SetTtl::At(60), 50);
        db.get(b"b", 70);
        assert!(db.take_notes().is_empty());
    }

    #[test]
    fn misses_and_created_keys_are_noted_only_when_asked() {
        let mut db = Db::new();
        db.get(b"missing", 0);
        db.set(b"new", s("1"), SetTtl::Clear, 0);
        assert!(db.take_notes().is_empty());

        db.track_expired(true);
        db.note_created(true);
        db.set(b"a", s("1"), SetTtl::At(10), 0);
        // Overwriting is not creating.
        db.set(b"a", s("2"), SetTtl::Keep, 0);
        db.get_or_insert_with(b"l", 0, || Value::List(VecDeque::new()));
        db.get_or_insert_with(b"l", 0, || Value::List(VecDeque::new()));
        // Lookups that find nothing count only while misses are noted.
        db.get(b"nope", 0);
        db.note_misses(true);
        db.get(b"nope", 0);
        db.note_misses(false);
        db.get(b"nope2", 0);
        assert_eq!(
            db.take_notes(),
            vec![
                (Note::Created, Bytes::from_static(b"a")),
                (Note::Created, Bytes::from_static(b"l")),
                (Note::Missed, Bytes::from_static(b"nope")),
            ]
        );
        // Recreating an expired key: expired, then created.
        db.set(b"a", s("3"), SetTtl::Clear, 20);
        assert_eq!(
            db.take_notes(),
            vec![
                (Note::Expired, Bytes::from_static(b"a")),
                (Note::Created, Bytes::from_static(b"a")),
            ]
        );
        db.note_created(false);
        db.set(b"c", s("1"), SetTtl::Clear, 20);
        assert!(db.take_notes().is_empty());
    }

    #[test]
    fn a_read_of_an_expired_key_notes_the_expiry_then_the_miss() {
        let mut db = Db::new();
        db.track_expired(true);
        db.set(b"a", s("1"), SetTtl::At(10), 0);
        db.note_misses(true);
        db.get(b"x", 0);
        db.exists(b"a", 20);
        assert_eq!(
            db.take_notes(),
            vec![
                (Note::Missed, Bytes::from_static(b"x")),
                (Note::Expired, Bytes::from_static(b"a")),
                (Note::Missed, Bytes::from_static(b"a")),
            ]
        );
    }

    #[test]
    fn remove_if_empty_says_whether_it_removed() {
        let mut db = Db::new();
        db.set(b"l", Value::List(VecDeque::new()), SetTtl::Clear, 0);
        db.set(b"s", s("x"), SetTtl::Clear, 0);
        assert!(db.remove_if_empty(b"l", 0));
        assert!(!db.remove_if_empty(b"l", 0));
        assert!(!db.remove_if_empty(b"s", 0));
    }

    #[test]
    fn lookups_update_idle_time_and_frequency() {
        let mut db = Db::new();
        db.set(b"used", s("1"), SetTtl::Clear, 0);
        db.set(b"unused", s("1"), SetTtl::Clear, 0);
        for _ in 0..500 {
            db.get(b"used", 10_000);
        }
        let used = db.access_info(b"used", 70_000).unwrap();
        let unused = db.access_info(b"unused", 70_000).unwrap();
        assert_eq!((used.idle_seconds, unused.idle_seconds), (60, 70));
        assert!(used.frequency > unused.frequency, "{used:?} {unused:?}");
        // Asking is not a lookup.
        assert_eq!(db.access_info(b"unused", 70_000).unwrap().idle_seconds, 70);
        assert_eq!(db.access_info(b"missing", 0), None);

        // Unused, the frequency fades: one step per minute by default.
        let later = db.access_info(b"used", 10_000 + 5 * 60_000).unwrap();
        assert_eq!(later.frequency, used.frequency.saturating_sub(4));
        db.set_lfu(10, 0);
        assert_eq!(
            db.access_info(b"used", 10_000_000).unwrap().frequency,
            used.frequency + 1
        );
    }

    #[test]
    fn eviction_candidates() {
        let mut db = Db::new();
        assert_eq!(db.sample(false, 0), None);
        assert_eq!(db.sample(true, 0), None);
        assert_eq!(db.soonest_to_expire(0), None);

        db.set(b"plain", s("1"), SetTtl::Clear, 0);
        assert_eq!(db.sample(true, 0), None);
        db.set(b"soon", s("1"), SetTtl::At(1_000), 0);
        db.set(b"later", s("1"), SetTtl::At(9_000), 0);
        db.set(b"last", s("1"), SetTtl::At(50_000), 0);
        assert_eq!(db.soonest_to_expire(0).unwrap().key, &b"soon"[..]);

        let mut any = std::collections::HashSet::new();
        let mut volatile = std::collections::HashSet::new();
        for _ in 0..400 {
            any.insert(db.sample(false, 0).unwrap().key);
            let candidate = db.sample(true, 0).unwrap();
            assert!(candidate.expire_at.is_some());
            volatile.insert(candidate.key);
        }
        assert_eq!(any.len(), 4);
        assert_eq!(volatile.len(), 3);
    }

    /// The `Changed` notes, as `key` or `key!` (by the server).
    fn changes(db: &mut Db) -> Vec<String> {
        db.take_notes()
            .into_iter()
            .filter_map(|(note, key)| match note {
                Note::Changed { by_server } => Some(format!(
                    "{}{}",
                    String::from_utf8_lossy(&key),
                    if by_server { "!" } else { "" }
                )),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn changes_are_noted_only_while_tracked() {
        let mut db = Db::new();
        db.set(b"a", s("1"), SetTtl::Clear, 0);
        assert!(!db.has_notes());
        db.track_changes(true);
        db.set(b"a", s("2"), SetTtl::Clear, 0);
        db.set_expire(b"a", Some(100), 0);
        db.remove(b"missing", 0);
        db.remove(b"a", 0);
        assert_eq!(changes(&mut db), ["a", "a", "a"]);
        db.set(b"b", s("1"), SetTtl::Clear, 0);
        db.track_changes(false);
        assert!(changes(&mut db).is_empty());
        db.set(b"b", s("2"), SetTtl::Clear, 0);
        assert!(!db.has_notes());
    }

    #[test]
    fn a_value_handed_out_for_writing_changed_unless_reported_unchanged() {
        let mut db = Db::new();
        db.set(b"a", s("1"), SetTtl::Clear, 0);
        db.set(b"b", s("1"), SetTtl::Clear, 0);
        db.track_changes(true);
        db.get_mut(b"a", 0);
        db.get_mut(b"a", 0);
        db.get_mut(b"b", 0);
        db.unchanged(b"b");
        assert!(db.has_notes());
        assert_eq!(changes(&mut db), ["a"]);
        db.get_or_insert_with(b"c", 0, || s("new"));
        assert_eq!(changes(&mut db), ["c"]);
    }

    #[test]
    fn expiry_and_eviction_are_changes_by_the_server_noted_after_the_expiry() {
        let mut db = Db::new();
        db.track_expired(true);
        db.track_changes(true);
        db.set(b"lazy", s("v"), SetTtl::At(10), 0);
        db.set(b"cycle", s("v"), SetTtl::At(10), 0);
        db.set(b"evicted", s("v"), SetTtl::Clear, 0);
        db.take_notes();
        db.get(b"lazy", 20);
        db.active_expire(20, 10);
        assert!(db.evict(b"evicted", 20));
        assert!(!db.evict(b"evicted", 20));
        let notes: Vec<String> = db
            .take_notes()
            .into_iter()
            .map(|(note, key)| format!("{note:?} {}", String::from_utf8_lossy(&key)))
            .collect();
        assert_eq!(
            notes,
            [
                "Expired lazy",
                "Changed { by_server: true } lazy",
                "Expired cycle",
                "Changed { by_server: true } cycle",
                "Changed { by_server: true } evicted",
            ]
        );
        // Writing a key that has expired: the expiry, then the write.
        db.set(b"k", s("v"), SetTtl::At(30), 20);
        db.take_notes();
        db.set(b"k", s("w"), SetTtl::Clear, 40);
        let notes: Vec<String> = db
            .take_notes()
            .into_iter()
            .map(|(note, _)| format!("{note:?}"))
            .collect();
        assert_eq!(
            notes,
            [
                "Expired",
                "Changed { by_server: true }",
                "Changed { by_server: false }"
            ]
        );
    }

    #[test]
    fn swapping_and_clearing_note_no_changes() {
        let mut a = Db::new();
        let mut b = Db::new();
        a.set(b"k", s("v"), SetTtl::Clear, 0);
        a.track_changes(true);
        b.track_changes(true);
        Db::swap_contents(&mut a, &mut b);
        b.clear();
        a.set(b"x", s("v"), SetTtl::Clear, 0);
        a.take_all();
        assert_eq!(changes(&mut a), ["x"]);
        assert!(changes(&mut b).is_empty());
    }

    #[test]
    fn take_all_empties_like_clear() {
        let mut db = Db::new();
        db.set(b"a", s("1"), SetTtl::At(100), 0);
        db.set(b"b", s("1"), SetTtl::Clear, 0);
        let version = db.watch(b"a", 0);
        let (saved, sink) = collecting_sink();
        db.begin_snapshot(0, sink);
        let contents = db.take_all();
        assert!(db.is_empty());
        assert_eq!(db.volatile_len(), 0);
        assert_ne!(db.watch_version(b"a", 0), Some(version));
        assert_eq!(saved.lock().unwrap().len(), 2);
        drop(contents);
        db.set(b"c", s("1"), SetTtl::Clear, 0);
        assert_eq!(db.len(), 1);
    }

    #[test]
    fn take_returns_value_and_ttl() {
        let mut db = Db::new();
        db.set(b"a", s("1"), SetTtl::At(100), 0);
        assert_eq!(db.take(b"a", 0), Some((s("1"), Some(100))));
        assert_eq!(db.take(b"a", 0), None);
        assert_eq!(db.volatile_len(), 0);

        db.set(b"b", s("1"), SetTtl::At(100), 0);
        assert_eq!(db.take(b"b", 100), None);
    }

    #[test]
    fn active_expire_removes_only_due_keys_in_order() {
        let mut db = Db::new();
        for i in 0..100u64 {
            db.set(format!("k{i}").as_bytes(), s("v"), SetTtl::At(i + 1), 0);
        }
        db.set(b"forever", s("v"), SetTtl::Clear, 0);

        assert_eq!(db.active_expire(0, 1_000), 0);
        assert_eq!(db.active_expire(50, 20), 20, "respects the budget");
        assert_eq!(db.active_expire(50, 1_000), 30);
        assert_eq!(db.len(), 51);
        assert_eq!(db.active_expire(u64::MAX, 1_000), 50);
        assert_eq!(db.len(), 1);
        assert!(db.exists(b"forever", u64::MAX));
        assert_consistent(&db);
    }

    #[test]
    fn random_key_skips_expired() {
        let mut db = Db::new();
        assert_eq!(db.random_key(0), None);
        db.set(b"dead", s("v"), SetTtl::At(10), 0);
        db.set(b"live", s("v"), SetTtl::Clear, 0);
        for _ in 0..50 {
            assert_eq!(db.random_key(20), Some(Bytes::from_static(b"live")));
        }
        db.remove(b"live", 20);
        assert_eq!(db.random_key(20), None);
    }

    #[test]
    fn clear_empties_everything() {
        let mut db = Db::new();
        db.set(b"a", s("1"), SetTtl::At(100), 0);
        db.set(b"b", s("1"), SetTtl::Clear, 0);
        db.clear();
        assert!(db.is_empty());
        assert_eq!(db.volatile_len(), 0);
        assert_eq!(db.get(b"a", 0), None);
    }

    #[derive(Debug, Clone)]
    enum Op {
        Set(u8, u8, Option<Option<u8>>),
        Expire(u8, Option<u8>),
        Remove(u8),
        Get(u8),
        Tick(u8),
        ActiveExpire,
    }

    fn op() -> impl Strategy<Value = Op> {
        let key = 0u8..12;
        prop_oneof![
            4 => (key.clone(), any::<u8>(), any::<Option<Option<u8>>>())
                .prop_map(|(k, v, t)| Op::Set(k, v, t)),
            2 => (key.clone(), any::<Option<u8>>()).prop_map(|(k, t)| Op::Expire(k, t)),
            2 => key.clone().prop_map(Op::Remove),
            3 => key.prop_map(Op::Get),
            2 => (0u8..40).prop_map(Op::Tick),
            1 => Just(Op::ActiveExpire),
        ]
    }

    #[derive(Debug, Clone)]
    enum FieldOp {
        /// A new hash of a few fields at a key.
        Create(u8),
        Ttl(u8, u8, u8),
        Persist(u8, u8),
        Write(u8, u8),
        Delete(u8, u8),
        Lookup(u8, u8),
        Copy(u8, u8),
        Rename(u8, u8),
        Overwrite(u8),
        Remove(u8),
        Tick(u8),
        Reclaim(u8),
    }

    fn field_op() -> impl Strategy<Value = FieldOp> {
        prop_oneof![
            (0u8..4).prop_map(FieldOp::Create),
            (0u8..4, 0u8..4, 0u8..50).prop_map(|(k, f, d)| FieldOp::Ttl(k, f, d)),
            (0u8..4, 0u8..4).prop_map(|(k, f)| FieldOp::Persist(k, f)),
            (0u8..4, 0u8..4).prop_map(|(k, f)| FieldOp::Write(k, f)),
            (0u8..4, 0u8..4).prop_map(|(k, f)| FieldOp::Delete(k, f)),
            (0u8..4, 0u8..4).prop_map(|(k, f)| FieldOp::Lookup(k, f)),
            (0u8..4, 0u8..4).prop_map(|(a, b)| FieldOp::Copy(a, b)),
            (0u8..4, 0u8..4).prop_map(|(a, b)| FieldOp::Rename(a, b)),
            (0u8..4).prop_map(FieldOp::Overwrite),
            (0u8..4).prop_map(FieldOp::Remove),
            (0u8..30).prop_map(FieldOp::Tick),
            (1u8..5).prop_map(FieldOp::Reclaim),
        ]
    }

    proptest! {
        /// Whatever happens to hashes with field TTLs, the field index
        /// keeps up, and reclaiming everything due leaves no field past its
        /// time and no empty hash.
        #[test]
        fn the_field_index_never_loses_a_hash(ops in prop::collection::vec(field_op(), 0..150)) {
            let mut db = Db::new();
            let mut now = 0u64;
            let key = |k: u8| [b'k', k];
            let field = |f: u8| Bytes::from(vec![b'f', f]);
            for op in &ops {
                match *op {
                    FieldOp::Create(k) => db.set(
                        &key(k),
                        hash(&[("f\u{0}", None), ("f\u{1}", None), ("f\u{2}", None)]),
                        SetTtl::Clear,
                        now,
                    ),
                    FieldOp::Ttl(k, f, d) => {
                        if let Some(Value::Hash(hash)) = db.get_mut(&key(k), now) {
                            hash.set_field_expire(&field(f), now + u64::from(d) + 1);
                            db.index_field_ttls(&key(k));
                        }
                    }
                    FieldOp::Persist(k, f) => {
                        if let Some(Value::Hash(hash)) = db.get_mut(&key(k), now) {
                            hash.persist_field(&field(f));
                        }
                    }
                    FieldOp::Write(k, f) => {
                        if let Some(Value::Hash(hash)) = db.get_mut(&key(k), now) {
                            hash.insert(&field(f), Bytes::from_static(b"w"));
                        }
                    }
                    FieldOp::Delete(k, f) => {
                        if let Some(Value::Hash(hash)) = db.get_mut(&key(k), now) {
                            hash.remove(&field(f));
                        }
                        db.remove_if_empty(&key(k), now);
                    }
                    FieldOp::Lookup(k, f) => {
                        db.expire_fields(&key(k), Some(&[field(f)]), now);
                    }
                    FieldOp::Copy(a, b) => {
                        if let Some(value) = db.get(&key(a), now).cloned() {
                            db.set(&key(b), value, SetTtl::Clear, now);
                        }
                    }
                    FieldOp::Rename(a, b) => {
                        if let Some((value, _)) = db.take(&key(a), now) {
                            db.set(&key(b), value, SetTtl::Clear, now);
                        }
                    }
                    FieldOp::Overwrite(k) => db.set(&key(k), s("x"), SetTtl::Clear, now),
                    FieldOp::Remove(k) => {
                        db.remove(&key(k), now);
                    }
                    FieldOp::Tick(d) => now += u64::from(d),
                    FieldOp::Reclaim(max) => {
                        db.active_expire_fields(now, usize::from(max));
                    }
                }
                assert_fields_indexed(&db);
            }
            db.active_expire_fields(now, usize::MAX);
            assert_fields_indexed(&db);
            for (key, entry) in db.iter() {
                if let Value::Hash(hash) = &entry.value {
                    prop_assert!(!hash.is_empty(), "{key:?} is empty");
                    prop_assert!(!hash.has_expired_fields(now), "{key:?} kept a field past its time");
                }
            }
        }
    }

    proptest! {
        /// Under any interleaving of writes, TTL changes, clock advances and
        /// active expiry, the database agrees with a trivially correct model
        /// and its expiry index never drifts from the entries.
        #[test]
        /// A snapshot interleaved with arbitrary operations still captures
        /// exactly the entries that were live when it began.
        #[test]
        fn snapshot_matches_the_starting_state(
            before in prop::collection::vec(op(), 0..200),
            during in prop::collection::vec((op(), 0usize..3), 0..200),
        ) {
            let mut db = Db::new();
            let mut now = 0u64;
            let apply = |db: &mut Db, op: &Op, now: &mut u64| match op {
                Op::Set(key, value, ttl) => {
                    let policy = match ttl {
                        None => SetTtl::Keep,
                        Some(None) => SetTtl::Clear,
                        Some(Some(d)) => SetTtl::At(*now + u64::from(*d) + 1),
                    };
                    db.set(&[*key], Value::String(Bytes::from(vec![*value])), policy, *now);
                }
                Op::Expire(key, ttl) => {
                    db.set_expire(&[*key], ttl.map(|d| *now + u64::from(d) + 1), *now);
                }
                Op::Remove(key) => {
                    db.remove(&[*key], *now);
                }
                Op::Get(key) => {
                    db.get(&[*key], *now);
                }
                Op::Tick(d) => *now += u64::from(*d),
                Op::ActiveExpire => {
                    db.active_expire(*now, usize::MAX);
                }
            };
            for op in &before {
                apply(&mut db, op, &mut now);
            }

            let mut expected: Vec<(Bytes, Value, Option<u64>)> = db
                .iter()
                .filter(|(_, entry)| !entry.is_expired(now))
                .map(|(key, entry)| (key.clone(), entry.value.clone(), entry.expire_at()))
                .collect();
            expected.sort_by(|a, b| a.0.cmp(&b.0));

            let (saved, sink) = collecting_sink();
            db.begin_snapshot(now, sink);
            for (op, steps) in &during {
                apply(&mut db, op, &mut now);
                db.snapshot_step(*steps);
                db.rehash_steps(1);
            }
            while !db.snapshot_step(4) {}
            db.end_snapshot();
            prop_assert_eq!(sorted(&saved), expected);
        }

        fn matches_model(ops in prop::collection::vec(op(), 0..300)) {
            let mut db = Db::new();
            // key -> (value, absolute expiry)
            let mut model: HashMap<u8, (u8, Option<u64>)> = HashMap::new();
            let mut now = 0u64;

            for op in ops {
                model.retain(|_, (_, at)| at.is_none_or(|at| at > now));
                match op {
                    Op::Set(k, v, ttl) => {
                        let (policy, expiry) = match ttl {
                            None => (SetTtl::Keep, model.get(&k).and_then(|(_, at)| *at)),
                            Some(None) => (SetTtl::Clear, None),
                            Some(Some(d)) => {
                                let at = now + u64::from(d) + 1;
                                (SetTtl::At(at), Some(at))
                            }
                        };
                        db.set(&[k], Value::String(Bytes::from(vec![v])), policy, now);
                        model.insert(k, (v, expiry));
                    }
                    Op::Expire(k, ttl) => {
                        let at = ttl.map(|d| now + u64::from(d) + 1);
                        let existed = db.set_expire(&[k], at, now);
                        prop_assert_eq!(existed, model.contains_key(&k));
                        if let Some(entry) = model.get_mut(&k) {
                            entry.1 = at;
                        }
                    }
                    Op::Remove(k) => {
                        prop_assert_eq!(db.remove(&[k], now), model.remove(&k).is_some());
                    }
                    Op::Get(k) => {
                        let expected = model.get(&k).map(|(v, _)| Value::String(Bytes::from(vec![*v])));
                        prop_assert_eq!(db.get(&[k], now).cloned(), expected);
                        prop_assert_eq!(
                            db.expire_at(&[k], now),
                            model.get(&k).map(|(_, at)| *at)
                        );
                    }
                    Op::Tick(d) => now += u64::from(d),
                    Op::ActiveExpire => {
                        db.active_expire(now, usize::MAX);
                        prop_assert_eq!(db.len(), model.len());
                    }
                }
                assert_consistent(&db);
            }
        }
    }
}
