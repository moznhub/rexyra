//! Snapshots on disk: `SAVE`, loading at start-up, `LASTSAVE`, `SHUTDOWN`,
//! and the per-key `DUMP` / `RESTORE`.
//!
//! A snapshot is one RDB file (see [`rexyra_persist::rdb`]) holding every
//! database and every function library. It is written to a temporary file
//! and renamed into place, so a crash while saving leaves the previous
//! snapshot intact.
//!
//! `SAVE` writes it while the server waits. `BGSAVE`, and the automatic
//! saves configured with `save`, write it in the background while commands
//! keep running; see [`BackgroundSave`].

use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use rexyra_engine::{Keyspace, SetTtl, Value};
use rexyra_persist::rdb::{self, Item, Writer};
use rexyra_resp::Frame;

use crate::{
    acl, copied, int, is, reply, scripting, CommandSpec, Ctx, Effect, Server, COMPAT_VERSION,
    ERR_SYNTAX,
};

/// Where the snapshot lives.
pub fn snapshot_path(server: &Server) -> PathBuf {
    Path::new(&server.dir).join(&server.dbfilename)
}

/// Makes `temporary` take the place of `path` in one step, then asks the
/// operating system to put that change on disk, so that a power failure
/// right afterwards does not bring the old file back.
pub(crate) fn replace_file(temporary: &Path, path: &Path) -> io::Result<()> {
    fs::rename(temporary, path)?;
    if let Some(dir) = path.parent() {
        sync_dir(dir);
    }
    Ok(())
}

/// Flushes a directory, which is where a rename is recorded. If this
/// cannot be done the rename still happened, and reaches the disk a little
/// later with the rest of the file system's bookkeeping.
#[cfg(windows)]
fn sync_dir(dir: &Path) {
    use std::os::windows::fs::OpenOptionsExt;
    // Needed to open a directory at all.
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    if let Ok(handle) = fs::OpenOptions::new()
        .write(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(dir)
    {
        let _ = handle.sync_all();
    }
}

#[cfg(not(windows))]
fn sync_dir(dir: &Path) {
    if let Ok(handle) = File::open(dir) {
        let _ = handle.sync_all();
    }
}

/// Whether a file name is one of the temporary files saves write to.
fn is_temporary_save(name: &str) -> bool {
    // `temp-<pid>.rdb`, `temp-bg-<pid>.rdb.<n>`, `temp-rewrite-<pid>.rdb.<n>`
    let Some(rest) = name.strip_prefix("temp-") else {
        return false;
    };
    let rest = rest
        .strip_prefix("bg-")
        .or_else(|| rest.strip_prefix("rewrite-"))
        .unwrap_or(rest);
    let Some((pid, tail)) = rest.split_once(".rdb") else {
        return false;
    };
    let digits = |text: &str| !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit());
    digits(pid) && (tail.is_empty() || tail.strip_prefix('.').is_some_and(digits))
}

/// Removes the unfinished files a save leaves behind when the server is
/// killed in the middle of it. Call at start-up, before anything is saved.
/// Returns the files removed.
///
/// The data directory belongs to one server at a time, so any such file is
/// left over from an earlier run.
pub fn remove_stale_temporaries(server: &Server) -> Vec<PathBuf> {
    let data = Path::new(&server.dir);
    let mut removed = Vec::new();
    for dir in [data.to_path_buf(), data.join(&server.aof_settings.dirname)] {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let is_file = entry.file_type().is_ok_and(|kind| kind.is_file());
            let ours = entry.file_name().to_str().is_some_and(is_temporary_save);
            if is_file && ours && fs::remove_file(entry.path()).is_ok() {
                removed.push(entry.path());
            }
        }
    }
    removed
}

/// Writes every database and function library to the snapshot file, and
/// returns once it is on disk. A background save in progress is abandoned
/// in favour of this one.
pub fn save(server: &mut Server, keyspace: &mut Keyspace, now: u64) -> io::Result<()> {
    abort_bgsave(server, keyspace);
    let path = snapshot_path(server);
    let temporary = Path::new(&server.dir).join(format!("temp-{}.rdb", std::process::id()));
    let written = write_snapshot(server, keyspace, now, &temporary)
        // Replaces any existing snapshot in one step.
        .and_then(|()| replace_file(&temporary, &path));
    if written.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    server.last_save_ok = written.is_ok();
    written?;
    server.dirty = 0;
    server.last_save = now / 1000;
    Ok(())
}

pub(crate) fn write_snapshot(
    server: &Server,
    keyspace: &mut Keyspace,
    now: u64,
    path: &Path,
) -> io::Result<()> {
    let mut writer =
        Writer::with_version(BufWriter::new(File::create(path)?), rdb_version(keyspace))?;
    write_preamble(&mut writer, server, now)?;
    for index in 0..keyspace.len() {
        let db = keyspace.db(index);
        // Keys whose TTL has passed but which have not been collected yet
        // are left out.
        let live = || db.iter().filter(|(_, entry)| !entry.is_expired(now));
        let keys = live().count();
        if keys == 0 {
            continue;
        }
        let expiring = live()
            .filter(|(_, entry)| entry.expire_at().is_some())
            .count();
        writer.select_db(index, keys, expiring)?;
        for (key, entry) in live() {
            writer.entry(key, &entry.value, entry.expire_at())?;
        }
    }
    let mut file = writer.finish()?;
    file.flush()?;
    // Make sure the data is on disk before the rename makes it the snapshot.
    file.get_ref().sync_all()
}

/// The RDB format version a snapshot of `keyspace` needs: the newer one,
/// which only Redis 7.4 and later read, only if a hash has field TTLs.
fn rdb_version(keyspace: &Keyspace) -> u16 {
    if keyspace.has_field_ttls() {
        rdb::VERSION_FIELD_TTLS
    } else {
        rdb::VERSION
    }
}

/// The start of every snapshot: metadata, then the function libraries.
fn write_preamble<W: Write>(writer: &mut Writer<W>, server: &Server, now: u64) -> io::Result<()> {
    let libraries = server
        .scripting
        .iter()
        .flat_map(|engine| engine.library_sources());
    write_header(writer, now, libraries)
}

fn write_header<'a, W: Write>(
    writer: &mut Writer<W>,
    now: u64,
    libraries: impl Iterator<Item = &'a Bytes>,
) -> io::Result<()> {
    writer.aux("redis-ver", COMPAT_VERSION)?;
    writer.aux("redis-bits", "64")?;
    writer.aux("ctime", &(now / 1000).to_string())?;
    for code in libraries {
        writer.function(code)?;
    }
    Ok(())
}

// --- saving in the background -------------------------------------------------

/// The walk of the keyspace pauses while this many bytes of entries are
/// waiting for the writer thread, which bounds the memory a save takes on
/// account of the walk.
const QUEUE_BYTES: usize = 16 * 1024 * 1024;
/// Hash-table buckets walked between looks at the clock and the queue.
const BUCKETS_PER_STEP: usize = 16;
/// A slice of background work ends after this many buckets or this long,
/// whichever comes first, so that commands are not kept waiting.
const BUCKETS_PER_SLICE: usize = 256;
const SLICE_MICROS: u128 = 500;
/// Once the walk has paused, the writer wakes it when the queue is down to
/// this, rather than at the first byte of room: fewer, longer stretches of
/// walking.
const RESUME_BYTES: usize = QUEUE_BYTES / 2;
/// Strings at least this long are handed to the writer as they are; shorter
/// ones are copied, encoded, along with their neighbours.
const SHARED_STRING_BYTES: usize = 64 * 1024;
/// Encoded entries are sent to the writer in pieces of about this size.
const BATCH_BYTES: usize = 64 * 1024;
/// Extra room in a batch for the entry that takes it over the size.
const BATCH_SLACK_BYTES: usize = 4 * 1024;
/// A list, hash, set or sorted set with at least this many elements, or
/// about this many bytes, is not captured in one go but a piece per slice
/// of background work: encoding a million elements at once would hold
/// every client up for the best part of a tenth of a second.
const STAGED_MIN_ELEMENTS: usize = 4_096;
const STAGED_MIN_BYTES: usize = 256 * 1024;
/// How much of such a collection is encoded between looks at the clock.
const STAGED_STEP_ELEMENTS: usize = 256;
const STAGED_STEP_BYTES: usize = 64 * 1024;
/// A large collection is encoded into pieces of about this size rather
/// than one buffer that grows: a buffer of tens of megabytes that has to
/// grow copies itself, which is a pause of milliseconds each time.
const PART_BYTES: usize = 256 * 1024;
/// A slice may be this long instead while a client is waiting to write to
/// the collection being captured: the client's wait gets shorter, at the
/// price of the other clients waiting this long, rather than half a
/// millisecond, between their commands for as long as it lasts.
const HELD_SLICE_MICROS: u128 = 2_000;
/// Bookkeeping bytes assumed for each queued job.
const QUEUED_JOB_OVERHEAD: usize = 64;
/// The writer thread's file buffer.
const WRITE_BUFFER_BYTES: usize = 64 * 1024;
/// After a failed automatic save, wait this long before trying again.
const RETRY_AFTER_MS: u64 = 5_000;

/// What the execution thread hands to the writer thread.
enum Job {
    /// One or more entries of one database as they were when the save began.
    Entries {
        db: usize,
        body: Body,
        /// How many keys this is.
        keys: u64,
        /// What it was counted as in the queue.
        cost: usize,
    },
    /// Everything has been sent: make the file the snapshot.
    Commit,
}

/// The two forms in which entries travel to the writer thread.
enum Body {
    /// A long string, still to be encoded. Strings are immutable and
    /// reference-counted, so `value` is the stored string itself, not a
    /// copy: taking it costs the same for a hundred megabytes as for
    /// sixty-four kilobytes.
    String {
        key: Bytes,
        value: Bytes,
        expire_at: Option<u64>,
    },
    /// Entries already encoded: short strings, for which a copy is cheap,
    /// and collections, which can change in place and so have to be
    /// captured before the command goes on.
    Encoded(Vec<u8>),
    /// One large collection, encoded in pieces that follow one another in
    /// the file.
    Parts(Vec<Vec<u8>>),
}

/// Called by the writer thread when the execution thread should look at
/// the save again.
pub type SaveWaker = Arc<dyn Fn() + Send + Sync>;

/// What the two threads of a background save share besides the channel.
struct Queue {
    state: Mutex<QueueState>,
    /// Set by the writer when it fails, and by the execution thread when
    /// the save is abandoned; either way the other side stops too.
    stopped: AtomicBool,
    waker: Option<SaveWaker>,
}

#[derive(Default)]
struct QueueState {
    /// Approximate bytes of the entries waiting to be written.
    bytes: usize,
    /// The walk found the queue full and is waiting to be woken.
    walk_waiting: bool,
}

impl Queue {
    fn new(waker: Option<SaveWaker>) -> Queue {
        Queue {
            state: Mutex::new(QueueState::default()),
            stopped: AtomicBool::new(false),
            waker,
        }
    }

    fn stopped(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    fn wake(&self) {
        if let Some(waker) = &self.waker {
            waker();
        }
    }

    fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.wake();
    }

    /// Whether the walk has to hold back. If so, the writer wakes the
    /// execution thread once it has worked the queue down.
    fn is_full(&self) -> bool {
        let mut state = lock(&self.state);
        state.walk_waiting = state.bytes >= QUEUE_BYTES;
        state.walk_waiting
    }

    fn add(&self, cost: usize) {
        lock(&self.state).bytes += cost;
    }

    fn release(&self, cost: usize) {
        let mut state = lock(&self.state);
        state.bytes = state.bytes.saturating_sub(cost);
        let wake = state.walk_waiting && state.bytes <= RESUME_BYTES;
        if wake {
            state.walk_waiting = false;
        }
        drop(state);
        if wake {
            self.wake();
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Whether a value is captured piece by piece; see [`STAGED_MIN_ELEMENTS`].
fn is_large(value: &Value) -> bool {
    let elements = match value {
        Value::String(_) => return false,
        Value::List(list) => list.len(),
        Value::Hash(hash) => hash.len(),
        Value::Set(set) => set.len(),
        Value::ZSet(zset) => zset.len(),
    };
    elements >= STAGED_MIN_ELEMENTS
        // Few elements can still be many bytes; a sample of them tells.
        || (elements > 0 && crate::admin::value_size(value, 16) >= STAGED_MIN_BYTES)
}

/// The large collections of a save: those waiting to be captured piece by
/// piece, the one being captured, and those just finished.
#[derive(Default)]
struct Staged {
    /// Database and key of collections the walk has passed and left for
    /// later, and of those a client is waiting to write to (at the front,
    /// with `true`). An entry may be stale: saved or gone by the time its
    /// turn comes.
    waiting: VecDeque<(usize, Bytes, bool)>,
    current: Option<Partial>,
    /// Captured since anyone last asked; see [`take_captured`].
    captured: Vec<(usize, Bytes)>,
}

/// A large collection part of the way through being captured.
struct Partial {
    db: usize,
    key: Bytes,
    /// What has been encoded so far, in pieces of about [`PART_BYTES`]. An
    /// entry has to reach the file whole, so they are kept here until it is
    /// complete.
    parts: Vec<Vec<u8>>,
    progress: rdb::Progress,
    /// A client is waiting to write to it.
    urgent: bool,
}

impl Partial {
    fn new(db: usize, key: Bytes, urgent: bool) -> Partial {
        Partial {
            db,
            key,
            parts: Vec::new(),
            progress: rdb::Progress::default(),
            urgent,
        }
    }

    /// Encodes a little more of `value`, which must be the collection this
    /// is the capture of: up to `max_elements` elements or about
    /// `max_bytes` bytes. Returns whether that completes it.
    fn encode(
        &mut self,
        value: &Value,
        expire_at: Option<u64>,
        max_elements: usize,
        max_bytes: usize,
    ) -> bool {
        // A new piece once the last one is about full.
        if self
            .parts
            .last()
            .is_none_or(|part| part.len() + max_bytes.min(PART_BYTES / 4) > PART_BYTES)
        {
            self.parts.push(Vec::with_capacity(PART_BYTES));
        }
        let Some(part) = self.parts.last_mut() else {
            return false;
        };
        rdb::encode_some(
            part,
            &self.key,
            value,
            expire_at,
            &mut self.progress,
            max_elements,
            max_bytes,
        )
    }

    /// Encodes all the rest of `value`, however long that takes.
    fn encode_rest(&mut self, value: &Value, expire_at: Option<u64>) {
        while !self.encode(value, expire_at, usize::MAX, PART_BYTES / 4) {}
    }

    /// The memory the encoded pieces occupy.
    fn held(&self) -> usize {
        self.parts.iter().map(Vec::capacity).sum()
    }
}

/// Encoded entries of one database, collected until there are enough to be
/// worth a trip to the writer.
#[derive(Default)]
struct Pending {
    db: usize,
    bytes: Vec<u8>,
    keys: u64,
}

/// The execution thread's end of a background save: where the snapshot's
/// entries go. Each database's sink has its own copy; only the execution
/// thread uses them.
#[derive(Clone)]
struct Feed {
    tx: Sender<Job>,
    queue: Arc<Queue>,
    pending: Arc<Mutex<Pending>>,
    staged: Arc<Mutex<Staged>>,
}

impl Feed {
    fn new(tx: Sender<Job>, queue: Arc<Queue>) -> Feed {
        Feed {
            tx,
            queue,
            pending: Arc::default(),
            staged: Arc::default(),
        }
    }

    /// Takes one entry, or with `now` false may decline to (returning
    /// `false`) and come back for it.
    ///
    /// This never waits for the disk: the walk holds back when the queue is
    /// full (see [`bgsave_step`]), but an entry that is about to be changed
    /// has to be taken now, whatever the queue holds.
    fn entry(
        &self,
        db: usize,
        key: &Bytes,
        value: &Value,
        expire_at: Option<u64>,
        now: bool,
    ) -> bool {
        if self.queue.stopped() {
            return true;
        }
        if is_large(value) {
            let mut staged = lock(&self.staged);
            if !now {
                // Reached by the walk: captured a piece at a time, later.
                staged
                    .waiting
                    .push_back((db, Bytes::copy_from_slice(key), false));
                return false;
            }
            // About to change, and nobody can wait (a client's write does
            // not normally get here: it waits for the piecewise capture
            // instead; see `capture_blockers`. Transactions, scripts,
            // expiry and eviction do). The rest of it, or all of it if it
            // had not been started, is encoded here and now.
            let mut partial = staged
                .current
                .take_if(|partial| partial.db == db && partial.key == key)
                .unwrap_or_else(|| Partial::new(db, Bytes::copy_from_slice(key), false));
            partial.encode_rest(value, expire_at);
            self.send_partial(&mut staged, partial);
            return true;
        }
        match value {
            Value::String(text) if text.len() >= SHARED_STRING_BYTES => {
                let body = Body::String {
                    // A copy, not a share: sharing a buffer for the first
                    // time makes it carry a reference count for the rest of
                    // its life, and the key outlives the save.
                    key: Bytes::copy_from_slice(key),
                    value: text.clone(),
                    expire_at,
                };
                // What stays allocated if the key is overwritten meanwhile.
                self.send(db, body, 1, key.len() + text.len());
            }
            _ => {
                let mut pending = lock(&self.pending);
                if pending.db != db {
                    self.send_pending(&mut pending);
                    pending.db = db;
                }
                if pending.bytes.capacity() == 0 {
                    // Room for a whole batch at once: a buffer left to grow
                    // by doubling can hold twice what is in it.
                    pending.bytes.reserve_exact(BATCH_BYTES + BATCH_SLACK_BYTES);
                }
                rdb::encode_entry(&mut pending.bytes, key, value, expire_at);
                pending.keys += 1;
                if pending.bytes.len() >= BATCH_BYTES {
                    self.send_pending(&mut pending);
                }
            }
        }
        true
    }

    /// Captures a little more of the large collections that were left for
    /// later, until `started` is a slice ago. Returns whether any of that
    /// work remains.
    fn staged_step(&self, keyspace: &mut Keyspace, started: Instant) -> bool {
        let mut staged = lock(&self.staged);
        loop {
            if staged.current.is_none() {
                let Some((db, key, urgent)) = staged.waiting.pop_front() else {
                    return false;
                };
                // Saved in the meantime (a write got to it first), or gone.
                if keyspace.db(db).unsaved(&key).is_none() {
                    continue;
                }
                staged.current = Some(Partial::new(db, key, urgent));
            }
            let Some(partial) = staged.current.as_mut() else {
                continue;
            };
            let budget = if partial.urgent {
                HELD_SLICE_MICROS
            } else {
                SLICE_MICROS
            };
            let done = match keyspace.db(partial.db).unsaved(&partial.key) {
                Some((value, expire_at)) => {
                    partial.encode(value, expire_at, STAGED_STEP_ELEMENTS, STAGED_STEP_BYTES)
                }
                // Not expected: a change to it would have completed it.
                None => {
                    staged.current = None;
                    continue;
                }
            };
            if done {
                if let Some(partial) = staged.current.take() {
                    keyspace.db(partial.db).mark_saved(&partial.key);
                    self.send_partial(&mut staged, partial);
                }
            }
            if started.elapsed().as_micros() >= budget {
                return staged.current.is_some() || !staged.waiting.is_empty();
            }
        }
    }

    /// Sends a completed large collection to the writer.
    fn send_partial(&self, staged: &mut Staged, partial: Partial) {
        let held = partial.held();
        self.send(partial.db, Body::Parts(partial.parts), 1, held);
        staged.captured.push((partial.db, partial.key));
    }

    /// Has the collection at `key` captured next, because a client is
    /// waiting to write to it.
    fn prioritize(&self, db: usize, key: &[u8]) {
        let mut staged = lock(&self.staged);
        if let Some(partial) = staged
            .current
            .as_mut()
            .filter(|partial| partial.db == db && partial.key == key)
        {
            partial.urgent = true;
            return;
        }
        match staged.waiting.front_mut() {
            Some(front) if front.0 == db && front.1 == key => front.2 = true,
            _ => staged
                .waiting
                .push_front((db, Bytes::copy_from_slice(key), true)),
        }
    }

    /// Sends on whatever has been collected.
    fn flush(&self) {
        self.send_pending(&mut lock(&self.pending));
    }

    fn send_pending(&self, pending: &mut Pending) {
        if pending.bytes.is_empty() {
            return;
        }
        let bytes = std::mem::take(&mut pending.bytes);
        let keys = std::mem::take(&mut pending.keys);
        // What the buffer occupies, which can be more than what it holds.
        let held = bytes.capacity();
        self.send(pending.db, Body::Encoded(bytes), keys, held);
    }

    fn send(&self, db: usize, body: Body, keys: u64, bytes: usize) {
        let cost = bytes + QUEUED_JOB_OVERHEAD;
        self.queue.add(cost);
        let job = Job::Entries {
            db,
            body,
            keys,
            cost,
        };
        if self.tx.send(job).is_err() {
            self.queue.stop();
        }
    }
}

/// A background save in progress.
///
/// The execution thread walks the keyspace in small slices between commands
/// (see [`rexyra_engine::Db::begin_snapshot`] for how the result stays a
/// point-in-time image) and hands each entry to a writer thread, which
/// does the disk I/O. Long strings are handed over as they are (shared,
/// not copied) and encoded by the writer; everything else is encoded first.
///
/// Neither thread waits on a timer for the other. When the walk has to
/// pause for the writer, the execution thread simply goes back to its
/// commands, and the writer wakes it through [`Server::save_waker`].
pub struct BackgroundSave {
    /// For what is still collected and the commit message; `None` once the
    /// walk is over.
    feed: Option<Feed>,
    queue: Arc<Queue>,
    staged: Arc<Mutex<Staged>>,
    /// The writer thread's verdict, once it has finished.
    done: Receiver<io::Result<()>>,
    /// Still walking the keyspace; afterwards, waiting for the writer.
    scanning: bool,
    /// Unsaved changes when the save began; these are what it covers.
    dirty_at_start: u64,
    /// The file is the base of a rewritten append-only file rather than
    /// the RDB snapshot.
    for_aof: bool,
}

/// What the writer thread needs to know about the server as it was when
/// the save began.
struct Header {
    /// The RDB format version; see [`rdb_version`].
    version: u16,
    now: u64,
    libraries: Vec<Bytes>,
    /// Keys and keys-with-TTL of each database.
    sizes: Vec<(usize, usize)>,
    /// Microseconds to pause per key; see `Server::rdb_key_save_delay`.
    delay_micros: u64,
}

/// Writes the entries it receives into `temporary`, then renames that to
/// `path` on commit. If the channel closes without a commit, or the save is
/// stopped, the file is removed.
fn writer_thread(
    rx: Receiver<Job>,
    queue: &Queue,
    file: File,
    header: Header,
    temporary: &Path,
    path: &Path,
) -> io::Result<()> {
    let abandoned = || io::Error::other("the background save was abandoned");
    let outcome = (|| {
        let mut writer = Writer::with_version(
            BufWriter::with_capacity(WRITE_BUFFER_BYTES, file),
            header.version,
        )?;
        write_header(&mut writer, header.now, header.libraries.iter())?;
        let mut current_db = None;
        // Pauses shorter than the clock can time are saved up.
        let mut owed_micros = 0;
        loop {
            match rx.recv() {
                Ok(Job::Entries {
                    db,
                    body,
                    keys,
                    cost,
                }) => {
                    if queue.stopped() {
                        return Err(abandoned());
                    }
                    // Entries arrive in whatever order they were taken, so
                    // the file switches database whenever the next ones
                    // belong to another one.
                    if current_db != Some(db) {
                        current_db = Some(db);
                        let (keys, expiring) = header.sizes[db];
                        writer.select_db(db, keys, expiring)?;
                    }
                    // The body is freed here, on this thread, before the
                    // queue is told there is room again.
                    let written = match body {
                        Body::String {
                            key,
                            value,
                            expire_at,
                        } => writer.entry(&key, &Value::String(value), expire_at),
                        Body::Encoded(bytes) => writer.encoded(&bytes),
                        Body::Parts(parts) => {
                            parts.iter().try_for_each(|part| writer.encoded(part))
                        }
                    };
                    owed_micros += header.delay_micros * keys;
                    if owed_micros >= 1_000 {
                        std::thread::sleep(Duration::from_micros(owed_micros));
                        owed_micros = 0;
                    }
                    queue.release(cost);
                    written?;
                }
                Ok(Job::Commit) => {
                    let file = writer.finish()?.into_inner().map_err(|e| e.into_error())?;
                    // On disk before the rename makes it the snapshot.
                    file.sync_all()?;
                    drop(file);
                    return replace_file(temporary, path);
                }
                Err(_) => return Err(abandoned()),
            }
        }
    })();
    if outcome.is_err() {
        // Tell the execution thread to stop sending. Whatever is still in
        // the channel is dropped with the receiver.
        queue.stop();
        let _ = fs::remove_file(temporary);
    }
    outcome
}

/// Starts saving the snapshot in the background.
pub fn start_bgsave(server: &mut Server, keyspace: &mut Keyspace, now: u64) -> io::Result<()> {
    if let Some(running) = &server.bgsave {
        return Err(io::Error::other(if running.for_aof {
            "Can't BGSAVE while AOF log rewriting is in progress"
        } else {
            "Background save already in progress"
        }));
    }
    server.last_bgsave_try = now;
    server.bgsave_scheduled = false;
    let path = snapshot_path(server);
    let temporary = Path::new(&server.dir).join(format!("temp-bg-{}.rdb", std::process::id()));
    start_background(server, keyspace, now, path, temporary, false)
}

/// Starts writing a point-in-time snapshot to `path` (by way of a file
/// named after `temporary`) in the background. `for_aof` says it is the
/// base of a rewritten append-only file.
pub(crate) fn start_background(
    server: &mut Server,
    keyspace: &mut Keyspace,
    now: u64,
    path: PathBuf,
    temporary: PathBuf,
    for_aof: bool,
) -> io::Result<()> {
    // Every save gets a file of its own, so that the writer of an abandoned
    // save, which may still be tidying up, cannot touch this one's.
    static SAVES: AtomicU64 = AtomicU64::new(0);
    let mut name = temporary.into_os_string();
    name.push(format!(".{}", SAVES.fetch_add(1, Ordering::Relaxed)));
    let temporary = PathBuf::from(name);
    let file = File::create(&temporary)?;

    let header = Header {
        version: rdb_version(keyspace),
        delay_micros: server.rdb_key_save_delay,
        now,
        libraries: server
            .scripting
            .as_ref()
            .map(|engine| engine.library_sources().cloned().collect())
            .unwrap_or_default(),
        sizes: (0..keyspace.len())
            .map(|index| {
                let db = keyspace.db(index);
                (db.len(), db.volatile_len())
            })
            .collect(),
    };
    let queue = Arc::new(Queue::new(server.save_waker.clone()));
    let (tx, rx) = mpsc::channel();
    let (done_tx, done) = mpsc::channel();
    let shared = Arc::clone(&queue);
    let spawned = std::thread::Builder::new()
        .name("rdb-writer".to_owned())
        .spawn({
            let temporary = temporary.clone();
            move || {
                let outcome = writer_thread(rx, &shared, file, header, &temporary, &path);
                let _ = done_tx.send(outcome);
                // So that the end of the save is noticed at once.
                shared.wake();
            }
        });
    if let Err(error) = spawned {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }

    let feed = Feed::new(tx, Arc::clone(&queue));
    keyspace.begin_snapshot(now, |index| {
        let feed = feed.clone();
        Box::new(move |key, value, expire_at, now| feed.entry(index, key, value, expire_at, now))
    });
    server.bgsave = Some(BackgroundSave {
        staged: Arc::clone(&feed.staged),
        feed: Some(feed),
        queue,
        done,
        scanning: true,
        dirty_at_start: server.dirty,
        for_aof,
    });
    Ok(())
}

/// Does one slice of a background save's work, if one is running: walks a
/// little more of the keyspace, or checks whether the writer has finished.
/// Call between commands.
///
/// Returns `true` if there is more walking that could be done right now,
/// in which case calling again soon makes the save finish sooner. `false`
/// means there is nothing to do for the moment: no save, or one that is
/// waiting for its writer, which will call [`Server::save_waker`] when
/// that changes.
pub fn bgsave_step(server: &mut Server, keyspace: &mut Keyspace, now: u64) -> bool {
    let Some(save) = &mut server.bgsave else {
        return false;
    };
    if save.scanning {
        let stopped = save.queue.stopped();
        let mut walked = stopped;
        if !stopped {
            let started = Instant::now();
            // Large collections first, a piece at a time: a client may be
            // waiting for one of them.
            let staged_left = save
                .feed
                .as_ref()
                .is_some_and(|feed| feed.staged_step(keyspace, started));
            if staged_left {
                return true;
            }
            let mut buckets = 0;
            loop {
                // While the writer has plenty to do, the walk waits: nothing
                // is lost by saving an entry later, and memory stays
                // bounded.
                if save.queue.is_full() {
                    return false;
                }
                if keyspace.snapshot_step(BUCKETS_PER_STEP) {
                    // The walk has been everywhere; what it left for later
                    // is the rest of the work.
                    let staged = lock(&save.staged);
                    walked = staged.current.is_none() && staged.waiting.is_empty();
                    break;
                }
                buckets += BUCKETS_PER_STEP;
                if buckets >= BUCKETS_PER_SLICE || started.elapsed().as_micros() >= SLICE_MICROS {
                    break;
                }
            }
        }
        if !walked {
            return true;
        }
        // Dropping the sinks and then this feed closes the channel.
        keyspace.end_snapshot();
        save.scanning = false;
        if let Some(feed) = save.feed.take() {
            if !stopped {
                feed.flush();
                let _ = feed.tx.send(Job::Commit);
            }
        }
    }
    let outcome = match save.done.try_recv() {
        Ok(outcome) => outcome,
        Err(mpsc::TryRecvError::Empty) => return false,
        Err(mpsc::TryRecvError::Disconnected) => {
            Err(io::Error::other("the snapshot writer stopped"))
        }
    };
    let covered = save.dirty_at_start;
    let for_aof = save.for_aof;
    server.bgsave = None;
    if for_aof {
        if let Some(aof) = &mut server.aof {
            aof.finish_rewrite(outcome.is_ok());
        }
        server.logs.push(match outcome {
            Ok(()) => (
                2,
                "Background AOF rewrite terminated with success".to_owned(),
            ),
            Err(error) => (3, format!("Background AOF rewrite error: {error}")),
        });
        return false;
    }
    server.last_save_ok = outcome.is_ok();
    match outcome {
        Ok(()) => {
            // Changes made while it ran are still unsaved.
            server.dirty = server.dirty.saturating_sub(covered);
            server.last_save = now / 1000;
            server
                .logs
                .push((2, "Background saving terminated with success".to_owned()));
        }
        Err(error) => server
            .logs
            .push((3, format!("Background saving error: {error}"))),
    }
    false
}

/// Whether a background save is still capturing the keyspace (as opposed
/// to none running, or one only waiting for its writer to finish).
pub fn capturing(server: &Server) -> bool {
    server.bgsave.as_ref().is_some_and(|save| save.scanning)
}

/// The large collections whose capture has completed since the last call,
/// by database and key. Clients held back by [`capture_blockers`] on one of
/// them can go on.
pub fn take_captured(server: &mut Server) -> Vec<(usize, Bytes)> {
    match &server.bgsave {
        Some(save) => std::mem::take(&mut lock(&save.staged).captured),
        None => Vec::new(),
    }
}

/// The keys of a write command that it should wait for before running:
/// large collections a background save has still to capture.
///
/// Running the command at once would mean capturing all of each first,
/// with every client waiting. Instead the caller holds back just this
/// client, the save captures those collections next, a piece at a time
/// between other clients' commands, and the command runs when
/// [`take_captured`] reports them done (or [`capturing`] turns false).
pub(crate) fn capture_blockers(
    ctx: &mut Ctx<'_>,
    spec: &CommandSpec,
    args: &[Bytes],
) -> Vec<Bytes> {
    let Some(feed) = ctx
        .server
        .bgsave
        .as_ref()
        .filter(|save| save.scanning)
        .and_then(|save| save.feed.as_ref())
    else {
        return Vec::new();
    };
    let db = ctx.client.db;
    let mut blockers = Vec::new();
    for key in acl::command_keys(spec, args) {
        let large = ctx
            .keyspace
            .db(db)
            .unsaved(key)
            .is_some_and(|(value, _)| is_large(value));
        if large {
            feed.prioritize(db, key);
            blockers.push(copied(key));
        }
    }
    blockers
}

/// Abandons a background save, if one is running. Its writer thread stops
/// and removes the unfinished file.
pub fn abort_bgsave(server: &mut Server, keyspace: &mut Keyspace) {
    if let Some(save) = server.bgsave.take() {
        save.queue.stop();
        keyspace.end_snapshot();
        if save.for_aof {
            if let Some(aof) = &mut server.aof {
                aof.finish_rewrite(false);
            }
        }
    }
}

/// Called before `FLUSHALL` or `FLUSHDB` empties a database. A background
/// save in progress would first have to take every entry it has not
/// reached, only for the data to be thrown away, so it is abandoned
/// instead, and a new one is started once the command (or the transaction
/// or script it is part of) has finished.
pub(crate) fn interrupt_for_flush(server: &mut Server, keyspace: &mut Keyspace) {
    let Some(save) = &server.bgsave else {
        return;
    };
    let for_aof = save.for_aof;
    abort_bgsave(server, keyspace);
    if for_aof {
        server.aof_rewrite_scheduled = true;
    } else {
        server.bgsave_scheduled = true;
    }
    server.logs.push((
        2,
        "Background save abandoned because the data was flushed; starting again".to_owned(),
    ));
}

/// What a server does before it stops: saves the snapshot if anything
/// changed since the last save, and makes sure the append-only file is on
/// disk. Returns whether it saved a snapshot.
pub fn prepare_exit(server: &mut Server, keyspace: &mut Keyspace, now: u64) -> io::Result<bool> {
    let saved = if server.dirty > 0 {
        save(server, keyspace, now).map(|()| true)
    } else {
        Ok(false)
    };
    if let Some(aof) = &mut server.aof {
        aof.close(now);
    }
    saved
}

/// Starts a background save if one of the configured save points has been
/// reached: at least that many changes, and that many seconds since the
/// last save. Call periodically.
pub fn autosave(server: &mut Server, keyspace: &mut Keyspace, now: u64) {
    if server.bgsave.is_some() {
        return;
    }
    // A save that a flush interrupted starts again, whatever the save
    // points say.
    if server.bgsave_scheduled {
        if let Err(error) = start_bgsave(server, keyspace, now) {
            server.bgsave_scheduled = false;
            server.last_save_ok = false;
            server
                .logs
                .push((3, format!("Can't save in background: {error}")));
        }
        return;
    }
    let since_save = (now / 1000).saturating_sub(server.last_save);
    let due = server
        .save_points
        .iter()
        .any(|&(seconds, changes)| server.dirty >= changes && since_save >= seconds);
    let may_retry =
        server.last_save_ok || now.saturating_sub(server.last_bgsave_try) >= RETRY_AFTER_MS;
    if due && may_retry {
        let message = format!(
            "{} changes in {since_save} seconds. Saving...",
            server.dirty
        );
        match start_bgsave(server, keyspace, now) {
            Ok(()) => server.logs.push((2, message)),
            Err(error) => {
                server.last_save_ok = false;
                server
                    .logs
                    .push((3, format!("Can't save in background: {error}")));
            }
        }
    }
}

/// Parses save points: pairs of seconds and changes, as in `save 900 1`.
pub fn parse_save_points(text: &str) -> Option<Vec<(u64, u64)>> {
    let numbers: Vec<u64> = text
        .split_whitespace()
        .map(|word| word.parse().ok())
        .collect::<Option<_>>()?;
    if !numbers.len().is_multiple_of(2) {
        return None;
    }
    Some(numbers.chunks(2).map(|pair| (pair[0], pair[1])).collect())
}

/// Save points in the form `CONFIG GET save` shows them.
pub fn format_save_points(points: &[(u64, u64)]) -> String {
    points
        .iter()
        .map(|(seconds, changes)| format!("{seconds} {changes}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// What [`load`] found in the snapshot.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Loaded {
    pub keys: usize,
    /// Keys left out because their TTL had already passed, or because they
    /// were hashes all of whose fields' TTLs had.
    pub expired: usize,
    /// Hash fields left out because their TTL had already passed.
    pub expired_fields: usize,
    pub libraries: usize,
}

/// Loads the snapshot file, if there is one, into a new keyspace, and its
/// function libraries into `server`. `Ok(None)` means there is no file.
pub fn load(server: &mut Server, now: u64) -> Result<Option<(Keyspace, Loaded)>, String> {
    let path = snapshot_path(server);
    let data = match fs::read(&path) {
        Ok(data) => data,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("cannot read {}: {error}", path.display())),
    };
    let (keyspace, loaded) = load_bytes(server, &data, now)
        .map_err(|error| format!("cannot load {}: {error}", path.display()))?;
    server.last_save = now / 1000;
    Ok(Some((keyspace, loaded)))
}

/// Loads RDB `data` into a new keyspace, and its function libraries into
/// `server`. Keys whose TTL is at or before `now` are left out.
pub(crate) fn load_bytes(
    server: &mut Server,
    data: &[u8],
    now: u64,
) -> Result<(Keyspace, Loaded), String> {
    let mut keyspace = Keyspace::new(server.databases);
    let mut loaded = Loaded::default();
    let mut libraries: Vec<Bytes> = Vec::new();
    let mut out_of_range = None;
    rdb::read(data, |item| match item {
        Item::Aux { .. } => {}
        Item::Function(code) => libraries.push(code),
        Item::Entry {
            db,
            key,
            value,
            expire_at,
        } => {
            if db >= server.databases {
                out_of_range.get_or_insert(db);
            } else if expire_at.is_some_and(|at| at <= now) {
                loaded.expired += 1;
            } else {
                let mut value = value;
                // Hash fields whose TTL has passed are left out, and a hash
                // left with none, as Redis does.
                if let Value::Hash(hash) = &mut value {
                    loaded.expired_fields += hash.drop_expired(now);
                }
                if value.is_empty_collection() {
                    loaded.expired += 1;
                } else {
                    let ttl = expire_at.map_or(SetTtl::Clear, SetTtl::At);
                    keyspace.db(db).set(&key, value, ttl, now);
                    loaded.keys += 1;
                }
            }
        }
    })
    .map_err(|error| error.to_string())?;
    if let Some(db) = out_of_range {
        return Err(format!(
            "it uses database {db}, but only {} are configured",
            server.databases
        ));
    }
    loaded.libraries = libraries.len();
    scripting::restore_libraries(server, &libraries)?;
    Ok((keyspace, loaded))
}

// --- commands ---------------------------------------------------------------

/// `SAVE` — writes the snapshot before replying.
pub(crate) fn save_command(ctx: &mut Ctx<'_>, _args: &[Bytes]) -> Frame {
    if ctx.server.bgsave.is_some() {
        return Frame::error("ERR Background save already in progress");
    }
    match save(ctx.server, ctx.keyspace, ctx.now) {
        Ok(()) => Frame::ok(),
        Err(error) => Frame::error(format!("ERR {error}")),
    }
}

/// `BGSAVE [SCHEDULE]` — starts writing the snapshot in the background and
/// replies at once.
pub(crate) fn bgsave(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let schedule = match &args[1..] {
        [] => false,
        [option] if is(option, "SCHEDULE") => true,
        _ => return Frame::error(ERR_SYNTAX),
    };
    // As in Redis: a save in progress is an error; behind a rewrite, or
    // inside `EXEC`, the save is scheduled (with `SCHEDULE`, or always in a
    // transaction) and started by the periodic tick once it can be.
    let rewriting = match &ctx.server.bgsave {
        Some(running) if !running.for_aof => {
            return Frame::error("ERR Background save already in progress")
        }
        Some(_) => true,
        None => false,
    };
    if rewriting || ctx.server.in_exec {
        if schedule || ctx.server.in_exec {
            ctx.server.bgsave_scheduled = true;
            return Frame::simple("Background saving scheduled");
        }
        return Frame::error(
            "ERR Another child process is active (AOF?): can't BGSAVE right now. Use BGSAVE SCHEDULE in order to schedule a BGSAVE whenever possible.",
        );
    }
    match start_bgsave(ctx.server, ctx.keyspace, ctx.now) {
        Ok(()) => Frame::simple("Background saving started"),
        Err(error) => Frame::error(format!("ERR {error}")),
    }
}

/// `LASTSAVE` — when the snapshot was last written or loaded, in seconds
/// since the Unix epoch.
pub(crate) fn lastsave(ctx: &mut Ctx<'_>, _args: &[Bytes]) -> Frame {
    Frame::Integer(ctx.server.last_save as i64)
}

/// `SHUTDOWN [NOSAVE | SAVE] [NOW] [FORCE] [ABORT]`
///
/// Saves first if anything changed since the last save (`SAVE` forces a
/// save, `NOSAVE` skips it). If the save fails the server keeps running.
pub(crate) fn shutdown(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (mut save_requested, mut no_save, mut abort) = (false, false, false);
    for option in &args[1..] {
        if is(option, "SAVE") {
            save_requested = true;
        } else if is(option, "NOSAVE") {
            no_save = true;
        } else if is(option, "ABORT") {
            abort = true;
        } else if !is(option, "NOW") && !is(option, "FORCE") {
            return Frame::error(ERR_SYNTAX);
        }
    }
    if (save_requested && no_save) || (abort && args.len() > 2) {
        return Frame::error(ERR_SYNTAX);
    }
    if abort {
        return Frame::error("ERR No shutdown in progress.");
    }
    if (save_requested || (!no_save && ctx.server.dirty > 0))
        && save(ctx.server, ctx.keyspace, ctx.now).is_err()
    {
        return Frame::error("ERR Errors trying to SHUTDOWN. Check logs.");
    }
    if let Some(aof) = &mut ctx.server.aof {
        aof.close(ctx.now);
    }
    ctx.server.effects.push(Effect::Shutdown);
    ctx.client.close_after_reply = true;
    Frame::ok()
}

/// `DUMP key` — the value in a form `RESTORE` accepts, or null.
pub(crate) fn dump(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let (db, now) = ctx.db();
    match db.get(&args[1], now) {
        Some(value) => Frame::Bulk(Bytes::from(rdb::dump_value(value))),
        None => Frame::Null,
    }
}

/// `RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds]
/// [FREQ frequency]`
pub(crate) fn restore(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    reply((|| {
        let (key, payload) = (&args[1], &args[3]);
        let (mut replace, mut absolute) = (false, false);
        let mut options = &args[4..];
        loop {
            options = match options {
                [] => break,
                [option, rest @ ..] if is(option, "REPLACE") => {
                    replace = true;
                    rest
                }
                [option, rest @ ..] if is(option, "ABSTTL") => {
                    absolute = true;
                    rest
                }
                // Accepted and checked; this server keeps no such statistics.
                [option, value, rest @ ..] if is(option, "IDLETIME") => {
                    if !matches!(int(value), Ok(seconds) if seconds >= 0) {
                        return Err(Frame::error("ERR Invalid IDLETIME value, must be >= 0"));
                    }
                    rest
                }
                [option, value, rest @ ..] if is(option, "FREQ") => {
                    if !matches!(int(value), Ok(0..=255)) {
                        return Err(Frame::error(
                            "ERR Invalid FREQ value, must be >= 0 and <= 255",
                        ));
                    }
                    rest
                }
                _ => return Err(Frame::error(ERR_SYNTAX)),
            };
        }
        let ttl = match int(&args[2])? {
            ttl if ttl >= 0 => ttl as u64,
            _ => return Err(Frame::error("ERR Invalid TTL value, must be >= 0")),
        };

        let (db, now) = ctx.db();
        if !replace && db.exists(key, now) {
            return Err(Frame::error("BUSYKEY Target key name already exists."));
        }
        let mut value = rdb::restore_value(payload).map_err(|error| {
            Frame::error(match error {
                rdb::RdbError::BadChecksum
                | rdb::RdbError::UnsupportedVersion(_)
                | rdb::RdbError::Truncated => {
                    "ERR DUMP payload version or checksum are wrong".to_owned()
                }
                rdb::RdbError::Unsupported(what) => format!("ERR Bad data format: {what}"),
                _ => "ERR Bad data format".to_owned(),
            })
        })?;
        let expire_at = match (ttl, absolute) {
            (0, _) => None,
            (at, true) => Some(at),
            (ttl, false) => Some(now.saturating_add(ttl)),
        };
        // Hash fields whose TTL has passed are left out, as Redis does; a
        // hash left with none is not created.
        let dropped_fields = match &mut value {
            Value::Hash(hash) => hash.drop_expired(now),
            _ => 0,
        };
        let already_expired = expire_at.is_some_and(|at| at <= now) || value.is_empty_collection();
        // Logged without the fields left out: on replay nothing expires.
        let restored = (dropped_fields > 0 && !already_expired).then(|| rdb::dump_value(&value));
        // A key replaced is deleted first; the restored one is new.
        let deleted = db.remove(key, now);
        if already_expired {
            // The key ends up not existing.
            if deleted {
                crate::notify::notify(ctx, crate::notify::GENERIC, "del", key);
            }
        } else {
            let ttl = expire_at.map_or(SetTtl::Clear, SetTtl::At);
            db.set(key, value, ttl, now);
            crate::notify::notify(ctx, crate::notify::GENERIC, "restore", key);
        }
        // Logged with an absolute expiry, whichever way it was given.
        ctx.log_instead(|| {
            vec![if already_expired {
                crate::aof::command(&[b"DEL", key])
            } else {
                crate::aof::command(&[
                    b"RESTORE",
                    key,
                    expire_at.unwrap_or(0).to_string().as_bytes(),
                    restored.as_deref().unwrap_or(payload),
                    b"REPLACE",
                    b"ABSTTL",
                ])
            }]
        });
        Ok(Frame::ok())
    })())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{bulk, bulks, err, Harness};
    use crate::{BlockKind, BlockRequest};

    /// A directory of its own for one test's snapshot files.
    fn temp_dir(name: &str) -> String {
        let dir =
            std::env::temp_dir().join(format!("rexyra-persist-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir.to_str().unwrap().to_owned()
    }

    fn harness_in(dir: &str) -> Harness {
        let mut h = Harness::new();
        h.server.dir = dir.to_owned();
        h
    }

    /// Starts a new server state from the snapshot `h` wrote.
    fn reload(h: &Harness) -> (Harness, Loaded) {
        let mut fresh = harness_in(&h.server.dir);
        fresh.now = h.now;
        let (keyspace, loaded) = load(&mut fresh.server, fresh.now).unwrap().unwrap();
        fresh.keyspace = keyspace;
        (fresh, loaded)
    }

    #[test]
    fn save_and_load_every_type() {
        let dir = temp_dir("types");
        let mut h = harness_in(&dir);
        h.run("SET str hello");
        h.run("SET counter 42");
        h.run("SET temp soon EX 100");
        h.run("RPUSH list a b c");
        h.run("HSET hash f1 v1 f2 v2");
        h.run("SADD set x y z");
        h.run("SADD ints 3 1 2");
        h.run("ZADD zset 1.5 a -2 b");
        h.run("SELECT 5");
        h.run("SET elsewhere five");
        h.run("SELECT 0");
        assert_eq!(h.run("SAVE"), Frame::ok());

        let (mut fresh, loaded) = reload(&h);
        assert_eq!(
            loaded,
            Loaded {
                keys: 9,
                expired: 0,
                expired_fields: 0,
                libraries: 0
            }
        );
        assert_eq!(fresh.run("GET str"), bulk("hello"));
        assert_eq!(fresh.run("INCR counter"), Frame::Integer(43));
        assert_eq!(fresh.run("TTL temp"), Frame::Integer(100));
        assert_eq!(
            fresh.run("LRANGE list 0 -1"),
            Frame::Array(bulks(&["a", "b", "c"]))
        );
        assert_eq!(fresh.run("HGET hash f2"), bulk("v2"));
        assert_eq!(fresh.run("SCARD set"), Frame::Integer(3));
        assert_eq!(fresh.run("OBJECT ENCODING ints"), bulk("intset"));
        assert_eq!(
            fresh.run("ZRANGE zset 0 -1 WITHSCORES"),
            Frame::Array(vec![
                bulk("b"),
                Frame::Double(-2.0),
                bulk("a"),
                Frame::Double(1.5)
            ])
        );
        assert_eq!(fresh.run("DBSIZE"), Frame::Integer(8));
        fresh.run("SELECT 5");
        assert_eq!(fresh.run("GET elsewhere"), bulk("five"));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn expired_keys_are_not_saved_or_loaded() {
        let dir = temp_dir("expiry");
        let mut h = harness_in(&dir);
        h.run("SET gone v PX 100");
        h.run("SET later v PX 5000");
        h.run("SET keep v");
        h.advance(1_000);
        // `gone` has expired but nothing has touched it since.
        h.run("SAVE");
        let (_, loaded) = reload(&h);
        assert_eq!(loaded.keys, 2);

        // `later` expires between the save and the next start.
        h.advance(10_000);
        let (mut fresh, loaded) = reload(&h);
        assert_eq!((loaded.keys, loaded.expired), (1, 1));
        assert_eq!(fresh.run("KEYS *"), Frame::Array(bulks(&["keep"])));
        fs::remove_dir_all(dir).unwrap();
    }

    fn ints(values: &[i64]) -> Frame {
        Frame::Array(values.iter().map(|&n| Frame::Integer(n)).collect())
    }

    #[test]
    fn hash_field_ttls_are_saved_and_loaded() {
        let dir = temp_dir("field-ttls");
        let mut h = harness_in(&dir);
        h.run("HSET plain a 1");
        h.run("SAVE");
        // Without field TTLs the file is in the format Redis 7.0 reads.
        assert!(fs::read(snapshot_path(&h.server))
            .unwrap()
            .starts_with(b"REDIS0011"));

        h.run("HSET small a 1 b 2 c 3");
        h.run("HPEXPIREAT small 1800000000000 FIELDS 1 b");
        h.run("HPEXPIRE small 1000 FIELDS 1 c");
        h.run("HSET gone x 1");
        h.run("HPEXPIRE gone 1000 FIELDS 1 x");
        let mut big = vec!["HSET".to_owned(), "big".to_owned()];
        for n in 0..1_000 {
            big.extend([format!("f{n}"), n.to_string()]);
        }
        h.cmd(&big.iter().map(String::as_str).collect::<Vec<_>>());
        h.run("HPEXPIREAT big 1800000000000 FIELDS 2 f1 f2");
        h.run("HPEXPIRE big 1000 FIELDS 1 f3");
        h.run("PEXPIRE big 100000");
        h.run("HSET persisted a 1");
        h.run("HPEXPIRE persisted 1000 FIELDS 1 a");
        h.run("HPERSIST persisted FIELDS 1 a");
        h.run("SAVE");
        // Field TTLs need the format Redis 7.4 introduced.
        assert!(fs::read(snapshot_path(&h.server))
            .unwrap()
            .starts_with(b"REDIS0012"));

        let (mut fresh, loaded) = reload(&h);
        assert_eq!(
            (loaded.keys, loaded.expired, loaded.expired_fields),
            (5, 0, 0)
        );
        let c_at = (h.now + 1000) as i64;
        assert_eq!(
            fresh.run("HPEXPIRETIME small FIELDS 3 a b c"),
            ints(&[-1, 1_800_000_000_000, c_at])
        );
        assert_eq!(
            fresh.run("HKEYS small"),
            Frame::Array(bulks(&["c", "b", "a"]))
        );
        assert_eq!(fresh.run("OBJECT ENCODING small"), bulk("listpackex"));
        assert_eq!(
            fresh.run("HPEXPIRETIME big FIELDS 4 f0 f1 f2 f3"),
            ints(&[-1, 1_800_000_000_000, 1_800_000_000_000, c_at])
        );
        assert_eq!(fresh.run("PTTL big"), Frame::Integer(100_000));
        assert_eq!(fresh.run("HLEN big"), Frame::Integer(1_000));
        assert_eq!(fresh.run("HGET persisted a"), bulk("1"));
        // The reloaded hashes reclaim their fields like any others.
        assert!(
            matches!(fresh.run("INFO keyspace"), Frame::Verbatim { data, .. }
            if data.ends_with(b"subexpiry=3\r\n"))
        );
        fresh.advance(1_000);
        fresh.keyspace.cron(fresh.now, 100);
        assert_eq!(fresh.run("HLEN big"), Frame::Integer(999));
        assert_eq!(fresh.run("EXISTS gone"), Frame::Integer(0));

        // Fields whose time passes before the next start are left out, and
        // a hash with none left with them.
        h.advance(5_000);
        let (mut later, loaded) = reload(&h);
        assert_eq!(
            (loaded.keys, loaded.expired, loaded.expired_fields),
            (4, 1, 3)
        );
        assert_eq!(later.run("HKEYS small"), Frame::Array(bulks(&["b", "a"])));
        assert_eq!(later.run("HLEN big"), Frame::Integer(999));
        assert_eq!(later.run("EXISTS gone"), Frame::Integer(0));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn bgsave_captures_field_ttls_as_they_were() {
        let dir = temp_dir("bgsave-field-ttls");
        let mut h = harness_in(&dir);
        large_collections(&mut h, 20_000);
        for n in 0..2_000 {
            h.cmd(&["HSET", &format!("h:{n}"), "a", "1", "b", "2"]);
            h.cmd(&["HPEXPIRE", &format!("h:{n}"), "500", "FIELDS", "1", "a"]);
        }
        let mut expire = vec!["HPEXPIRE", "bighash", "500", "FIELDS", "1000"];
        let names: Vec<String> = (0..1_000).map(|n| format!("field-{n}")).collect();
        expire.extend(names.iter().map(String::as_str));
        h.cmd(&expire);
        let started = h.now;
        h.run("BGSAVE");
        // While it runs: TTLs change, and fields expire and are reclaimed,
        // in the big hash too, which cannot wait for its capture there.
        h.run("HPERSIST h:0 FIELDS 1 a");
        h.run("HPEXPIRE h:1 10000 FIELDS 1 b");
        h.run("HDEL h:2 a");
        h.advance(1_000);
        h.keyspace.cron(h.now, 100_000);
        assert_eq!(h.run("HLEN h:3"), Frame::Integer(1));
        assert_eq!(h.run("HLEN bighash"), Frame::Integer(19_000));
        finish_bgsave(&mut h);
        assert!(h.server.last_save_ok);

        // Loaded as of the moment the save began.
        let mut fresh = harness_in(&dir);
        fresh.now = started;
        let (keyspace, _) = load(&mut fresh.server, fresh.now).unwrap().unwrap();
        fresh.keyspace = keyspace;
        let at = (started + 500) as i64;
        for n in 0..4 {
            assert_eq!(
                fresh.cmd(&["HPEXPIRETIME", &format!("h:{n}"), "FIELDS", "2", "a", "b"]),
                ints(&[at, -1]),
                "h:{n}"
            );
        }
        assert_eq!(fresh.run("HLEN bighash"), Frame::Integer(20_000));
        assert_eq!(
            fresh.run("HPEXPIRETIME bighash FIELDS 2 field-0 field-1000"),
            ints(&[at, -1])
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn function_libraries_are_saved_and_loaded() {
        let dir = temp_dir("functions");
        let mut h = harness_in(&dir);
        h.cmd(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=lib\nredis.register_function('double', function(keys, args) return args[1] * 2 end)",
        ]);
        h.run("SAVE");
        let (mut fresh, loaded) = reload(&h);
        assert_eq!(loaded.libraries, 1);
        assert_eq!(fresh.run("FCALL double 0 21"), Frame::Integer(42));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn no_file_is_not_an_error_but_a_bad_file_is() {
        let dir = temp_dir("bad");
        let mut h = harness_in(&dir);
        assert!(load(&mut h.server, h.now).unwrap().is_none());

        fs::write(snapshot_path(&h.server), b"not a snapshot").unwrap();
        let error = load(&mut h.server, h.now).map(|_| ()).unwrap_err();
        assert!(error.contains("not an RDB file"), "{error}");

        // A snapshot that uses a database this server is not configured for.
        h.run("SELECT 9");
        h.run("SET k v");
        h.run("SAVE");
        let mut small = harness_in(&dir);
        small.server.databases = 4;
        let error = load(&mut small.server, small.now).map(|_| ()).unwrap_err();
        assert!(error.contains("database 9"), "{error}");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn save_replaces_the_previous_snapshot_and_tracks_changes() {
        let dir = temp_dir("replace");
        let mut h = harness_in(&dir);
        h.run("SET k one");
        h.run("LPUSH l a");
        assert_eq!(h.server.dirty, 2);
        h.run("GET k");
        h.run("INCR k"); // fails: not a change
        assert_eq!(h.server.dirty, 2);
        h.run("SAVE");
        assert_eq!(h.server.dirty, 0);
        assert_eq!(h.run("LASTSAVE"), Frame::Integer((h.now / 1000) as i64));

        h.advance(5_000);
        h.run("SET k two");
        h.run("SAVE");
        assert_eq!(h.run("LASTSAVE"), Frame::Integer((h.now / 1000) as i64));
        let (mut fresh, _) = reload(&h);
        assert_eq!(fresh.run("GET k"), bulk("two"));
        // Only the snapshot is left in the directory.
        let files: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(files, ["dump.rdb"]);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_failed_save_is_reported() {
        let mut h = harness_in("this/directory/does/not/exist");
        h.run("SET k v");
        assert!(matches!(h.run("SAVE"), Frame::Error(text) if text.starts_with("ERR ")));
        assert_eq!(h.server.dirty, 1);
        assert!(!h.server.last_save_ok);
        // SHUTDOWN refuses to stop the server when it cannot save.
        assert_eq!(
            h.run("SHUTDOWN"),
            err("ERR Errors trying to SHUTDOWN. Check logs.")
        );
        assert!(h.server.effects.is_empty());
        assert_eq!(h.run("SHUTDOWN NOSAVE"), Frame::ok());
        assert_eq!(h.server.effects, [Effect::Shutdown]);
    }

    #[test]
    fn shutdown_saves_what_changed() {
        let dir = temp_dir("shutdown");
        let mut h = harness_in(&dir);
        // Nothing changed: nothing to save.
        assert_eq!(h.run("SHUTDOWN"), Frame::ok());
        assert!(!snapshot_path(&h.server).exists());
        h.server.effects.clear();

        h.run("SET k v");
        assert_eq!(h.run("SHUTDOWN"), Frame::ok());
        assert_eq!(h.server.effects, [Effect::Shutdown]);
        assert!(h.client.close_after_reply);
        let (mut fresh, _) = reload(&h);
        assert_eq!(fresh.run("GET k"), bulk("v"));

        for (command, message) in [
            ("SHUTDOWN ABORT", "ERR No shutdown in progress."),
            ("SHUTDOWN SAVE NOSAVE", "ERR syntax error"),
            ("SHUTDOWN BOGUS", "ERR syntax error"),
        ] {
            assert_eq!(h.run(command), err(message), "{command}");
        }
        fs::remove_dir_all(dir).unwrap();
    }

    /// Runs background-save slices until the save has finished.
    fn finish_bgsave(h: &mut Harness) {
        for _ in 0..10_000 {
            bgsave_step(&mut h.server, &mut h.keyspace, h.now);
            if h.server.bgsave.is_none() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("the background save never finished");
    }

    #[test]
    fn bgsave_captures_the_moment_it_started() {
        let dir = temp_dir("bgsave");
        let mut h = harness_in(&dir);
        for n in 0..2_000 {
            h.cmd(&["SET", &format!("key:{n}"), &n.to_string()]);
        }
        h.run("RPUSH list a b c");
        h.run("HSET hash f v");
        h.run("SET doomed bye");
        h.run("SET temp v PX 500");
        h.run("SELECT 2");
        h.run("SET other-db here");
        h.run("SELECT 0");
        assert_eq!(h.server.dirty, 2_005);

        assert_eq!(h.run("BGSAVE"), Frame::simple("Background saving started"));
        assert!(h.server.bgsave.is_some());
        // Nothing is on disk yet, and the server keeps taking commands.
        assert!(!snapshot_path(&h.server).exists());
        h.run("SET key:0 changed");
        h.run("SET key:1999 changed");
        h.run("DEL doomed");
        h.run("RPUSH list d");
        h.run("HSET hash f other g new");
        h.run("SET born-later x");
        h.run("PEXPIRE key:5 1");
        h.advance(1_000); // key:5 and temp expire while the save runs
        h.run("GET key:5");
        h.run("SELECT 2");
        h.run("DEL other-db");
        h.run("SELECT 0");
        assert_eq!(
            h.run("BGSAVE"),
            err("ERR Background save already in progress")
        );
        assert_eq!(
            h.run("SAVE"),
            err("ERR Background save already in progress")
        );
        let during = h.server.dirty - 2_005;
        finish_bgsave(&mut h);
        assert!(h.server.last_save_ok);
        // Only what changed after the save began is still unsaved.
        assert_eq!(h.server.dirty, during);
        assert_eq!(h.run("LASTSAVE"), Frame::Integer((h.now / 1000) as i64));

        // Load it as of the moment the save began (before the clock moved).
        let mut fresh = harness_in(&dir);
        fresh.now = h.now - 1_000;
        let (keyspace, loaded) = load(&mut fresh.server, fresh.now).unwrap().unwrap();
        fresh.keyspace = keyspace;
        assert_eq!(loaded.keys, 2_005);
        assert_eq!(fresh.run("GET key:0"), bulk("0"));
        assert_eq!(fresh.run("GET key:5"), bulk("5"));
        assert_eq!(fresh.run("GET key:1999"), bulk("1999"));
        assert_eq!(fresh.run("GET doomed"), bulk("bye"));
        assert_eq!(fresh.run("PTTL temp"), Frame::Integer(500));
        assert_eq!(
            fresh.run("LRANGE list 0 -1"),
            Frame::Array(bulks(&["a", "b", "c"]))
        );
        assert_eq!(fresh.run("HGETALL hash"), {
            Frame::Map(vec![(bulk("f"), bulk("v"))])
        });
        assert_eq!(fresh.run("EXISTS born-later"), Frame::Integer(0));
        assert_eq!(fresh.run("DBSIZE"), Frame::Integer(2_004));
        fresh.run("SELECT 2");
        assert_eq!(fresh.run("GET other-db"), bulk("here"));

        // The live server kept its own, newer state.
        assert_eq!(h.run("GET key:0"), bulk("changed"));
        assert_eq!(h.run("EXISTS doomed"), Frame::Integer(0));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn bgsave_of_an_empty_server_and_with_functions() {
        let dir = temp_dir("bgsave-empty");
        let mut h = harness_in(&dir);
        h.run("BGSAVE");
        finish_bgsave(&mut h);
        let (_, loaded) = reload(&h);
        assert_eq!(loaded, Loaded::default());

        h.cmd(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=lib\nredis.register_function('one', function() return 1 end)",
        ]);
        h.run("SET k v");
        assert_eq!(
            h.run("BGSAVE SCHEDULE"),
            Frame::simple("Background saving started")
        );
        finish_bgsave(&mut h);
        let (mut fresh, loaded) = reload(&h);
        assert_eq!((loaded.keys, loaded.libraries), (1, 1));
        assert_eq!(fresh.run("FCALL one 0"), Frame::Integer(1));
        assert_eq!(h.run("BGSAVE NOW"), err("ERR syntax error"));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_failed_bgsave_is_reported_and_leaves_no_file() {
        let mut h = harness_in("this/directory/does/not/exist");
        h.run("SET k v");
        assert!(matches!(h.run("BGSAVE"), Frame::Error(text) if text.starts_with("ERR ")));
        assert!(h.server.bgsave.is_none());
        assert_eq!(h.server.dirty, 1);
    }

    #[test]
    fn shutdown_during_a_bgsave_saves_everything_itself() {
        let dir = temp_dir("bgsave-shutdown");
        let mut h = harness_in(&dir);
        for n in 0..500 {
            h.cmd(&["SET", &format!("key:{n}"), "v"]);
        }
        h.run("BGSAVE");
        h.run("SET late arrival");
        assert_eq!(h.run("SHUTDOWN"), Frame::ok());
        assert!(h.server.bgsave.is_none());
        // The synchronous save includes what the background save would not.
        let (mut fresh, loaded) = reload(&h);
        assert_eq!(loaded.keys, 501);
        assert_eq!(fresh.run("GET late"), bulk("arrival"));
        // The abandoned background file is cleaned up by its writer.
        for _ in 0..1_000 {
            let leftovers = fs::read_dir(&dir).unwrap().count();
            if leftovers == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let files: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(files, ["dump.rdb"]);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn leftovers_of_a_killed_save_are_removed_at_start_up() {
        let dir = temp_dir("stale");
        let h = harness_in(&dir);
        let aof_dir = Path::new(&dir).join(&h.server.aof_settings.dirname);
        fs::create_dir_all(&aof_dir).unwrap();
        let ours = [
            "temp-1234.rdb",
            "temp-bg-1234.rdb.0",
            "temp-bg-99.rdb.17",
            "appendonlydir/temp-rewrite-1234.rdb.3",
        ];
        let not_ours = [
            "dump.rdb",
            "temp-notes.txt",
            "temp-bg-1234.rdb.bak",
            "temp-x.rdb",
            "my-temp-1.rdb",
            "appendonlydir/appendonly.aof.1.incr.aof",
        ];
        for name in ours.iter().chain(&not_ours) {
            fs::write(Path::new(&dir).join(name), b"x").unwrap();
        }
        // A directory is never touched, whatever it is called.
        fs::create_dir(Path::new(&dir).join("temp-5.rdb.1")).unwrap();

        let mut removed: Vec<String> = remove_stale_temporaries(&h.server)
            .iter()
            .map(|path| path.file_name().unwrap().to_str().unwrap().to_owned())
            .collect();
        removed.sort();
        assert_eq!(
            removed,
            [
                "temp-1234.rdb",
                "temp-bg-1234.rdb.0",
                "temp-bg-99.rdb.17",
                "temp-rewrite-1234.rdb.3"
            ]
        );
        for name in not_ours {
            assert!(Path::new(&dir).join(name).exists(), "{name}");
        }
        assert!(Path::new(&dir).join("temp-5.rdb.1").is_dir());
        assert!(remove_stale_temporaries(&h.server).is_empty());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn replacing_a_file_keeps_exactly_one() {
        let dir = temp_dir("replace-file");
        let (old, new) = (Path::new(&dir).join("dump.rdb"), Path::new(&dir).join("t"));
        fs::write(&old, b"old").unwrap();
        fs::write(&new, b"new").unwrap();
        replace_file(&new, &old).unwrap();
        assert_eq!(fs::read(&old).unwrap(), b"new");
        assert!(!new.exists());
        // Nothing to replace with: the file in place stays as it is.
        assert!(replace_file(&new, &old).is_err());
        assert_eq!(fs::read(&old).unwrap(), b"new");
        fs::remove_dir_all(dir).unwrap();
    }

    /// Waits for abandoned saves to remove their files, then lists `dir`.
    fn files_in(dir: &str) -> Vec<String> {
        let list = || -> Vec<String> {
            let mut names: Vec<String> = fs::read_dir(dir)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .collect();
            names.sort();
            names
        };
        for _ in 0..1_000 {
            if list().iter().all(|name| !name.starts_with("temp-")) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        list()
    }

    #[test]
    fn a_flush_abandons_the_save_and_a_new_one_follows() {
        let dir = temp_dir("bgsave-flush");
        let mut h = harness_in(&dir);
        for n in 0..5_000 {
            h.cmd(&["SET", &format!("key:{n}"), "v"]);
        }
        h.run("SELECT 1");
        h.run("SET other v");
        h.run("SELECT 0");
        h.run("BGSAVE");
        assert!(h.server.bgsave.is_some());

        // Emptying a database does not make the save take everything in
        // it first: the save is dropped ...
        assert_eq!(h.run("FLUSHDB"), Frame::ok());
        assert!(h.server.bgsave.is_none());
        assert!(h.server.bgsave_scheduled);
        assert!(!h.keyspace.db(0).is_snapshotting());
        h.run("SET after flush");
        // ... and starts again at the next opportunity, with what is there
        // then.
        autosave(&mut h.server, &mut h.keyspace, h.now);
        assert!(h.server.bgsave.is_some());
        assert!(!h.server.bgsave_scheduled);
        finish_bgsave(&mut h);
        assert!(h.server.last_save_ok);
        let (mut fresh, loaded) = reload(&h);
        assert_eq!(loaded.keys, 2);
        assert_eq!(fresh.run("GET after"), bulk("flush"));
        fresh.run("SELECT 1");
        assert_eq!(fresh.run("GET other"), bulk("v"));

        // The same for every database at once, in the background or not.
        for flush in ["FLUSHALL", "FLUSHALL ASYNC", "FLUSHDB ASYNC"] {
            for n in 0..1_000 {
                h.cmd(&["SET", &format!("key:{n}"), "v"]);
            }
            h.run("BGSAVE");
            assert_eq!(h.run(flush), Frame::ok(), "{flush}");
            assert!(h.server.bgsave.is_none(), "{flush}");
            assert!(h.server.bgsave_scheduled, "{flush}");
            // One save at a time still holds for the one that follows.
            autosave(&mut h.server, &mut h.keyspace, h.now);
            assert_eq!(
                h.run("BGSAVE"),
                err("ERR Background save already in progress")
            );
            finish_bgsave(&mut h);
            let (mut fresh, _) = reload(&h);
            assert_eq!(fresh.run("DBSIZE"), Frame::Integer(0), "{flush}");
        }
        // Without a save running, a flush schedules nothing.
        h.run("FLUSHALL");
        assert!(!h.server.bgsave_scheduled);
        // The abandoned saves cleaned up after themselves.
        assert_eq!(files_in(&dir), ["dump.rdb"]);
        fs::remove_dir_all(dir).unwrap();
    }

    /// What a test playing the writer sees of a save.
    struct Stalled {
        queue: Arc<Queue>,
        rx: Receiver<Job>,
        /// How often the writer has asked for the execution thread.
        wakes: Arc<AtomicU64>,
        /// Kept so the save does not see its writer as gone.
        _done: Sender<io::Result<()>>,
    }

    /// Starts a save whose writer is the test itself: nothing leaves the
    /// queue until the test takes it.
    fn stalled_save(h: &mut Harness) -> Stalled {
        let wakes = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&wakes);
        let queue = Arc::new(Queue::new(Some(Arc::new(move || {
            counter.fetch_add(1, Ordering::Relaxed);
        }))));
        let (tx, rx) = mpsc::channel();
        let (done_tx, done) = mpsc::channel();
        let feed = Feed::new(tx, Arc::clone(&queue));
        h.keyspace.begin_snapshot(h.now, |index| {
            let feed = feed.clone();
            Box::new(move |key, value, expire_at, now| {
                feed.entry(index, key, value, expire_at, now)
            })
        });
        h.server.bgsave = Some(BackgroundSave {
            staged: Arc::clone(&feed.staged),
            feed: Some(feed),
            queue: Arc::clone(&queue),
            done,
            scanning: true,
            dirty_at_start: 0,
            for_aof: false,
        });
        Stalled {
            queue,
            rx,
            wakes,
            _done: done_tx,
        }
    }

    #[test]
    fn the_walk_waits_for_the_writer_but_commands_never_do() {
        const KEYS: usize = 256;
        const VALUE_BYTES: usize = 256 * 1024;
        let mut h = Harness::new();
        let value = "x".repeat(VALUE_BYTES);
        for n in 0..KEYS {
            h.cmd(&["SET", &format!("key:{n}"), &value]);
        }
        let save = stalled_save(&mut h);

        // The walk goes on while there is room, then reports that there is
        // nothing to do for now, however often it is asked.
        let mut slices = 0;
        while bgsave_step(&mut h.server, &mut h.keyspace, h.now) {
            slices += 1;
            assert!(slices < 10_000, "the walk never paused");
        }
        for _ in 0..100 {
            assert!(!bgsave_step(&mut h.server, &mut h.keyspace, h.now));
        }
        let queued = lock(&save.queue.state).bytes;
        assert!(queued >= QUEUE_BYTES, "{queued}");
        assert!(queued < QUEUE_BYTES + 8 * 1024 * 1024, "{queued}");
        let mut jobs: Vec<Job> = save.rx.try_iter().collect();
        assert!(jobs.len() < KEYS / 2, "{} entries queued", jobs.len());
        // What is queued is the stored string itself, not a copy of it.
        for job in &jobs {
            let Job::Entries {
                body: Body::String { key, value, .. },
                keys: 1,
                ..
            } = job
            else {
                panic!("not a long string")
            };
            let Some(Value::String(live)) = h.keyspace.db(0).peek(key, h.now) else {
                panic!("not a string")
            };
            assert_eq!(value.as_ptr(), live.as_ptr());
        }

        // Commands that change keys the walk has not reached are not held
        // up by the full queue: the old values join it at once.
        for n in 0..KEYS {
            assert_eq!(h.cmd(&["SET", &format!("key:{n}"), "changed"]), Frame::ok());
        }
        jobs.extend(save.rx.try_iter());
        assert_eq!(jobs.len(), KEYS);
        let mut seen = std::collections::HashSet::new();
        for job in &jobs {
            let Job::Entries {
                body: Body::String { key, value, .. },
                cost,
                ..
            } = job
            else {
                panic!("not a long string")
            };
            assert!(seen.insert(key.clone()), "saved twice");
            assert_eq!(value.len(), VALUE_BYTES);
            assert!(*cost > VALUE_BYTES);
        }

        // The writer says nothing while the queue is still long, and speaks
        // up once, when it has worked it down to half the limit.
        assert_eq!(save.wakes.load(Ordering::Relaxed), 0);
        let mut woken_at = None;
        for job in jobs {
            if let Job::Entries { cost, .. } = job {
                save.queue.release(cost);
            }
            if woken_at.is_none() && save.wakes.load(Ordering::Relaxed) == 1 {
                woken_at = Some(lock(&save.queue.state).bytes);
            }
        }
        let woken_at = woken_at.expect("the writer never woke the walk");
        assert!(
            woken_at <= RESUME_BYTES && woken_at > RESUME_BYTES / 2,
            "{woken_at}"
        );
        assert_eq!(save.wakes.load(Ordering::Relaxed), 1);
        assert_eq!(lock(&save.queue.state).bytes, 0);

        // Woken, the walk finishes and commits.
        assert!(!bgsave_step(&mut h.server, &mut h.keyspace, h.now));
        assert!(matches!(save.rx.try_recv(), Ok(Job::Commit)));
        assert!(!h.keyspace.db(0).is_snapshotting());
        abort_bgsave(&mut h.server, &mut h.keyspace);
    }

    #[test]
    fn short_values_travel_encoded_and_in_batches() {
        let mut h = Harness::new();
        let value = "v".repeat(1_000);
        for n in 0..2_000 {
            h.cmd(&["SET", &format!("key:{n}"), &value]);
        }
        h.run("RPUSH list a b c");
        h.cmd(&["SET", "long", &"x".repeat(SHARED_STRING_BYTES)]);
        h.run("SELECT 3");
        h.run("SET elsewhere v");
        h.run("SELECT 0");
        let save = stalled_save(&mut h);
        // One changed before the walk gets to it.
        h.run("SET key:7 changed");
        while bgsave_step(&mut h.server, &mut h.keyspace, h.now) {}
        assert!(!h.keyspace.db(0).is_snapshotting());

        let (mut keys, mut batches, mut shared) = (0, 0, 0);
        let mut encoded = Vec::new();
        let mut databases = Vec::new();
        let mut committed = false;
        for job in save.rx.try_iter() {
            assert!(!committed, "something followed the commit");
            match job {
                Job::Entries {
                    db,
                    body,
                    keys: count,
                    cost,
                } => {
                    keys += count;
                    databases.push(db);
                    match body {
                        Body::Encoded(bytes) => {
                            batches += 1;
                            assert!(cost >= bytes.len());
                            // A batch is sent on once it reaches the size.
                            assert!(bytes.len() < BATCH_BYTES + 2_000, "{}", bytes.len());
                            encoded.extend_from_slice(&bytes);
                        }
                        Body::String { key, value, .. } => {
                            shared += 1;
                            assert_eq!(&key[..], b"long");
                            assert_eq!(value.len(), SHARED_STRING_BYTES);
                        }
                        Body::Parts(_) => panic!("nothing here is large"),
                    }
                }
                Job::Commit => committed = true,
            }
        }
        // Everything, once: 2,000 strings, the list, the long string and
        // the key in the other database.
        assert!(committed);
        assert_eq!(keys, 2_003);
        assert_eq!(shared, 1);
        // About two megabytes in pieces of about 64 KB, not 2,000 pieces.
        assert!((25..60).contains(&batches), "{batches} batches");
        assert!(databases.contains(&3));
        // The value as it was when the save began, not as it is now.
        // (A key is written as its length and then its bytes, which tells
        // "key:7" from "key:70"; the value follows after two length bytes.)
        let needle = b"key:7";
        let at = encoded
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("key:7 was not saved");
        let value_at = at + needle.len() + 2;
        assert_eq!(&encoded[value_at..value_at + 1_000], value.as_bytes());
        abort_bgsave(&mut h.server, &mut h.keyspace);
    }

    /// Four large collections, one of each kind, and a few small keys.
    fn large_collections(h: &mut Harness, elements: usize) {
        for kind in [
            "RPUSH biglist",
            "SADD bigset",
            "HSET bighash",
            "ZADD bigzset",
        ] {
            for chunk in (0..elements).collect::<Vec<_>>().chunks(500) {
                let mut command: Vec<String> = kind.split(' ').map(str::to_owned).collect();
                for n in chunk {
                    match command[0].as_str() {
                        "HSET" => command.extend([format!("field-{n}"), format!("value-{n}")]),
                        "ZADD" => command.extend([n.to_string(), format!("member-{n}")]),
                        _ => command.push(format!("element-{n}")),
                    }
                }
                h.cmd(&command.iter().map(String::as_str).collect::<Vec<_>>());
            }
        }
        h.run("SET small v");
        h.run("RPUSH smalllist a b c");
        h.run("PEXPIRE bigset 100000");
    }

    /// What the four collections hold, in an order that does not depend on
    /// how they are stored.
    fn collections(h: &mut Harness) -> Vec<String> {
        let mut out = Vec::new();
        for command in [
            "LRANGE biglist 0 -1",
            "SMEMBERS bigset",
            "HGETALL bighash",
            "ZRANGE bigzset 0 -1 WITHSCORES",
            "LRANGE smalllist 0 -1",
            "GET small",
            "PTTL bigset",
        ] {
            let mut lines: Vec<String> = match h.run(command) {
                Frame::Array(items) | Frame::Set(items) => {
                    items.iter().map(|item| format!("{item:?}")).collect()
                }
                Frame::Map(pairs) => pairs.iter().map(|pair| format!("{pair:?}")).collect(),
                other => vec![format!("{other:?}")],
            };
            if !command.starts_with("LRANGE") && !command.starts_with("ZRANGE") {
                lines.sort();
            }
            out.push(format!("{command}: {} items", lines.len()));
            out.extend(lines);
        }
        out
    }

    #[test]
    fn large_collections_are_captured_a_piece_at_a_time() {
        const ELEMENTS: usize = 20_000;
        let dir = temp_dir("bgsave-staged");
        let mut h = harness_in(&dir);
        large_collections(&mut h, ELEMENTS);
        let before = collections(&mut h);
        h.run("BGSAVE");

        // Not all in one go: each slice does a bounded amount, and part-way
        // through some of the collections are still to be saved.
        let mut slices = 0;
        let mut seen_unsaved = false;
        let mut longest = Duration::ZERO;
        loop {
            let started = Instant::now();
            let more = bgsave_step(&mut h.server, &mut h.keyspace, h.now);
            longest = longest.max(started.elapsed());
            if !more {
                // Nothing to do for the moment: done walking, or the queue
                // is full and the writer has to catch up.
                if !capturing(&h.server) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
                continue;
            }
            slices += 1;
            seen_unsaved |= ["biglist", "bigset", "bighash", "bigzset"]
                .iter()
                .any(|key| h.keyspace.db(0).unsaved(key.as_bytes()).is_some());
            assert!(slices < 1_000_000, "the save never finished walking");
        }
        assert!(seen_unsaved);
        assert!(slices >= 20, "only {slices} slices");
        // Generous: a slice aims at half a millisecond, and a whole
        // collection here takes many times that in a debug build.
        assert!(
            longest < Duration::from_millis(250),
            "a slice took {longest:?}"
        );
        finish_bgsave(&mut h);
        assert!(h.server.last_save_ok);

        let (mut fresh, loaded) = reload(&h);
        assert_eq!(loaded.keys, 6);
        assert_eq!(collections(&mut fresh), before);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_write_to_a_large_collection_waits_for_its_capture() {
        const ELEMENTS: usize = 20_000;
        let dir = temp_dir("bgsave-held");
        let mut h = harness_in(&dir);
        large_collections(&mut h, ELEMENTS);
        let before = collections(&mut h);
        h.run("BGSAVE");

        // The client is asked to wait; nothing has been done to the hash.
        assert_eq!(
            h.run("HSET bighash field-0 changed added yes"),
            Frame::NullArray
        );
        assert_eq!(
            h.block,
            Some(BlockRequest {
                keys: vec![Bytes::from_static(b"bighash")],
                kind: BlockKind::Capture,
                deadline: None
            })
        );
        assert_eq!(h.run("HGET bighash field-0"), bulk("value-0"));
        assert_eq!(h.run("HLEN bighash"), Frame::Integer(ELEMENTS as i64));
        // So is any other write that names it, a deletion included; reads,
        // and writes to anything else, are not.
        for command in ["DEL bighash", "EXPIRE bighash 100", "RENAME bighash other"] {
            assert_eq!(h.run(command), Frame::NullArray, "{command}");
            assert!(h.block.is_some(), "{command}");
        }
        assert_eq!(h.run("SET small changed"), Frame::ok());
        assert!(h.block.is_none());
        assert_eq!(h.run("RPUSH smalllist d"), Frame::Integer(4));
        assert_eq!(h.run("HEXISTS bighash field-1"), Frame::Integer(1));

        // The hash is captured next, ahead of whatever else was waiting,
        // and in pieces.
        let mut slices = 0;
        let captured = loop {
            assert!(bgsave_step(&mut h.server, &mut h.keyspace, h.now));
            slices += 1;
            assert!(slices < 100_000, "the hash was never captured");
            let captured = take_captured(&mut h.server);
            if !captured.is_empty() {
                break captured;
            }
        };
        assert!(slices >= 2, "captured in {slices} slice");
        assert_eq!(captured, [(0, Bytes::from_static(b"bighash"))]);
        assert!(h.keyspace.db(0).unsaved(b"bighash").is_none());
        // The others have not had their turn yet.
        assert!(h.keyspace.db(0).unsaved(b"biglist").is_some());

        // Now the command goes through.
        assert_eq!(
            h.run("HSET bighash field-0 changed added yes"),
            Frame::Integer(1)
        );
        assert!(h.block.is_none());
        assert_eq!(h.run("DEL bigzset"), Frame::NullArray);
        finish_bgsave(&mut h);
        // With the save over nothing is held back any more.
        assert_eq!(h.run("DEL bigzset"), Frame::Integer(1));

        let (mut fresh, _) = reload(&h);
        assert_eq!(collections(&mut fresh), before);
        assert_eq!(h.run("HGET bighash added"), bulk("yes"));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn where_a_command_cannot_wait_the_collection_is_captured_at_once() {
        const ELEMENTS: usize = 20_000;
        let dir = temp_dir("bgsave-forced");
        let mut h = harness_in(&dir);
        large_collections(&mut h, ELEMENTS);
        let before = collections(&mut h);
        h.run("BGSAVE");

        // Get the hash part of the way through its capture.
        assert_eq!(h.run("HDEL bighash field-1"), Frame::NullArray);
        assert!(bgsave_step(&mut h.server, &mut h.keyspace, h.now));
        assert!(h.keyspace.db(0).unsaved(b"bighash").is_some());
        assert!(take_captured(&mut h.server).is_empty());

        // A transaction runs as one piece, so its commands cannot wait:
        // the rest of the hash is captured there and then, and a list the
        // save had not started on is captured whole.
        h.run("MULTI");
        h.run("HDEL bighash field-1 field-2");
        h.run("HSET bighash new 1");
        h.run("LPOP biglist");
        h.run("DEL bigset");
        assert_eq!(
            h.run("EXEC"),
            Frame::Array(vec![
                Frame::Integer(2),
                Frame::Integer(1),
                bulk("element-0"),
                Frame::Integer(1)
            ])
        );
        let mut captured = take_captured(&mut h.server);
        captured.sort();
        let expected: Vec<(usize, Bytes)> = ["bighash", "biglist", "bigset"]
            .iter()
            .map(|key| (0, Bytes::copy_from_slice(key.as_bytes())))
            .collect();
        assert_eq!(captured, expected);
        // So does a script.
        assert_eq!(
            h.cmd(&[
                "EVAL",
                "return redis.call('ZREM', KEYS[1], 'member-0')",
                "1",
                "bigzset"
            ]),
            Frame::Integer(1)
        );
        finish_bgsave(&mut h);
        assert!(h.server.last_save_ok);

        let (mut fresh, _) = reload(&h);
        assert_eq!(collections(&mut fresh), before);
        // The live server has the changes.
        assert_eq!(h.run("HLEN bighash"), Frame::Integer(ELEMENTS as i64 - 1));
        assert_eq!(h.run("EXISTS bigset"), Frame::Integer(0));
        assert_eq!(h.run("ZCARD bigzset"), Frame::Integer(ELEMENTS as i64 - 1));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn what_counts_as_a_large_collection() {
        let list = |count: usize, size: usize| {
            Value::List((0..count).map(|_| Bytes::from(vec![b'x'; size])).collect())
        };
        assert!(!is_large(&Value::String(Bytes::from(vec![0; 10_000_000]))));
        assert!(!is_large(&list(0, 0)));
        assert!(!is_large(&list(STAGED_MIN_ELEMENTS - 1, 3)));
        assert!(is_large(&list(STAGED_MIN_ELEMENTS, 3)));
        // Few elements, many bytes.
        assert!(is_large(&list(10, STAGED_MIN_BYTES / 10 + 1)));
        assert!(!is_large(&list(10, 1_000)));
    }

    #[test]
    fn a_slowed_writer_delays_the_save_but_not_commands() {
        let dir = temp_dir("bgsave-slow");
        let mut h = harness_in(&dir);
        for n in 0..300 {
            h.cmd(&["SET", &format!("key:{n}"), "v"]);
        }
        assert_eq!(h.run("CONFIG SET rdb-key-save-delay 1000"), Frame::ok());
        assert_eq!(
            h.run("CONFIG GET rdb-key-save-delay"),
            Frame::Map(vec![(bulk("rdb-key-save-delay"), bulk("1000"))])
        );
        let started = Instant::now();
        h.run("BGSAVE");
        // The walk is over at once; commands run while the writer crawls.
        while bgsave_step(&mut h.server, &mut h.keyspace, h.now) {}
        for n in 0..300 {
            assert_eq!(h.cmd(&["SET", &format!("key:{n}"), "changed"]), Frame::ok());
        }
        let commands_done = started.elapsed();
        finish_bgsave(&mut h);
        let save_done = started.elapsed();
        assert!(h.server.last_save_ok);
        // 300 keys at a millisecond each.
        assert!(save_done >= Duration::from_millis(300), "{save_done:?}");
        assert!(
            commands_done < save_done / 2,
            "{commands_done:?} of {save_done:?}"
        );
        let (mut fresh, loaded) = reload(&h);
        assert_eq!(loaded.keys, 300);
        assert_eq!(fresh.run("GET key:0"), bulk("v"));
        assert!(matches!(
            h.run("CONFIG SET rdb-key-save-delay -1"),
            Frame::Error(_)
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_failing_writer_stops_the_save() {
        let dir = temp_dir("bgsave-fail");
        let mut h = harness_in(&dir);
        for n in 0..2_000 {
            h.cmd(&["SET", &format!("key:{n}"), "v"]);
        }
        h.run("SAVE");
        let before = fs::read(snapshot_path(&h.server)).unwrap();
        h.run("SET more v");
        h.run("BGSAVE");
        // As if the disk had failed under the writer.
        h.server.bgsave.as_ref().unwrap().queue.stop();
        finish_bgsave(&mut h);
        assert!(!h.server.last_save_ok);
        assert_eq!(h.server.dirty, 1);
        assert!(!h.keyspace.db(0).is_snapshotting());
        // The previous snapshot is untouched and nothing is left behind.
        assert_eq!(fs::read(snapshot_path(&h.server)).unwrap(), before);
        assert_eq!(files_in(&dir), ["dump.rdb"]);
        // The next save works.
        h.run("BGSAVE");
        finish_bgsave(&mut h);
        assert!(h.server.last_save_ok);
        assert_eq!(reload(&h).1.keys, 2_001);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn save_points_start_a_background_save() {
        let dir = temp_dir("autosave");
        let mut h = harness_in(&dir);
        h.server.last_save = h.now / 1000;
        assert_eq!(
            h.run("CONFIG GET save"),
            Frame::Map(vec![(bulk("save"), bulk("3600 1 300 100 60 10000"))])
        );
        assert_eq!(h.cmd(&["CONFIG", "SET", "save", "10 3 100 1"]), Frame::ok());
        let tick = |h: &mut Harness| autosave(&mut h.server, &mut h.keyspace, h.now);

        // Two changes: not enough for "10 seconds, 3 changes".
        h.run("SET a 1");
        h.run("SET b 2");
        h.advance(11_000);
        tick(&mut h);
        assert!(h.server.bgsave.is_none());
        // A third change, but the clock restarts only with a save.
        h.run("SET c 3");
        tick(&mut h);
        assert!(h.server.bgsave.is_some());
        finish_bgsave(&mut h);
        assert_eq!(h.server.dirty, 0);

        // One change is enough after 100 seconds.
        h.run("SET d 4");
        h.advance(99_000);
        tick(&mut h);
        assert!(h.server.bgsave.is_none());
        h.advance(1_000);
        tick(&mut h);
        assert!(h.server.bgsave.is_some());
        finish_bgsave(&mut h);

        // Turned off.
        assert_eq!(h.cmd(&["CONFIG", "SET", "save", ""]), Frame::ok());
        h.run("SET e 5");
        h.advance(1_000_000);
        tick(&mut h);
        assert!(h.server.bgsave.is_none());
        assert_eq!(
            h.cmd(&["CONFIG", "SET", "save", "10"]),
            err("ERR CONFIG SET failed (possibly related to argument 'save') - Invalid save parameters")
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_failing_autosave_is_not_retried_at_once() {
        let mut h = harness_in("this/directory/does/not/exist");
        h.server.last_save = h.now / 1000;
        h.server.save_points = vec![(0, 1)];
        h.run("SET a 1");
        autosave(&mut h.server, &mut h.keyspace, h.now);
        assert!(!h.server.last_save_ok);
        let tries = h.server.logs.len();
        // Straight away: no new attempt.
        h.advance(1_000);
        autosave(&mut h.server, &mut h.keyspace, h.now);
        assert_eq!(h.server.logs.len(), tries);
        // After the pause: tried again.
        h.advance(5_000);
        autosave(&mut h.server, &mut h.keyspace, h.now);
        assert!(h.server.logs.len() > tries);
    }

    #[test]
    fn save_points_text() {
        assert_eq!(
            parse_save_points("900 1  300 10"),
            Some(vec![(900, 1), (300, 10)])
        );
        assert_eq!(parse_save_points(""), Some(vec![]));
        assert_eq!(parse_save_points("900"), None);
        assert_eq!(parse_save_points("900 x"), None);
        assert_eq!(parse_save_points("-1 1"), None);
        assert_eq!(format_save_points(&[(900, 1), (300, 10)]), "900 1 300 10");
        assert_eq!(format_save_points(&[]), "");
    }

    #[test]
    fn dump_and_restore() {
        let mut h = Harness::new();
        h.run("RPUSH l a b c");
        h.run("ZADD z 1 a 2 b");
        assert_eq!(h.run("DUMP nokey"), Frame::Null);
        for key in ["l", "z"] {
            let Frame::Bulk(payload) = h.cmd(&["DUMP", key]) else {
                panic!("no payload")
            };
            let args = |extra: &[&str]| {
                let mut args = vec![
                    Bytes::from_static(b"RESTORE"),
                    Bytes::from(format!("{key}2")),
                    Bytes::from_static(b"0"),
                    payload.clone(),
                ];
                args.extend(
                    extra
                        .iter()
                        .map(|arg| Bytes::copy_from_slice(arg.as_bytes())),
                );
                args
            };
            assert_eq!(h.raw(&args(&[])), Frame::ok());
            assert_eq!(
                h.raw(&args(&[])),
                err("BUSYKEY Target key name already exists.")
            );
            assert_eq!(
                h.raw(&args(&["REPLACE", "IDLETIME", "5", "FREQ", "3"])),
                Frame::ok()
            );
            assert_eq!(h.raw(&args(&["BOGUS"])), err("ERR syntax error"));
            assert_eq!(
                h.raw(&args(&["REPLACE", "FREQ", "256"])),
                err("ERR Invalid FREQ value, must be >= 0 and <= 255")
            );
        }
        assert_eq!(
            h.run("LRANGE l2 0 -1"),
            Frame::Array(bulks(&["a", "b", "c"]))
        );
        assert_eq!(h.run("ZSCORE z2 b"), Frame::Double(2.0));
        assert_eq!(h.run("TTL l2"), Frame::Integer(-1));
    }

    #[test]
    fn restore_ttl_and_bad_payloads() {
        let mut h = Harness::new();
        h.run("SET k v");
        let Frame::Bulk(payload) = h.run("DUMP k") else {
            panic!("no payload")
        };
        let restore = |h: &mut Harness, key: &str, ttl: &str, extra: &[&str]| {
            let mut args = vec![
                Bytes::from_static(b"RESTORE"),
                Bytes::copy_from_slice(key.as_bytes()),
                Bytes::copy_from_slice(ttl.as_bytes()),
                payload.clone(),
            ];
            args.extend(
                extra
                    .iter()
                    .map(|arg| Bytes::copy_from_slice(arg.as_bytes())),
            );
            h.raw(&args)
        };
        assert_eq!(restore(&mut h, "relative", "5000", &[]), Frame::ok());
        assert_eq!(h.run("PTTL relative"), Frame::Integer(5000));
        let at = (h.now + 9000).to_string();
        assert_eq!(restore(&mut h, "absolute", &at, &["ABSTTL"]), Frame::ok());
        assert_eq!(h.run("PTTL absolute"), Frame::Integer(9000));
        // An absolute time in the past: restored and gone at once.
        assert_eq!(restore(&mut h, "past", "1", &["ABSTTL"]), Frame::ok());
        assert_eq!(h.run("EXISTS past"), Frame::Integer(0));
        assert_eq!(
            restore(&mut h, "neg", "-1", &[]),
            err("ERR Invalid TTL value, must be >= 0")
        );
        assert_eq!(
            restore(&mut h, "x", "x", &[]),
            err("ERR value is not an integer or out of range")
        );

        for bad in [&b"garbage"[..], &b""[..], &payload[..payload.len() - 1]] {
            assert_eq!(
                h.raw(&[
                    Bytes::from_static(b"RESTORE"),
                    Bytes::from_static(b"bad"),
                    Bytes::from_static(b"0"),
                    Bytes::copy_from_slice(bad),
                ]),
                err("ERR DUMP payload version or checksum are wrong")
            );
        }
        assert_eq!(h.run("EXISTS bad"), Frame::Integer(0));
    }
}
