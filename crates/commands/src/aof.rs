//! The append-only file: every change is appended to a log as the command
//! that made it, and the log is replayed at start-up.
//!
//! The layout on disk is the one Redis 7 uses, so either server can read
//! the other's files:
//!
//! ```text
//! appendonlydir/
//!   appendonly.aof.manifest      which files make up the log, in order
//!   appendonly.aof.3.base.rdb    a snapshot (RDB format)
//!   appendonly.aof.3.incr.aof    the commands run since that snapshot
//! ```
//!
//! # Replay-safe logging
//!
//! Replaying the log must rebuild exactly the data that was there, whenever
//! the replay happens. So commands are not always logged as they were typed:
//!
//! * relative expiries become absolute (`EXPIRE k 10` is logged as
//!   `PEXPIREAT k <time>`), and commands with a random outcome are logged as
//!   their outcome (`SPOP` as `SREM`); handlers request this with
//!   [`Aof::log_instead`];
//! * a key deleted by its TTL is logged as a `DEL`, and during replay
//!   nothing expires by itself (the clock is held at zero), so a key exists
//!   at each point of the replay exactly when it did originally;
//! * a script is logged as the commands it ran, and those, like the commands
//!   of a `MULTI` block, are wrapped in `MULTI`/`EXEC` so that a log cut
//!   short by a crash never replays half of them.
//!
//! # Rewriting
//!
//! The log grows without bound, so now and then it is rewritten: a new
//! snapshot is saved in the background (see
//! [`crate::persistence::BackgroundSave`]) while new commands go to a new
//! incremental file opened at the same instant. When the snapshot is
//! complete the manifest switches to the new pair and the old files go.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;

use bytes::{Bytes, BytesMut};
use rexyra_engine::Keyspace;
use rexyra_resp::parse_request;

use crate::{dispatch, lookup, lossy, persistence, unwatch_all, ClientState, Ctx, Server};

/// When the log is forced to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fsync {
    /// Before each reply. Nothing acknowledged is ever lost.
    Always,
    /// About once a second. A crash can lose the last second.
    EverySecond,
    /// When the operating system decides.
    Never,
}

impl Fsync {
    pub fn parse(text: &str) -> Option<Fsync> {
        match text.to_ascii_lowercase().as_str() {
            "always" => Some(Fsync::Always),
            "everysec" => Some(Fsync::EverySecond),
            "no" => Some(Fsync::Never),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Fsync::Always => "always",
            Fsync::EverySecond => "everysec",
            Fsync::Never => "no",
        }
    }
}

/// Settings of the append-only file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AofSettings {
    pub enabled: bool,
    /// Directory, inside the data directory, that holds the files.
    pub dirname: String,
    /// The name the files' names start with.
    pub filename: String,
    pub fsync: Fsync,
    /// Rewrite when the log has grown by this percentage since the last
    /// rewrite (zero: never automatically) ...
    pub rewrite_percentage: u64,
    /// ... and is at least this many bytes.
    pub rewrite_min_size: u64,
    /// Whether a log that ends in the middle of a command is cut back to
    /// its last complete command at start-up, rather than refused.
    pub load_truncated: bool,
}

impl Default for AofSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            dirname: "appendonlydir".to_owned(),
            filename: "appendonly.aof".to_owned(),
            fsync: Fsync::EverySecond,
            rewrite_percentage: 100,
            rewrite_min_size: 64 * 1024 * 1024,
            load_truncated: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LogFile {
    name: String,
    seq: u64,
}

/// The list of files that make up the log.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Manifest {
    base: Option<LogFile>,
    /// Incremental files, oldest first.
    incrs: Vec<LogFile>,
}

impl Manifest {
    fn parse(text: &str) -> Result<Manifest, String> {
        let mut manifest = Manifest::default();
        for line in text.lines().map(str::trim) {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            // `file <name> seq <n> type <b|i|h>`, possibly with more pairs.
            let words: Vec<&str> = line.split_whitespace().collect();
            let field = |name: &str| {
                words
                    .chunks(2)
                    .find(|pair| pair[0] == name)
                    .and_then(|pair| pair.get(1).copied())
            };
            let bad = || format!("bad line in the manifest: {line}");
            let file = LogFile {
                name: field("file").ok_or_else(bad)?.to_owned(),
                seq: field("seq")
                    .and_then(|seq| seq.parse().ok())
                    .ok_or_else(bad)?,
            };
            if file.name.contains(['/', '\\']) {
                return Err(bad());
            }
            match field("type").ok_or_else(bad)? {
                "b" => manifest.base = Some(file),
                "i" => manifest.incrs.push(file),
                // History files are leftovers waiting to be deleted.
                "h" => {}
                _ => return Err(bad()),
            }
        }
        Ok(manifest)
    }

    fn render(&self) -> String {
        let mut text = String::new();
        if let Some(base) = &self.base {
            text.push_str(&format!("file {} seq {} type b\n", base.name, base.seq));
        }
        for incr in &self.incrs {
            text.push_str(&format!("file {} seq {} type i\n", incr.name, incr.seq));
        }
        text
    }
}

/// A rewrite in progress: the snapshot being saved and what it replaces.
struct Rewrite {
    base: LogFile,
    /// Files to delete once the new snapshot is in place.
    obsolete: Vec<String>,
}

/// The open append-only file.
pub struct Aof {
    dir: PathBuf,
    filename: String,
    manifest: Manifest,
    /// The incremental file being appended to.
    file: File,
    /// Encoded commands not yet written to `file`.
    buffer: Vec<u8>,
    /// Commands of the transaction or script now running, held back until
    /// it ends so they can be wrapped in `MULTI`/`EXEC`.
    batch: Vec<u8>,
    batch_commands: usize,
    batch_depth: usize,
    /// The database the last logged command ran in.
    logged_db: Option<usize>,
    /// What the running command asked to have logged in its place.
    replacement: Option<Vec<Vec<Bytes>>>,
    pub fsync: Fsync,
    unsynced: bool,
    last_sync: u64,
    /// Receives a handle of the file about once a second and syncs it, so
    /// the execution thread never waits for the disk in `everysec` mode.
    syncer: mpsc::Sender<File>,
    /// Bytes in the incremental files, and in the base snapshot.
    incr_size: u64,
    base_size: u64,
    /// The total size right after the last rewrite (or at start-up), which
    /// growth is measured against.
    size_at_rewrite: u64,
    rewrite: Option<Rewrite>,
    pub last_rewrite_ok: bool,
    pub last_write_ok: bool,
}

fn encode(out: &mut Vec<u8>, args: &[impl AsRef<[u8]>]) {
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        let arg = arg.as_ref();
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
}

/// Builds a command to log from text arguments.
pub(crate) fn command(args: &[&[u8]]) -> Vec<Bytes> {
    args.iter().map(|arg| Bytes::copy_from_slice(arg)).collect()
}

fn write_atomically(path: &Path, contents: &[u8]) -> io::Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = File::create(&temporary)?;
    file.write_all(contents)?;
    file.sync_all()?;
    drop(file);
    persistence::replace_file(&temporary, path)
}

fn file_size(path: &Path) -> u64 {
    fs::metadata(path).map_or(0, |metadata| metadata.len())
}

impl Aof {
    fn manifest_path(&self) -> PathBuf {
        self.dir.join(format!("{}.manifest", self.filename))
    }

    fn incr_name(&self, seq: u64) -> String {
        format!("{}.{seq}.incr.aof", self.filename)
    }

    fn base_name(&self, seq: u64) -> String {
        format!("{}.{seq}.base.rdb", self.filename)
    }

    fn save_manifest(&self) -> io::Result<()> {
        write_atomically(&self.manifest_path(), self.manifest.render().as_bytes())
    }

    /// Opens the log described by `manifest` for appending, creating the
    /// incremental file if there is none yet.
    fn open(dir: PathBuf, settings: &AofSettings, mut manifest: Manifest) -> io::Result<Aof> {
        fs::create_dir_all(&dir)?;
        let filename = settings.filename.clone();
        if manifest.incrs.is_empty() {
            let seq = manifest.base.as_ref().map_or(1, |base| base.seq);
            manifest.incrs.push(LogFile {
                name: format!("{filename}.{seq}.incr.aof"),
                seq,
            });
        }
        let current = manifest.incrs.last().expect("just ensured");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(&current.name))?;
        let incr_size = manifest
            .incrs
            .iter()
            .map(|incr| file_size(&dir.join(&incr.name)))
            .sum();
        let base_size = manifest
            .base
            .as_ref()
            .map_or(0, |base| file_size(&dir.join(&base.name)));

        let (syncer, to_sync) = mpsc::channel::<File>();
        std::thread::Builder::new()
            .name("aof-sync".to_owned())
            .spawn(move || {
                for file in to_sync {
                    let _ = file.sync_data();
                }
            })?;

        let aof = Aof {
            dir,
            filename,
            manifest,
            file,
            buffer: Vec::new(),
            batch: Vec::new(),
            batch_commands: 0,
            batch_depth: 0,
            logged_db: None,
            replacement: None,
            fsync: settings.fsync,
            unsynced: false,
            last_sync: 0,
            syncer,
            incr_size,
            base_size,
            size_at_rewrite: incr_size + base_size,
            rewrite: None,
            last_rewrite_ok: true,
            last_write_ok: true,
        };
        aof.save_manifest()?;
        Ok(aof)
    }

    /// Total size of the log's files, including what is still buffered.
    pub fn size(&self) -> u64 {
        self.base_size + self.incr_size + (self.buffer.len() + self.batch.len()) as u64
    }

    pub fn base_size(&self) -> u64 {
        self.base_size
    }

    pub fn is_rewriting(&self) -> bool {
        self.rewrite.is_some()
    }

    // --- logging ------------------------------------------------------------

    /// Asks for `commands` to be logged in place of the command now running
    /// (an empty list: log nothing).
    pub(crate) fn log_instead(&mut self, commands: Vec<Vec<Bytes>>) {
        self.replacement = Some(commands);
    }

    pub(crate) fn take_replacement(&mut self) -> Option<Vec<Vec<Bytes>>> {
        self.replacement.take()
    }

    /// Appends a command that ran in database `db`.
    pub(crate) fn log(&mut self, db: usize, args: &[impl AsRef<[u8]>]) {
        let out = if self.batch_depth > 0 {
            self.batch_commands += 1;
            &mut self.batch
        } else {
            &mut self.buffer
        };
        if self.logged_db != Some(db) {
            encode(out, &[&b"SELECT"[..], db.to_string().as_bytes()]);
            self.logged_db = Some(db);
        }
        encode(out, args);
    }

    /// Starts holding commands back, to be logged as one unit.
    pub(crate) fn begin_batch(&mut self) {
        self.batch_depth += 1;
    }

    /// Ends the unit started by the matching [`Aof::begin_batch`].
    pub(crate) fn end_batch(&mut self) {
        self.batch_depth = self.batch_depth.saturating_sub(1);
        if self.batch_depth > 0 {
            return;
        }
        // One command is atomic by itself; more need wrapping.
        let wrap = self.batch_commands > 1;
        if wrap {
            encode(&mut self.buffer, &[&b"MULTI"[..]]);
        }
        self.buffer.append(&mut self.batch);
        if wrap {
            encode(&mut self.buffer, &[&b"EXEC"[..]]);
        }
        self.batch_commands = 0;
    }

    /// Writes what has been logged to the file. Call before replying to the
    /// client whose command was logged.
    pub fn flush(&mut self, now: u64) {
        if self.buffer.is_empty() {
            return;
        }
        let written = self.file.write_all(&self.buffer).and_then(|()| {
            if self.fsync == Fsync::Always {
                self.last_sync = now;
                self.file.sync_data()
            } else {
                self.unsynced = true;
                Ok(())
            }
        });
        self.last_write_ok = written.is_ok();
        self.incr_size += self.buffer.len() as u64;
        self.buffer.clear();
    }

    /// Periodic work: in `everysec` mode, has the file synced if a second
    /// has passed since the last time.
    pub fn tick(&mut self, now: u64) {
        self.flush(now);
        if self.fsync == Fsync::EverySecond
            && self.unsynced
            && now.saturating_sub(self.last_sync) >= 1000
        {
            if let Ok(handle) = self.file.try_clone() {
                let _ = self.syncer.send(handle);
            }
            self.unsynced = false;
            self.last_sync = now;
        }
    }

    /// Writes and syncs everything, before the server stops.
    pub fn close(&mut self, now: u64) {
        self.flush(now);
        let _ = self.file.sync_data();
    }

    // --- rewriting ------------------------------------------------------------

    /// Whether the log has grown enough since the last rewrite to be worth
    /// rewriting.
    pub fn should_rewrite(&self, settings: &AofSettings) -> bool {
        if settings.rewrite_percentage == 0 || self.rewrite.is_some() {
            return false;
        }
        let size = self.size();
        let grown = size.saturating_sub(self.size_at_rewrite);
        size >= settings.rewrite_min_size
            && grown.saturating_mul(100)
                >= self.size_at_rewrite.max(1) * settings.rewrite_percentage
    }

    /// Switches logging to a new incremental file and returns where the new
    /// snapshot is to be saved (final path, temporary path). The caller
    /// starts the snapshot at this same instant, so the new pair of files
    /// describes the data without gap or overlap.
    pub(crate) fn begin_rewrite(&mut self, now: u64) -> io::Result<(PathBuf, PathBuf)> {
        self.flush(now);
        let _ = self.file.sync_data();
        let seq = self
            .manifest
            .incrs
            .iter()
            .chain(&self.manifest.base)
            .map(|file| file.seq)
            .max()
            .unwrap_or(0)
            + 1;
        let incr = LogFile {
            name: self.incr_name(seq),
            seq,
        };
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(&incr.name))?;
        let obsolete = self
            .manifest
            .incrs
            .iter()
            .chain(&self.manifest.base)
            .map(|file| file.name.clone())
            .collect();
        self.manifest.incrs.push(incr);
        // Until the snapshot is done the old files plus the new incremental
        // file are the log; if the rewrite fails they simply stay that way.
        if let Err(error) = self.save_manifest() {
            self.manifest.incrs.pop();
            return Err(error);
        }
        self.file = file;
        self.logged_db = None;
        let base = LogFile {
            name: self.base_name(seq),
            seq,
        };
        let target = self.dir.join(&base.name);
        let temporary = self
            .dir
            .join(format!("temp-rewrite-{}.rdb", std::process::id()));
        self.rewrite = Some(Rewrite { base, obsolete });
        Ok((target, temporary))
    }

    /// Ends a rewrite. On success the manifest switches to the new snapshot
    /// and the files it replaces are deleted.
    pub(crate) fn finish_rewrite(&mut self, succeeded: bool) {
        let Some(rewrite) = self.rewrite.take() else {
            return;
        };
        self.last_rewrite_ok = succeeded;
        if !succeeded {
            return;
        }
        let previous = self.manifest.clone();
        let current = self.manifest.incrs.last().cloned();
        self.manifest.base = Some(rewrite.base);
        self.manifest.incrs = current.into_iter().collect();
        if self.save_manifest().is_err() {
            self.manifest = previous;
            self.last_rewrite_ok = false;
            return;
        }
        for name in &rewrite.obsolete {
            let _ = fs::remove_file(self.dir.join(name));
        }
        self.base_size = self
            .manifest
            .base
            .as_ref()
            .map_or(0, |base| file_size(&self.dir.join(&base.name)));
        self.incr_size = self
            .manifest
            .incrs
            .iter()
            .map(|incr| file_size(&self.dir.join(&incr.name)))
            .sum();
        self.size_at_rewrite = self.base_size + self.incr_size;
    }
}

/// Logs what a command did, after it ran. `failed` is whether it replied
/// with an error; `waiting` whether it is a blocking command about to wait.
pub(crate) fn propagate(
    ctx: &mut Ctx<'_>,
    writes: bool,
    args: &[Bytes],
    failed: bool,
    waiting: bool,
) {
    let Some(aof) = &mut ctx.server.aof else {
        return;
    };
    let replacement = aof.take_replacement();
    // Keys this command found expired went before anything it did itself.
    log_expired(aof, ctx.keyspace);
    if failed || waiting {
        return;
    }
    match replacement {
        Some(commands) => {
            for command in &commands {
                aof.log(ctx.client.db, command);
            }
        }
        None if writes => aof.log(ctx.client.db, args),
        None => {}
    }
}

fn log_expired(aof: &mut Aof, keyspace: &mut Keyspace) {
    for (db, key) in keyspace.take_expired() {
        aof.log(db, &[&b"DEL"[..], &key[..]]);
    }
}

/// Periodic work for the append-only file: logs keys removed by active
/// expiry, writes and syncs the log, and starts a rewrite when it is due.
pub fn tick(server: &mut Server, keyspace: &mut Keyspace, now: u64) {
    let Some(aof) = &mut server.aof else {
        return;
    };
    log_expired(aof, keyspace);
    aof.tick(now);
    let due = aof.should_rewrite(&server.aof_settings);
    if (due || server.aof_rewrite_scheduled) && server.bgsave.is_none() {
        server.aof_rewrite_scheduled = false;
        if let Err(error) = start_rewrite(server, keyspace, now) {
            server
                .logs
                .push((3, format!("Can't rewrite the append-only file: {error}")));
        }
    }
}

/// Starts rewriting the log in the background.
pub fn start_rewrite(server: &mut Server, keyspace: &mut Keyspace, now: u64) -> io::Result<()> {
    if server.bgsave.is_some() {
        return Err(io::Error::other("a background save is in progress"));
    }
    let Some(aof) = &mut server.aof else {
        return Err(io::Error::other("the append-only file is not enabled"));
    };
    let (target, temporary) = aof.begin_rewrite(now)?;
    let started = persistence::start_background(server, keyspace, now, target, temporary, true);
    if started.is_err() {
        if let Some(aof) = &mut server.aof {
            aof.finish_rewrite(false);
        }
    }
    started
}

/// `BGREWRITEAOF`
pub(crate) fn bgrewriteaof(ctx: &mut Ctx<'_>, _args: &[Bytes]) -> rexyra_resp::Frame {
    use rexyra_resp::Frame;
    let Some(aof) = &ctx.server.aof else {
        return Frame::error("ERR The append-only file is not enabled");
    };
    if aof.is_rewriting() {
        return Frame::error("ERR Background append only file rewriting already in progress");
    }
    if ctx.server.bgsave.is_some() {
        // Runs as soon as the save in progress has finished.
        ctx.server.aof_rewrite_scheduled = true;
        return Frame::simple("Background append only file rewriting scheduled");
    }
    match start_rewrite(ctx.server, ctx.keyspace, ctx.now) {
        Ok(()) => Frame::simple("Background append only file rewriting started"),
        Err(error) => Frame::error(format!("ERR {error}")),
    }
}

/// Parses a size such as `64mb` or `1024`, with Redis's units: `k`, `m`
/// and `g` are powers of 1000; `kb`, `mb` and `gb` powers of 1024.
pub fn parse_size(text: &str) -> Option<u64> {
    let text = text.trim().to_ascii_lowercase();
    let digits = text.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let unit = match &text[digits.len()..] {
        "" | "b" => 1,
        "k" => 1000,
        "kb" => 1024,
        "m" => 1000 * 1000,
        "mb" => 1024 * 1024,
        "g" => 1000 * 1000 * 1000,
        "gb" => 1024 * 1024 * 1024,
        _ => return None,
    };
    digits.parse::<u64>().ok()?.checked_mul(unit)
}

fn aof_dir(server: &Server) -> PathBuf {
    Path::new(&server.dir).join(&server.aof_settings.dirname)
}

/// Turns the append-only file on while the server is running: opens a new
/// log and starts a rewrite to give it a snapshot of the current data.
pub fn enable(server: &mut Server, keyspace: &mut Keyspace, now: u64) -> io::Result<()> {
    if server.aof.is_some() {
        return Ok(());
    }
    let dir = aof_dir(server);
    // Any files left from an earlier time describe other data.
    let manifest_path = dir.join(format!("{}.manifest", server.aof_settings.filename));
    let seq = fs::read_to_string(&manifest_path)
        .ok()
        .and_then(|text| Manifest::parse(&text).ok())
        .map_or(0, |old| {
            old.incrs
                .iter()
                .chain(&old.base)
                .map(|file| file.seq)
                .max()
                .unwrap_or(0)
        });
    let fresh = Manifest {
        base: None,
        incrs: vec![LogFile {
            name: format!("{}.{}.incr.aof", server.aof_settings.filename, seq + 1),
            seq: seq + 1,
        }],
    };
    server.aof = Some(Aof::open(dir, &server.aof_settings, fresh)?);
    server.aof_settings.enabled = true;
    keyspace.track_expired(true);
    if server.bgsave.is_some() {
        server.aof_rewrite_scheduled = true;
        Ok(())
    } else {
        start_rewrite(server, keyspace, now)
    }
}

/// Turns the append-only file off. The files stay on disk.
pub fn disable(server: &mut Server, keyspace: &mut Keyspace, now: u64) {
    if let Some(mut aof) = server.aof.take() {
        if aof.is_rewriting() {
            persistence::abort_bgsave(server, keyspace);
        }
        aof.close(now);
    }
    server.aof_settings.enabled = false;
    server.aof_rewrite_scheduled = false;
    keyspace.track_expired(false);
}

// --- loading ------------------------------------------------------------------

/// What [`load`] did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct AofLoaded {
    /// Keys in the base snapshot.
    pub base_keys: usize,
    /// Commands replayed from the incremental files.
    pub commands: usize,
    /// Bytes cut from the end of the log because it stopped mid-command.
    pub truncated: u64,
    /// The log did not exist and was created (from the RDB snapshot, if
    /// there was one).
    pub created: bool,
}

/// Runs the commands in `data` against `keyspace`. Returns how many ran and
/// how many bytes they took up; anything after that is an unfinished
/// command or transaction.
fn replay(
    server: &mut Server,
    keyspace: &mut Keyspace,
    data: &[u8],
) -> Result<(usize, usize), String> {
    let mut client = ClientState::new(0, "append-only-file".to_owned());
    let mut clients = HashMap::new();
    let mut buffer = BytesMut::from(data);
    let (mut commands, mut complete) = (0, 0);
    server.loading = true;
    loop {
        let args = match parse_request(&mut buffer) {
            Ok(Some(args)) => args,
            Ok(None) => break,
            Err(error) => {
                server.loading = false;
                return Err(format!(
                    "bad format after byte {}: {error}",
                    data.len() - buffer.len()
                ));
            }
        };
        let Some(name) = args.first() else {
            continue;
        };
        if lookup(name).is_none() {
            server.loading = false;
            return Err(format!("unknown command '{}'", lossy(name)));
        }
        let mut ctx = Ctx {
            client: &mut client,
            clients: &mut clients,
            server: &mut *server,
            keyspace: &mut *keyspace,
            // Nothing expires during replay; the log says when keys went.
            now: 0,
            block: None,
        };
        // A command that failed originally was not logged, so a failure
        // here would be a difference in behaviour; there is nothing better
        // to do with it than carry on, as Redis does.
        dispatch(&mut ctx, &args);
        commands += 1;
        if client.multi.is_none() {
            complete = data.len() - buffer.len();
        }
    }
    server.loading = false;
    // A transaction the log never finished did not happen.
    client.multi = None;
    unwatch_all(&mut client, keyspace);
    Ok((commands, complete))
}

/// Loads the append-only file and opens it for appending. If there is none
/// yet, starts one from the RDB snapshot (or from nothing).
pub fn load(server: &mut Server, now: u64) -> Result<(Keyspace, AofLoaded), String> {
    let dir = aof_dir(server);
    let settings = server.aof_settings.clone();
    let manifest_path = dir.join(format!("{}.manifest", settings.filename));
    let mut loaded = AofLoaded::default();
    let io_error = |what: &str, path: &Path, error: io::Error| {
        format!("cannot {what} {}: {error}", path.display())
    };

    let (mut keyspace, manifest) = match fs::read_to_string(&manifest_path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            // No log yet. Whatever the RDB snapshot holds becomes its base.
            loaded.created = true;
            let keyspace = match persistence::load(server, now)? {
                Some((keyspace, from_rdb)) => {
                    loaded.base_keys = from_rdb.keys;
                    keyspace
                }
                None => Keyspace::new(server.databases),
            };
            (keyspace, Manifest::default())
        }
        Err(error) => return Err(io_error("read", &manifest_path, error)),
        Ok(text) => {
            let manifest = Manifest::parse(&text)
                .map_err(|error| format!("cannot load {}: {error}", manifest_path.display()))?;
            (Keyspace::new(server.databases), manifest)
        }
    };

    if !loaded.created {
        if let Some(base) = &manifest.base {
            let path = dir.join(&base.name);
            let data = fs::read(&path).map_err(|error| io_error("read", &path, error))?;
            if data.starts_with(b"REDIS") {
                // Clock at zero: keys past their TTL are kept, because the
                // commands that follow were logged with them present.
                let (from_base, counts) = persistence::load_bytes(server, &data, 0)
                    .map_err(|error| format!("cannot load {}: {error}", path.display()))?;
                keyspace = from_base;
                loaded.base_keys = counts.keys;
            } else {
                // A base written as commands (Redis with the RDB preamble
                // turned off).
                let (commands, complete) = replay(server, &mut keyspace, &data)
                    .map_err(|error| format!("cannot load {}: {error}", path.display()))?;
                if complete != data.len() {
                    return Err(format!("cannot load {}: it is incomplete", path.display()));
                }
                loaded.commands += commands;
            }
        }
        let last = manifest.incrs.len().saturating_sub(1);
        for (index, incr) in manifest.incrs.iter().enumerate() {
            let path = dir.join(&incr.name);
            let data = match fs::read(&path) {
                Ok(data) => data,
                // The newest file may not have been created yet.
                Err(error) if error.kind() == io::ErrorKind::NotFound && index == last => {
                    Vec::new()
                }
                Err(error) => return Err(io_error("read", &path, error)),
            };
            let (commands, complete) = replay(server, &mut keyspace, &data)
                .map_err(|error| format!("cannot load {}: {error}", path.display()))?;
            loaded.commands += commands;
            if complete != data.len() {
                let extra = (data.len() - complete) as u64;
                if index != last || !settings.load_truncated {
                    return Err(format!(
                        "cannot load {}: it ends in the middle of a command, {extra} bytes \
                         after the last complete one",
                        path.display()
                    ));
                }
                // The usual result of a crash while appending. Cut the file
                // back so that what is appended next follows a whole command.
                OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .and_then(|file| file.set_len(complete as u64))
                    .map_err(|error| io_error("truncate", &path, error))?;
                loaded.truncated = extra;
            }
        }
    }

    let mut manifest = manifest;
    if loaded.created && (loaded.base_keys > 0 || server.scripting.is_some()) {
        // Give the new log a base holding what was loaded from the RDB file.
        fs::create_dir_all(&dir).map_err(|error| io_error("create", &dir, error))?;
        let base = LogFile {
            name: format!("{}.1.base.rdb", settings.filename),
            seq: 1,
        };
        let path = dir.join(&base.name);
        persistence::write_snapshot(server, &mut keyspace, now, &path)
            .map_err(|error| io_error("write", &path, error))?;
        manifest.base = Some(base);
    }
    let aof = Aof::open(dir.clone(), &settings, manifest)
        .map_err(|error| io_error("open the append-only file in", &dir, error))?;
    server.aof = Some(aof);
    keyspace.track_expired(true);
    // Replay went through the normal command path; none of it is news.
    server.stats.reset();
    server.dirty = 0;
    server.logs.clear();
    server.last_save = now / 1000;
    Ok((keyspace, loaded))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{bulk, bulks, err, Harness};
    use rexyra_resp::Frame;

    /// A directory of its own for one test's files.
    fn temp_dir(name: &str) -> String {
        let dir = std::env::temp_dir().join(format!("rexyra-aof-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir.to_str().unwrap().to_owned()
    }

    /// Starts a server state on `dir` with the append-only file enabled,
    /// loading whatever is there.
    fn start_at(dir: &str, now: u64) -> (Harness, AofLoaded) {
        let mut h = Harness::new();
        h.now = now;
        h.server.dir = dir.to_owned();
        h.server.aof_settings.enabled = true;
        // No automatic rewrites unless a test asks for them.
        h.server.aof_settings.rewrite_percentage = 0;
        let (keyspace, loaded) = load(&mut h.server, h.now).unwrap();
        h.keyspace = keyspace;
        (h, loaded)
    }

    fn start(dir: &str) -> Harness {
        start_at(dir, Harness::new().now).0
    }

    /// Stops `h` without ceremony (everything logged is written, as it is
    /// before any reply) and starts again from the same files.
    fn restart(h: &mut Harness) -> (Harness, AofLoaded) {
        h.server.aof.as_mut().unwrap().flush(h.now);
        start_at(&h.server.dir.clone(), h.now)
    }

    /// Everything in every database, in a form that does not depend on the
    /// internal order of hashes and sets.
    fn contents(h: &mut Harness) -> Vec<String> {
        let mut out = Vec::new();
        let text = |frame: &Frame| match frame {
            Frame::Bulk(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => format!("{other:?}"),
        };
        for db in 0..16 {
            h.cmd(&["SELECT", &db.to_string()]);
            let Frame::Array(keys) = h.run("KEYS *") else {
                panic!("KEYS failed")
            };
            let mut keys: Vec<String> = keys.iter().map(text).collect();
            keys.sort();
            for key in keys {
                let kind = match h.cmd(&["TYPE", &key]) {
                    Frame::Simple(kind) => kind,
                    other => panic!("{other:?}"),
                };
                let read: &[&str] = match kind.as_str() {
                    "string" => &["GET"],
                    "list" => &["LRANGE", "0", "-1"],
                    "hash" => &["HGETALL"],
                    "set" => &["SMEMBERS"],
                    _ => &["ZRANGE", "0", "-1", "WITHSCORES"],
                };
                let mut command = vec![read[0], key.as_str()];
                command.extend_from_slice(&read[1..]);
                let mut value = match h.cmd(&command) {
                    Frame::Array(items) | Frame::Set(items) => items.iter().map(text).collect(),
                    Frame::Map(pairs) => pairs
                        .iter()
                        .map(|(field, value)| format!("{}={}", text(field), text(value)))
                        .collect(),
                    other => vec![text(&other)],
                };
                if kind == "hash" || kind == "set" {
                    value.sort();
                }
                let expiry = h.cmd(&["PEXPIRETIME", &key]);
                out.push(format!("db{db} {key} {kind} {value:?} {expiry:?}"));
            }
        }
        h.run("SELECT 0");
        out
    }

    /// The whole log as text.
    fn log_text(h: &mut Harness) -> String {
        let aof = h.server.aof.as_mut().unwrap();
        aof.flush(h.now);
        let mut text = String::new();
        for incr in &aof.manifest.incrs {
            text.push_str(&String::from_utf8_lossy(
                &fs::read(aof.dir.join(&incr.name)).unwrap(),
            ));
        }
        text.replace("\r\n", " ")
    }

    fn incr_path(h: &Harness) -> PathBuf {
        let aof = h.server.aof.as_ref().unwrap();
        aof.dir.join(&aof.manifest.incrs.last().unwrap().name)
    }

    fn finish_rewrite(h: &mut Harness) {
        for _ in 0..10_000 {
            persistence::bgsave_step(&mut h.server, &mut h.keyspace, h.now);
            if h.server.bgsave.is_none() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("the rewrite never finished");
    }

    fn files(h: &Harness) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(&h.server.aof.as_ref().unwrap().dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn every_kind_of_change_is_replayed() {
        let dir = temp_dir("replay");
        let mut h = start(&dir);
        for command in [
            "SET s hello",
            "APPEND s -world",
            "INCRBY n 41",
            "INCR n",
            "INCRBYFLOAT f 1.5",
            "SETRANGE s 0 J",
            "MSET a 1 b 2",
            "RPUSH l a b c d",
            "LPOP l",
            "LSET l 0 B",
            "LINSERT l AFTER B x",
            "RPOPLPUSH l l2",
            "HSET h f1 v1 f2 v2",
            "HDEL h f1",
            "HINCRBY h count 5",
            "SADD set a b c",
            "SREM set b",
            "SMOVE set set2 a",
            "ZADD z 1 a 2 b 3 c",
            "ZINCRBY z 10 a",
            "ZPOPMIN z",
            "ZUNIONSTORE zu 2 z z",
            "RENAME a renamed",
            "COPY b copied",
            "DEL b",
            "SETBIT bits 7 1",
            "SORT l2 ALPHA STORE sorted",
            "SELECT 3",
            "SET in-three yes",
            "LPUSH l3 x",
            "SELECT 0",
            "SET last one",
            "UNLINK nothing",
        ] {
            let reply = h.run(command);
            assert!(!matches!(reply, Frame::Error(_)), "{command}: {reply:?}");
        }
        let before = contents(&mut h);
        assert!(before.len() > 15, "{before:?}");
        let (mut fresh, loaded) = restart(&mut h);
        assert!(loaded.commands > 30);
        assert_eq!(loaded.truncated, 0);
        assert_eq!(contents(&mut fresh), before);
        // Replaying is not news.
        assert_eq!(fresh.server.dirty, 0);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn expiries_are_logged_as_absolute_times() {
        let dir = temp_dir("expiry");
        let mut h = start(&dir);
        let now = h.now;
        h.run("SET a v EX 100");
        h.run("SETEX b 100 v");
        h.run("PSETEX c 100000 v");
        h.run("SET d v");
        h.run("EXPIRE d 100");
        h.run("SET e v");
        h.run("PEXPIRE e 100000");
        h.run("SET f v EX 50");
        h.run("GETEX f EX 100");
        h.run("SET g v EX 100");
        h.run("GETEX g PERSIST");
        h.run("SET h v EX 100 KEEPTTL");
        // Conditions that do not hold change nothing and log nothing.
        h.run("EXPIRE a 500 NX");
        h.run("EXPIRE a 500 LT");
        h.run("EXPIRE nokey 500");
        h.run("SET d other NX");
        h.run("GETEX a");

        let log = log_text(&mut h);
        let at = (now + 100_000).to_string();
        assert!(
            log.contains(&format!("SET $1 a $1 v $4 PXAT $13 {at}")),
            "{log}"
        );
        assert!(log.contains(&format!("PEXPIREAT $1 d $13 {at}")), "{log}");
        assert!(log.contains("PERSIST $1 g"), "{log}");
        for word in [
            " EX ",
            "SETEX",
            "PSETEX",
            " EXPIRE ",
            " PEXPIRE ",
            "GETEX",
            " NX ",
        ] {
            assert!(!log.contains(word), "{word:?} in {log}");
        }

        // Restarted 40 seconds later: 60 seconds are left, not 100.
        h.advance(40_000);
        let (mut fresh, _) = restart(&mut h);
        for key in ["a", "b", "c", "d", "e", "f"] {
            assert_eq!(fresh.cmd(&["TTL", key]), Frame::Integer(60), "{key}");
        }
        assert_eq!(fresh.run("TTL g"), Frame::Integer(-1));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_key_is_there_during_replay_exactly_when_it_was() {
        let dir = temp_dir("timeline");
        let mut h = start(&dir);
        // While `lock` exists, SETNX fails ...
        h.run("SET lock first PX 1000");
        assert_eq!(h.run("SETNX lock second"), Frame::Integer(0));
        // ... and after it has expired, SETNX succeeds.
        h.advance(2_000);
        assert_eq!(h.run("SETNX lock third"), Frame::Integer(1));
        // An expiry in the past deletes; an absolute one too.
        h.run("SET gone v");
        h.run("EXPIRE gone -1");
        h.run("SET gone2 v");
        h.run("PEXPIREAT gone2 1");
        h.run("RPUSH q a");
        h.run("EXPIRE q 1");
        h.advance(5_000);
        h.run("RPUSH q fresh");

        let log = log_text(&mut h);
        assert!(log.contains("DEL $4 lock"), "{log}");
        assert!(log.contains("DEL $4 gone"), "{log}");
        // Replayed much later, when every TTL above is long past.
        h.advance(1_000_000);
        let (mut fresh, _) = restart(&mut h);
        assert_eq!(fresh.run("GET lock"), bulk("third"));
        assert_eq!(fresh.run("EXISTS gone gone2"), Frame::Integer(0));
        assert_eq!(fresh.run("LRANGE q 0 -1"), Frame::Array(bulks(&["fresh"])));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn keys_removed_by_the_expiry_cycle_are_logged() {
        let dir = temp_dir("active-expiry");
        let mut h = start(&dir);
        h.run("SET temp v PX 100");
        h.advance(500);
        // Nobody reads the key; the background cycle removes it.
        h.keyspace.cron(h.now, 100);
        tick(&mut h.server, &mut h.keyspace, h.now);
        assert!(log_text(&mut h).contains("DEL $4 temp"));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn random_outcomes_are_logged_as_what_happened() {
        let dir = temp_dir("random");
        let mut h = start(&dir);
        h.run("SADD s a b c d e f g h");
        h.run("SPOP s 3");
        h.run("SPOP s");
        h.run("SPOP empty");
        h.run("SADD single only");
        h.run("SPOP single");
        let log = log_text(&mut h);
        assert!(log.contains("SREM"), "{log}");
        assert!(!log.contains("SPOP"), "{log}");
        let before = contents(&mut h);
        let (mut fresh, _) = restart(&mut h);
        assert_eq!(contents(&mut fresh), before);
        assert_eq!(fresh.run("SCARD s"), Frame::Integer(4));
        assert_eq!(fresh.run("EXISTS single"), Frame::Integer(0));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn scripts_and_transactions_are_logged_as_units() {
        let dir = temp_dir("units");
        let mut h = start(&dir);
        h.cmd(&[
            "EVAL",
            "redis.call('SET', 'from-script', 1) redis.call('INCRBY', 'counter', 5) \
             return redis.call('GET', 'counter')",
            "0",
        ]);
        h.run("MULTI");
        h.run("RPUSH tx a");
        h.run("RPUSH tx b");
        h.run("GET counter");
        h.run("EXEC");
        // One write needs no wrapping; a read-only script logs nothing.
        h.cmd(&["EVAL", "return redis.call('SET', 'single', 1)", "0"]);
        h.cmd(&["EVAL", "return redis.call('GET', 'single')", "0"]);
        h.run("MULTI");
        h.run("GET single");
        h.run("EXEC");

        let log = log_text(&mut h);
        assert!(!log.contains("EVAL"), "{log}");
        assert_eq!(log.matches("*1 $5 MULTI").count(), 2, "{log}");
        assert_eq!(log.matches("*1 $4 EXEC").count(), 2, "{log}");
        assert!(!log.contains("GET"), "{log}");
        let before = contents(&mut h);
        let (mut fresh, _) = restart(&mut h);
        assert_eq!(contents(&mut fresh), before);
        assert_eq!(fresh.run("GET counter"), bulk("5"));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_log_cut_inside_a_transaction_drops_the_whole_transaction() {
        let dir = temp_dir("cut-transaction");
        let mut h = start(&dir);
        h.run("SET before 1");
        h.run("MULTI");
        h.run("SET half-a 1");
        h.run("SET half-b 1");
        h.run("EXEC");
        h.server.aof.as_mut().unwrap().flush(h.now);
        let path = incr_path(&h);
        let data = fs::read(&path).unwrap();
        // Cut just before the EXEC: the crash came mid-transaction.
        let cut = data.len() - b"*1\r\n$4\r\nEXEC\r\n".len();
        fs::write(&path, &data[..cut]).unwrap();

        let (mut fresh, loaded) = start_at(&dir, h.now);
        assert!(loaded.truncated > 0);
        assert_eq!(fresh.run("GET before"), bulk("1"));
        assert_eq!(fresh.run("EXISTS half-a half-b"), Frame::Integer(0));
        // The file was cut back, so what is appended now is well-formed.
        fresh.run("SET after 2");
        let (mut again, loaded) = restart(&mut fresh);
        assert_eq!(loaded.truncated, 0);
        assert_eq!(
            again.run("MGET before after"),
            Frame::Array(bulks(&["1", "2"]))
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_log_cut_inside_a_command_is_cut_back_or_refused() {
        let dir = temp_dir("cut-command");
        let mut h = start(&dir);
        h.run("SET a 1");
        h.run("SET b 2");
        h.server.aof.as_mut().unwrap().flush(h.now);
        let path = incr_path(&h);
        let data = fs::read(&path).unwrap();
        fs::write(&path, &data[..data.len() - 3]).unwrap();

        // With `aof-load-truncated no` the server refuses to start.
        let mut strict = Harness::new();
        strict.server.dir = dir.clone();
        strict.server.aof_settings.enabled = true;
        strict.server.aof_settings.load_truncated = false;
        let error = load(&mut strict.server, strict.now)
            .map(|_| ())
            .unwrap_err();
        assert!(error.contains("ends in the middle of a command"), "{error}");
        assert_eq!(fs::read(&path).unwrap().len(), data.len() - 3);

        let (mut fresh, loaded) = start_at(&dir, h.now);
        assert!(loaded.truncated > 0);
        assert_eq!(fresh.run("GET a"), bulk("1"));
        assert_eq!(fresh.run("EXISTS b"), Frame::Integer(0));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_damaged_log_is_refused() {
        let dir = temp_dir("damaged");
        let mut h = start(&dir);
        h.run("SET a 1");
        h.server.aof.as_mut().unwrap().flush(h.now);
        let path = incr_path(&h);
        let good = fs::read(&path).unwrap();

        for (bad, expected) in [
            (&b"*2\r\n$x\r\n"[..], "bad format"),
            (&b"*1\r\n$7\r\nNOSUCHC\r\n"[..], "unknown command 'NOSUCHC'"),
        ] {
            let mut data = good.clone();
            data.extend_from_slice(bad);
            data.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n");
            fs::write(&path, &data).unwrap();
            let mut other = Harness::new();
            other.server.dir = dir.clone();
            other.server.aof_settings.enabled = true;
            let error = load(&mut other.server, other.now).map(|_| ()).unwrap_err();
            assert!(error.contains(expected), "{error}");
        }
        // A manifest that makes no sense is refused too.
        let manifest = h.server.aof.as_ref().unwrap().manifest_path();
        fs::write(&manifest, "garbage\n").unwrap();
        let mut other = Harness::new();
        other.server.dir = dir.clone();
        other.server.aof_settings.enabled = true;
        assert!(load(&mut other.server, other.now).is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn turning_the_log_on_inside_a_transaction_loses_and_repeats_nothing() {
        let dir = temp_dir("enable-in-multi");
        let mut h = Harness::new();
        h.server.dir = dir.clone();
        h.run("SET counter 5");
        h.run("RPUSH list a");
        // The log starts in the middle of a transaction: what the
        // transaction did before that is in the snapshot the log begins
        // with, and what it does afterwards is in the log, once each.
        h.run("MULTI");
        h.run("INCR counter");
        h.run("RPUSH list b");
        h.run("CONFIG SET appendonly yes");
        h.run("INCR counter");
        h.run("RPUSH list c");
        assert!(matches!(h.run("EXEC"), Frame::Array(replies) if replies.len() == 5));
        h.run("INCR counter");
        finish_rewrite(&mut h);
        assert!(h.server.aof.as_ref().unwrap().last_rewrite_ok);

        let before = contents(&mut h);
        let (mut fresh, _) = restart(&mut h);
        assert_eq!(contents(&mut fresh), before);
        assert_eq!(fresh.run("GET counter"), bulk("8"));
        assert_eq!(
            fresh.run("LRANGE list 0 -1"),
            Frame::Array(bulks(&["a", "b", "c"]))
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rewriting_replaces_the_log_with_a_snapshot() {
        let dir = temp_dir("rewrite");
        let mut h = start(&dir);
        for n in 0..500 {
            h.cmd(&["SET", &format!("key:{n}"), "first"]);
            h.cmd(&["SET", &format!("key:{n}"), "second"]);
        }
        h.run("RPUSH list a b c");
        h.run("SET temp v EX 1000");
        h.cmd(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=lib\nredis.register_function('one', function() return 1 end)",
        ]);
        assert_eq!(
            files(&h),
            ["appendonly.aof.1.incr.aof", "appendonly.aof.manifest"]
        );
        let size_before = h.server.aof.as_mut().unwrap().size();

        assert_eq!(
            h.run("BGREWRITEAOF"),
            Frame::simple("Background append only file rewriting started")
        );
        assert_eq!(
            h.run("BGREWRITEAOF"),
            err("ERR Background append only file rewriting already in progress")
        );
        assert_eq!(
            h.run("BGSAVE"),
            err("ERR Can't BGSAVE while AOF log rewriting is in progress")
        );
        // Changes made while it runs go to the new incremental file.
        h.run("SET key:0 during");
        h.run("DEL key:1");
        h.run("RPUSH list d");
        h.run("SET new-key v");
        finish_rewrite(&mut h);
        let aof = h.server.aof.as_ref().unwrap();
        assert!(aof.last_rewrite_ok);
        assert!(!aof.is_rewriting());
        assert_eq!(
            files(&h),
            [
                "appendonly.aof.2.base.rdb",
                "appendonly.aof.2.incr.aof",
                "appendonly.aof.manifest"
            ]
        );
        assert!(h.server.aof.as_mut().unwrap().size() < size_before);

        let before = contents(&mut h);
        let (mut fresh, loaded) = restart(&mut h);
        assert_eq!(loaded.base_keys, 502);
        assert_eq!(loaded.commands, 5); // a SELECT and the four changes
        assert_eq!(contents(&mut fresh), before);
        assert_eq!(fresh.run("GET key:0"), bulk("during"));
        assert_eq!(fresh.run("EXISTS key:1"), Frame::Integer(0));
        assert_eq!(fresh.run("TTL temp"), Frame::Integer(1000));
        assert_eq!(fresh.run("FCALL one 0"), Frame::Integer(1));

        // And again, from a log that already has a base.
        fresh.run("SET round two");
        fresh.run("BGREWRITEAOF");
        finish_rewrite(&mut fresh);
        assert_eq!(
            files(&fresh),
            [
                "appendonly.aof.3.base.rdb",
                "appendonly.aof.3.incr.aof",
                "appendonly.aof.manifest"
            ]
        );
        let before = contents(&mut fresh);
        let (mut third, _) = restart(&mut fresh);
        assert_eq!(contents(&mut third), before);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_log_is_rewritten_automatically_when_it_has_grown() {
        let dir = temp_dir("auto-rewrite");
        let mut h = start(&dir);
        h.server.aof_settings.rewrite_percentage = 100;
        h.server.aof_settings.rewrite_min_size = 2_000;
        let tick_now = |h: &mut Harness| tick(&mut h.server, &mut h.keyspace, h.now);

        h.run("SET k v");
        tick_now(&mut h);
        assert!(h.server.bgsave.is_none(), "too small to bother");
        for n in 0..200 {
            h.cmd(&["SET", "k", &n.to_string()]);
        }
        tick_now(&mut h);
        assert!(h.server.aof.as_ref().unwrap().is_rewriting());
        finish_rewrite(&mut h);
        // 200 overwrites of one key shrink to one small snapshot.
        assert!(h.server.aof.as_mut().unwrap().size() < 500);
        tick_now(&mut h);
        assert!(h.server.bgsave.is_none(), "nothing has grown since");

        // A rewrite asked for during a background save waits its turn.
        h.run("SET other v");
        h.run("BGSAVE");
        assert_eq!(
            h.run("BGREWRITEAOF"),
            Frame::simple("Background append only file rewriting scheduled")
        );
        finish_rewrite(&mut h);
        tick_now(&mut h);
        assert!(h.server.aof.as_ref().unwrap().is_rewriting());
        finish_rewrite(&mut h);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn a_new_log_starts_from_the_rdb_snapshot() {
        let dir = temp_dir("from-rdb");
        // A server without the append-only file saves a snapshot ...
        let mut plain = Harness::new();
        plain.server.dir = dir.clone();
        plain.run("SET from-rdb yes");
        plain.run("RPUSH l a b");
        plain.run("SAVE");

        // ... and is then restarted with it turned on.
        let (mut h, loaded) = start_at(&dir, plain.now);
        assert!(loaded.created);
        assert_eq!(loaded.base_keys, 2);
        assert_eq!(
            files(&h),
            [
                "appendonly.aof.1.base.rdb",
                "appendonly.aof.1.incr.aof",
                "appendonly.aof.manifest"
            ]
        );
        h.run("SET from-aof yes");
        let before = contents(&mut h);

        // From now on the log is what counts, not the old snapshot.
        fs::remove_file(persistence::snapshot_path(&h.server)).unwrap();
        let (mut fresh, loaded) = restart(&mut h);
        assert!(!loaded.created);
        assert_eq!(contents(&mut fresh), before);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn turning_the_log_on_and_off_while_running() {
        let dir = temp_dir("config-set");
        let mut h = Harness::new();
        h.server.dir = dir.clone();
        h.server.aof_settings.rewrite_percentage = 0;
        h.run("SET existing data");
        assert_eq!(
            h.run("BGREWRITEAOF"),
            err("ERR The append-only file is not enabled")
        );
        assert_eq!(h.run("CONFIG SET appendonly yes"), Frame::ok());
        // The data already there is captured by an initial rewrite.
        assert!(h.server.aof.as_ref().unwrap().is_rewriting());
        h.run("SET later data");
        finish_rewrite(&mut h);
        assert_eq!(
            h.run("CONFIG GET appendonly"),
            Frame::Map(vec![(bulk("appendonly"), bulk("yes"))])
        );
        let before = contents(&mut h);
        let (mut fresh, _) = restart(&mut h);
        assert_eq!(contents(&mut fresh), before);

        assert_eq!(fresh.run("CONFIG SET appendonly no"), Frame::ok());
        assert!(fresh.server.aof.is_none());
        fresh.run("SET unlogged x");
        assert_eq!(fresh.run("CONFIG SET appendfsync always"), Frame::ok());
        assert_eq!(
            fresh.run("CONFIG GET appendfsync"),
            Frame::Map(vec![(bulk("appendfsync"), bulk("always"))])
        );
        assert_eq!(
            fresh.run("CONFIG SET auto-aof-rewrite-min-size 1mb"),
            Frame::ok()
        );
        assert_eq!(fresh.server.aof_settings.rewrite_min_size, 1024 * 1024);
        for (command, why) in [
            (
                "CONFIG SET appendonly maybe",
                "argument must be 'yes' or 'no'",
            ),
            (
                "CONFIG SET appendfsync sometimes",
                "argument(s) must be one of the following: everysec, always, no",
            ),
            ("CONFIG SET appenddirname x", "can't set immutable config"),
        ] {
            assert!(
                matches!(fresh.run(command), Frame::Error(text) if text.ends_with(why)),
                "{command}"
            );
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn function_libraries_and_flushes_are_logged() {
        let dir = temp_dir("functions");
        let mut h = start(&dir);
        let library = |name: &str, value: i64| {
            format!(
                "#!lua name={name}\nredis.register_function('{name}_fn', function() return {value} end)"
            )
        };
        h.cmd(&["FUNCTION", "LOAD", &library("a", 1)]);
        h.cmd(&["FUNCTION", "LOAD", &library("b", 2)]);
        h.cmd(&["FUNCTION", "LOAD", "REPLACE", &library("a", 10)]);
        h.run("FUNCTION DELETE b");
        // A failed load is not logged.
        h.cmd(&["FUNCTION", "LOAD", &library("a", 99)]);
        h.run("SET doomed 1");
        h.run("SELECT 4");
        h.run("SET also-doomed 1");
        h.run("FLUSHALL");
        h.run("SET survivor 1");
        h.run("SELECT 0");
        // Blocking commands that found nothing leave no trace.
        h.run("MULTI");
        h.run("BLPOP nolist 0");
        h.run("EXEC");

        let (mut fresh, _) = restart(&mut h);
        assert_eq!(fresh.run("FCALL a_fn 0"), Frame::Integer(10));
        assert_eq!(fresh.run("FCALL b_fn 0"), err("ERR Function not found"));
        assert_eq!(fresh.run("DBSIZE"), Frame::Integer(0));
        fresh.run("SELECT 4");
        assert_eq!(fresh.run("KEYS *"), Frame::Array(bulks(&["survivor"])));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn info_describes_the_log() {
        let dir = temp_dir("info");
        let mut h = start(&dir);
        h.run("SET k v");
        let Frame::Verbatim { data, .. } = h.run("INFO persistence") else {
            panic!("not text")
        };
        let info = String::from_utf8(data.to_vec()).unwrap();
        for line in [
            "aof_enabled:1",
            "aof_rewrite_in_progress:0",
            "aof_last_bgrewrite_status:ok",
            "aof_last_write_status:ok",
            "aof_base_size:0",
        ] {
            assert!(info.contains(line), "{line} not in {info}");
        }
        assert!(info.contains("aof_current_size:"), "{info}");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn manifest_round_trip() {
        let text = "file appendonly.aof.2.base.rdb seq 2 type b\n\
                    file appendonly.aof.2.incr.aof seq 2 type i\n\
                    file appendonly.aof.3.incr.aof seq 3 type i\n";
        let manifest = Manifest::parse(text).unwrap();
        assert_eq!(manifest.base.as_ref().unwrap().seq, 2);
        assert_eq!(manifest.incrs.len(), 2);
        assert_eq!(manifest.render(), text);
        assert_eq!(Manifest::parse("").unwrap(), Manifest::default());
    }

    #[test]
    fn manifest_as_redis_writes_it() {
        // History entries and extra fields are tolerated.
        let manifest = Manifest::parse(
            "file appendonly.aof.1.base.rdb seq 1 type h\n\
             file appendonly.aof.2.base.rdb seq 2 type b\n\
             file appendonly.aof.2.incr.aof seq 2 type i startoffset 0 endoffset 120\n",
        )
        .unwrap();
        assert_eq!(manifest.base.unwrap().name, "appendonly.aof.2.base.rdb");
        assert_eq!(manifest.incrs[0].name, "appendonly.aof.2.incr.aof");
    }

    #[test]
    fn bad_manifests() {
        for text in [
            "file x seq 1",
            "file x seq one type i",
            "file x seq 1 type z",
            "seq 1 type i",
            "file ../outside seq 1 type i",
            "nonsense",
        ] {
            assert!(Manifest::parse(text).is_err(), "{text}");
        }
    }

    #[test]
    fn sizes() {
        assert_eq!(parse_size("1024"), Some(1024));
        assert_eq!(parse_size("64mb"), Some(64 * 1024 * 1024));
        assert_eq!(parse_size("1GB"), Some(1 << 30));
        assert_eq!(parse_size("5k"), Some(5000));
        assert_eq!(parse_size("2m"), Some(2_000_000));
        assert_eq!(parse_size("7b"), Some(7));
        assert_eq!(parse_size("mb"), None);
        assert_eq!(parse_size("5x"), None);
        assert_eq!(parse_size("-1"), None);
    }

    #[test]
    fn fsync_names() {
        for policy in [Fsync::Always, Fsync::EverySecond, Fsync::Never] {
            assert_eq!(Fsync::parse(policy.name()), Some(policy));
        }
        assert_eq!(Fsync::parse("EVERYSEC"), Some(Fsync::EverySecond));
        assert_eq!(Fsync::parse("sometimes"), None);
    }

    #[test]
    fn commands_are_encoded_as_requests() {
        let mut out = Vec::new();
        encode(&mut out, &[&b"SET"[..], b"k", b""]);
        assert_eq!(out, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n");
    }
}
