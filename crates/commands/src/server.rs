//! Server-wide state that commands read and change: settings, statistics
//! and the slow log. Owned by the execution thread, like everything else.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use bytes::Bytes;
use rexyra_resp::Frame;

use crate::{ClientState, CommandSpec, Scripting, COMMANDS};

/// How many rate samples `instantaneous_ops_per_sec` averages over.
const RATE_SAMPLES: usize = 16;
/// At most this many arguments of a slow command are kept.
const SLOWLOG_MAX_ARGS: usize = 32;
/// Longer arguments of a slow command are cut to this many bytes.
const SLOWLOG_MAX_ARG_LEN: usize = 128;

/// Lets a long-running script give other clients a turn.
///
/// The script engine calls [`BusyPoll::poll`] over and over once a script
/// has run for longer than `busy-reply-threshold`. The implementation
/// answers the commands waiting from other clients with a `BUSY` error,
/// except `SCRIPT KILL`.
pub trait BusyPoll: Send + Sync {
    /// Returns `true` if a client asked for the script to be killed.
    /// `killable` is `false` once the script has written data; a kill
    /// request is then refused.
    fn poll(&self, killable: bool) -> bool;
}

/// Something a command asks the server to do to *another* client. The
/// execution loop carries these out after the command returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Close the client's connection.
    Kill(u64),
    /// End the client's blocking command, with a null reply or an error.
    Unblock { id: u64, error: bool },
    /// Stop the server (`SHUTDOWN`). Any save has already been done.
    Shutdown,
}

pub struct Server {
    // --- settings -----------------------------------------------------------
    pub databases: usize,
    pub bind: String,
    pub port: u16,
    /// Shared with the accept loop, so `CONFIG SET maxclients` takes effect.
    pub maxclients: Arc<AtomicUsize>,
    pub config_file: Option<String>,
    /// Directory and file name of the snapshot.
    pub dir: String,
    pub dbfilename: String,
    /// Commands slower than this many microseconds go to the slow log. Zero
    /// logs every command; a negative value turns the log off.
    pub slowlog_slower_than: i64,
    pub slowlog_max_len: usize,
    /// Milliseconds a script may run before other clients get a `BUSY`
    /// error and `SCRIPT KILL` becomes possible. Zero disables this.
    pub busy_reply_threshold: u64,
    /// The memory limit in bytes (zero: none), what to evict when it is
    /// reached, and how many keys to sample when choosing.
    pub maxmemory: u64,
    pub maxmemory_policy: crate::memory::Policy,
    pub maxmemory_samples: usize,
    pub lfu_log_factor: u32,
    pub lfu_decay_time: u32,
    /// How memory in use is measured.
    pub meter: crate::memory::MemoryMeter,
    /// Users and their permissions.
    pub acl: crate::acl::Acl,
    /// The default user's password as `requirepass` set it, which
    /// `CONFIG GET` shows; empty if there is none.
    pub requirepass: String,
    /// Refuse connections from other machines while no password is needed.
    pub protected_mode: bool,
    /// Whether that is the case right now. Shared with the accept loop.
    pub loopback_only: Arc<std::sync::atomic::AtomicBool>,
    /// Who may run `DEBUG`; fixed at start-up, as in Redis.
    pub enable_debug_command: crate::DebugCommand,
    /// The keyspace notifications to publish (`notify-keyspace-events`),
    /// as [`crate::notify`] flags. Zero, the default, publishes none.
    pub notify_flags: u32,
    /// Whether the periodic tick deletes keys whose TTL has passed. Only
    /// `DEBUG SET-ACTIVE-EXPIRE` turns it off, for tests.
    pub active_expire: bool,

    // --- identity -----------------------------------------------------------
    /// Milliseconds since the Unix epoch at start-up.
    pub started_at: u64,
    /// Forty random hexadecimal digits identifying this run.
    pub run_id: String,

    // --- runtime ------------------------------------------------------------
    pub stats: Stats,
    pub slowlog: VecDeque<SlowEntry>,
    slowlog_next_id: u64,
    /// Number of clients waiting inside a blocking command. Maintained by
    /// the execution loop.
    pub blocked_clients: usize,
    /// Write commands that succeeded since the snapshot was last saved.
    pub dirty: u64,
    /// When the snapshot was last saved or loaded, in seconds since the
    /// Unix epoch, and whether the last attempt to save worked.
    pub last_save: u64,
    pub last_save_ok: bool,
    /// The append-only file, when it is enabled, and its settings.
    pub aof: Option<crate::aof::Aof>,
    pub aof_settings: crate::aof::AofSettings,
    /// A rewrite was asked for while a background save was running.
    pub aof_rewrite_scheduled: bool,
    /// The background save in progress, if any.
    pub bgsave: Option<crate::persistence::BackgroundSave>,
    /// Microseconds the writer of a background save pauses per key, to
    /// stand in for a slow disk when testing (Redis's
    /// `rdb-key-save-delay`).
    pub rdb_key_save_delay: u64,
    /// How the writer thread of a background save gets the execution
    /// thread's attention: when it has made room for the walk to go on,
    /// and when it has finished. Without one, the save is looked at again
    /// at the next command or periodic tick.
    pub save_waker: Option<crate::persistence::SaveWaker>,
    /// A background save was interrupted and should start again.
    pub bgsave_scheduled: bool,
    /// When a background save was last started, in milliseconds.
    pub last_bgsave_try: u64,
    /// Automatic saves: `(seconds, changes)` pairs. A background save
    /// starts when at least that many changes are unsaved and that many
    /// seconds have passed since the last save.
    pub save_points: Vec<(u64, u64)>,
    /// Requests for the execution loop; see [`Effect`].
    pub effects: Vec<Effect>,
    /// Every Pub/Sub subscription; see [`crate::pubsub`].
    pub pubsub: crate::pubsub::PubSub,
    /// Messages for clients, as `(client id, message)`, which the execution
    /// loop appends to each client's output after the reply of the command
    /// that produced them.
    pub pushes: Vec<(u64, Frame)>,
    /// The Lua interpreter and script cache, started by the first script.
    pub scripting: Option<Box<Scripting>>,
    /// How the script engine reaches waiting clients; see [`BusyPoll`].
    pub busy_poll: Option<Arc<dyn BusyPoll>>,
    /// Lines scripts wrote with `redis.log`, as `(level, text)` with level
    /// 0 (debug) to 3 (warning). The execution loop writes them out.
    pub logs: Vec<(u8, String)>,
    /// Data is being read from disk: nothing is evicted or refused.
    pub loading: bool,
    /// Memory was over the limit, with nothing left to evict, when the
    /// command now running started.
    pub oom: bool,
    /// The thread that frees large values in the background, once started.
    pub lazy_free: Option<std::sync::mpsc::Sender<Box<dyn std::any::Any + Send>>>,
}

impl Default for Server {
    fn default() -> Self {
        Self {
            databases: 16,
            bind: "127.0.0.1".to_owned(),
            port: 6379,
            maxclients: Arc::new(AtomicUsize::new(10_000)),
            config_file: None,
            dir: ".".to_owned(),
            dbfilename: "dump.rdb".to_owned(),
            slowlog_slower_than: 10_000,
            slowlog_max_len: 128,
            busy_reply_threshold: 5_000,
            maxmemory: 0,
            maxmemory_policy: crate::memory::Policy::NoEviction,
            maxmemory_samples: 5,
            lfu_log_factor: 10,
            lfu_decay_time: 1,
            meter: crate::memory::MemoryMeter::Estimate,
            acl: crate::acl::Acl::default(),
            requirepass: String::new(),
            protected_mode: true,
            loopback_only: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            enable_debug_command: crate::DebugCommand::No,
            notify_flags: 0,
            active_expire: true,
            started_at: 0,
            run_id: new_run_id(),
            stats: Stats::default(),
            slowlog: VecDeque::new(),
            slowlog_next_id: 0,
            blocked_clients: 0,
            dirty: 0,
            last_save: 0,
            last_save_ok: true,
            aof: None,
            aof_settings: crate::aof::AofSettings::default(),
            aof_rewrite_scheduled: false,
            bgsave: None,
            rdb_key_save_delay: 0,
            save_waker: None,
            bgsave_scheduled: false,
            last_bgsave_try: 0,
            save_points: vec![(3600, 1), (300, 100), (60, 10_000)],
            effects: Vec::new(),
            pubsub: crate::pubsub::PubSub::default(),
            pushes: Vec::new(),
            scripting: None,
            busy_poll: None,
            logs: Vec::new(),
            loading: false,
            oom: false,
            lazy_free: None,
        }
    }
}

fn new_run_id() -> String {
    (0..5)
        .map(|_| format!("{:016x}", rexyra_engine::random_u64()))
        .collect::<String>()[..40]
        .to_owned()
}

/// One entry of the slow log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlowEntry {
    pub id: u64,
    /// Seconds since the Unix epoch when the command ran.
    pub time: u64,
    pub micros: u64,
    pub args: Vec<Bytes>,
    pub addr: String,
    pub name: Bytes,
}

/// Counters for one command.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommandStat {
    pub calls: u64,
    pub micros: u64,
    /// Refused before running (wrong number of arguments).
    pub rejected: u64,
    /// Ran and replied with an error.
    pub failed: u64,
}

#[derive(Debug, Default)]
pub struct Stats {
    pub connections: u64,
    pub commands: u64,
    pub expired_keys: u64,
    pub evicted_keys: u64,
    pub error_replies: u64,
    /// Error replies by their first word (`ERR`, `WRONGTYPE`, ...).
    pub errors_by_code: BTreeMap<String, u64>,
    /// One slot per entry of [`COMMANDS`], in the same order.
    per_command: Vec<CommandStat>,
    rate_samples: [u64; RATE_SAMPLES],
    rate_next: usize,
    rate_last: Option<(u64, u64)>,
}

impl Stats {
    fn stat_mut(&mut self, spec: &CommandSpec) -> Option<&mut CommandStat> {
        if self.per_command.is_empty() {
            self.per_command = vec![CommandStat::default(); COMMANDS.len()];
        }
        // `spec` always comes from `COMMANDS`, so its position follows from
        // its address.
        let offset = (std::ptr::from_ref(spec) as usize).checked_sub(COMMANDS.as_ptr() as usize)?;
        self.per_command
            .get_mut(offset / std::mem::size_of::<CommandSpec>())
    }

    /// Counters of every command that has been called or rejected.
    pub fn commands(&self) -> impl Iterator<Item = (&'static str, CommandStat)> + '_ {
        COMMANDS
            .iter()
            .zip(&self.per_command)
            .filter(|(_, stat)| stat.calls > 0 || stat.rejected > 0)
            .map(|(spec, stat)| (spec.name, *stat))
    }

    fn count_error(&mut self, frame: &Frame) {
        if let Frame::Error(text) = frame {
            self.error_replies += 1;
            let code = text.split(' ').next().unwrap_or_default();
            match self.errors_by_code.get_mut(code) {
                Some(count) => *count += 1,
                None => {
                    self.errors_by_code.insert(code.to_owned(), 1);
                }
            }
        }
    }

    /// Feeds the commands-per-second estimate. Call periodically.
    pub fn sample_rate(&mut self, now: u64) {
        if let Some((then, commands)) = self.rate_last {
            let elapsed = now.saturating_sub(then);
            if elapsed == 0 {
                return;
            }
            self.rate_samples[self.rate_next] = (self.commands - commands) * 1000 / elapsed;
            self.rate_next = (self.rate_next + 1) % RATE_SAMPLES;
        }
        self.rate_last = Some((now, self.commands));
    }

    /// Commands per second, averaged over the recent samples.
    pub fn ops_per_sec(&self) -> u64 {
        self.rate_samples.iter().sum::<u64>() / RATE_SAMPLES as u64
    }

    pub fn reset(&mut self) {
        *self = Stats::default();
    }
}

impl Server {
    /// Accounts for a command that ran: statistics and the slow log.
    pub(crate) fn record(
        &mut self,
        spec: &CommandSpec,
        args: &[Bytes],
        micros: u64,
        reply: &Frame,
        client: &ClientState,
        now: u64,
    ) {
        self.stats.commands += 1;
        let failed = matches!(reply, Frame::Error(_));
        if let Some(stat) = self.stats.stat_mut(spec) {
            stat.calls += 1;
            stat.micros += micros;
            stat.failed += u64::from(failed);
        }
        self.stats.count_error(reply);
        if !failed && spec.flags.contains(&"write") {
            self.dirty += 1;
        }

        let slow = u64::try_from(self.slowlog_slower_than).is_ok_and(|limit| micros >= limit);
        // Commands that carry passwords are kept out of the log.
        let secret = spec.flags.contains(&"skip_slowlog")
            || (spec.name == "config"
                && args
                    .iter()
                    .any(|arg| arg.eq_ignore_ascii_case(b"requirepass")));
        if slow && self.slowlog_max_len > 0 && !secret {
            self.log_slow(args, micros, client, now);
        }
    }

    /// Accounts for a command refused before it ran.
    pub(crate) fn record_rejected(&mut self, spec: Option<&CommandSpec>, reply: &Frame) {
        if let Some(stat) = spec.and_then(|spec| self.stats.stat_mut(spec)) {
            stat.rejected += 1;
        }
        self.stats.count_error(reply);
    }

    fn log_slow(&mut self, args: &[Bytes], micros: u64, client: &ClientState, now: u64) {
        let mut kept: Vec<Bytes> = args
            .iter()
            .take(SLOWLOG_MAX_ARGS)
            .map(|arg| {
                if arg.len() > SLOWLOG_MAX_ARG_LEN {
                    let mut cut = arg[..SLOWLOG_MAX_ARG_LEN].to_vec();
                    cut.extend_from_slice(
                        format!("... ({} more bytes)", arg.len() - SLOWLOG_MAX_ARG_LEN).as_bytes(),
                    );
                    Bytes::from(cut)
                } else {
                    Bytes::copy_from_slice(arg)
                }
            })
            .collect();
        if args.len() > SLOWLOG_MAX_ARGS {
            let more = args.len() - SLOWLOG_MAX_ARGS + 1;
            kept[SLOWLOG_MAX_ARGS - 1] = Bytes::from(format!("... ({more} more arguments)"));
        }
        self.slowlog.push_front(SlowEntry {
            id: self.slowlog_next_id,
            time: now / 1000,
            micros,
            args: kept,
            addr: client.addr.clone(),
            name: client.name.clone().unwrap_or_default(),
        });
        self.slowlog_next_id += 1;
        self.slowlog.truncate(self.slowlog_max_len);
    }

    /// Applies a new `slowlog-max-len`, dropping the oldest entries.
    pub(crate) fn set_slowlog_max_len(&mut self, len: usize) {
        self.slowlog_max_len = len;
        self.slowlog.truncate(len);
    }
}
