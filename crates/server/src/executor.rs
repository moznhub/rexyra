//! The command execution thread.
//!
//! Exactly one OS thread runs commands and owns all mutable server state
//! (the keyspace, per-client state, settings and statistics). Network tasks
//! parse requests and hand them over in batches, so:
//!
//! * every command is atomic without any locking,
//! * a pipeline costs one channel round-trip, not one per command,
//! * replies for a connection are produced in request order by construction.
//!
//! # Blocking commands
//!
//! A command such as `BLPOP` that finds nothing to return does not hold up
//! this thread. Its client is *parked*: the rest of its batch, the replies
//! produced so far and the reply channel are set aside, and the thread moves
//! on to other clients. A parked client resumes when
//!
//! * another command creates one of the keys it waits for, in which case its
//!   command simply runs again,
//! * its timeout passes, in which case it receives a null reply, or
//! * another client ends the wait with `CLIENT UNBLOCK`.
//!
//! Clients waiting for the same key are served in the order they started
//! waiting.
//!
//! # Long-running scripts
//!
//! A Lua script runs on this thread, so while it runs nothing else does. If
//! a script goes on for longer than `busy-reply-threshold`, the script
//! engine starts calling [`Mailbox::poll`] between instructions. That
//! answers the commands other clients have sent with a `BUSY` error, and
//! lets `SCRIPT KILL` stop the script, as long as it has not written
//! anything.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use rexyra_commands::{
    aof, dispatch, persistence, unwatch_all, BlockKind, BlockRequest, BusyPoll, ClientState, Ctx,
    Effect, Server,
};
use rexyra_engine::{Keyspace, Value};
use rexyra_resp::{encode, Frame};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// Upper bound on keys removed per database by one maintenance tick, so a
/// burst of expirations is spread over several ticks instead of stalling
/// commands.
const MAX_EXPIRED_PER_TICK: usize = 2_000;

/// How many waiting messages are handled before their replies are sent.
/// Handling several at once lets one write of the append-only file cover
/// them all; the cap keeps the first of them from waiting long.
const MAX_MESSAGES_PER_COMMIT: usize = 128;

/// Milliseconds since the Unix epoch.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

enum Message {
    /// Periodic maintenance: active expiry and incremental rehashing.
    Tick,
    /// A background save's writer has made room or finished: have another
    /// look at the save.
    SaveProgress,
    /// A blocked client's timeout may have passed.
    Timeouts,
    Connect {
        id: u64,
        addr: String,
        laddr: String,
        kill: oneshot::Sender<()>,
    },
    Disconnect {
        id: u64,
    },
    /// The peer closed its end while a batch was in flight.
    Hangup {
        id: u64,
    },
    /// Write the snapshot if anything changed since the last save.
    SaveIfDirty {
        done: oneshot::Sender<io::Result<bool>>,
    },
    Batch {
        id: u64,
        commands: Vec<Vec<Bytes>>,
        reply: oneshot::Sender<BatchReply>,
    },
}

const BUSY: &[u8] =
    b"-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n";
const UNKILLABLE: &[u8] = b"-UNKILLABLE Sorry the script already executed write commands against \
    the dataset. You can either wait the script termination or kill the server in a hard way \
    using the SHUTDOWN NOSAVE command.\r\n";

/// The execution thread's inbox.
///
/// Normally the thread just takes the next message. While a script is over
/// its time limit, the script engine also looks in here through
/// [`BusyPoll`], answering commands itself and setting everything else
/// aside in `backlog` for when the script has finished.
struct Mailbox {
    rx: Mutex<mpsc::UnboundedReceiver<Message>>,
    backlog: Mutex<VecDeque<Message>>,
}

/// Locks a mutex that guards plain data, which stays valid even if a holder
/// panicked. Only the execution thread ever takes these locks.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Mailbox {
    /// The next message, waiting for one if need be. `None` once every
    /// [`Executor`] handle is gone.
    fn next(&self) -> Option<Message> {
        if let Some(message) = lock(&self.backlog).pop_front() {
            return Some(message);
        }
        lock(&self.rx).blocking_recv()
    }

    /// The next message if one is already waiting.
    fn try_next(&self) -> Option<Message> {
        if let Some(message) = lock(&self.backlog).pop_front() {
            return Some(message);
        }
        lock(&self.rx).try_recv().ok()
    }
}

impl BusyPoll for Mailbox {
    fn poll(&self, killable: bool) -> bool {
        let mut kill = false;
        let mut rx = lock(&self.rx);
        while let Ok(message) = rx.try_recv() {
            let Message::Batch {
                commands, reply, ..
            } = message
            else {
                // Connections coming and going, timers: after the script.
                lock(&self.backlog).push_back(message);
                continue;
            };
            let mut out = BytesMut::new();
            for args in &commands {
                let script_kill = matches!(
                    &args[..],
                    [command, sub]
                        if (command.eq_ignore_ascii_case(b"script") || command.eq_ignore_ascii_case(b"function")) && sub.eq_ignore_ascii_case(b"kill")
                );
                out.extend_from_slice(match (script_kill, killable) {
                    (true, true) => {
                        kill = true;
                        b"+OK\r\n"
                    }
                    (true, false) => UNKILLABLE,
                    (false, _) => BUSY,
                });
            }
            let _ = reply.send(BatchReply {
                data: out.freeze(),
                close: false,
            });
        }
        kill
    }
}

/// Encoded replies for one batch of commands.
pub struct BatchReply {
    pub data: Bytes,
    /// The client asked to close (`QUIT`) or was told to (`CLIENT KILL` of
    /// itself); flush `data`, then disconnect.
    pub close: bool,
}

/// The execution thread has stopped, or it abandoned this client's batch
/// because the client hung up while blocked or was killed. Either way the
/// connection is finished.
#[derive(Debug)]
pub struct ExecutorGone;

/// Cheap, cloneable handle used by connection tasks.
#[derive(Clone)]
pub struct Executor {
    // Unbounded is safe: a connection has at most one batch in flight because
    // it waits for the reply before reading more, which is the backpressure.
    tx: mpsc::UnboundedSender<Message>,
}

impl Executor {
    /// Starts the execution thread. It exits when every handle is dropped.
    ///
    /// When called from inside a Tokio runtime, blocked clients time out to
    /// the millisecond; otherwise their timeouts are only noticed on
    /// [`Executor::tick`].
    ///
    /// `keyspace` is the data to start with (empty, or loaded from a
    /// snapshot). The returned receiver fires when a client asks the server
    /// to stop with `SHUTDOWN`.
    pub fn spawn(
        mut server: Server,
        keyspace: Keyspace,
    ) -> io::Result<(Executor, oneshot::Receiver<()>)> {
        let (tx, rx) = mpsc::unbounded_channel();
        let (stop, stopped) = oneshot::channel();
        // A weak sender, so pending timers do not keep the thread alive.
        let timers = Handle::try_current().ok().map(|rt| (rt, tx.downgrade()));
        server.started_at = now_ms();
        let mailbox = Arc::new(Mailbox {
            rx: Mutex::new(rx),
            backlog: Mutex::new(VecDeque::new()),
        });
        server.busy_poll = Some(Arc::clone(&mailbox) as Arc<dyn BusyPoll>);
        // Weak, so that a save in progress does not keep the thread alive.
        let wake = tx.downgrade();
        server.save_waker = Some(Arc::new(move || {
            if let Some(tx) = wake.upgrade() {
                let _ = tx.send(Message::SaveProgress);
            }
        }));
        thread::Builder::new()
            .name("executor".to_owned())
            .spawn(move || State::new(server, keyspace, timers, stop).run(&mailbox))?;
        Ok((Executor { tx }, stopped))
    }

    /// Saves the snapshot if there are unsaved changes; returns whether it
    /// saved. Used when the process is asked to exit.
    pub async fn save_if_dirty(&self) -> io::Result<bool> {
        let (done, saved) = oneshot::channel();
        let gone = || io::Error::other("the execution thread has stopped");
        self.tx
            .send(Message::SaveIfDirty { done })
            .map_err(|_| gone())?;
        saved.await.map_err(|_| gone())?
    }

    /// Registers a new connection. The returned receiver fires when another
    /// client kills this one (`CLIENT KILL`); the connection task must then
    /// close the socket.
    pub fn connect(&self, id: u64, addr: String, laddr: String) -> oneshot::Receiver<()> {
        let (kill, killed) = oneshot::channel();
        let _ = self.tx.send(Message::Connect {
            id,
            addr,
            laddr,
            kill,
        });
        killed
    }

    pub fn disconnect(&self, id: u64) {
        let _ = self.tx.send(Message::Disconnect { id });
    }

    /// Reports that the peer closed its end of the connection while a batch
    /// is in flight. If the client is blocked, its wait is abandoned and the
    /// pending [`Executor::execute`] fails; otherwise nothing happens and the
    /// batch completes normally.
    pub fn hangup(&self, id: u64) {
        let _ = self.tx.send(Message::Hangup { id });
    }

    /// Requests one round of background maintenance.
    pub fn tick(&self) {
        let _ = self.tx.send(Message::Tick);
    }

    /// Runs `commands` in order for client `id` and returns the encoded
    /// replies. This takes as long as the commands block.
    pub async fn execute(
        &self,
        id: u64,
        commands: Vec<Vec<Bytes>>,
    ) -> Result<BatchReply, ExecutorGone> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(Message::Batch {
                id,
                commands,
                reply,
            })
            .map_err(|_| ExecutorGone)?;
        response.await.map_err(|_| ExecutorGone)
    }
}

/// One client's batch, part-way through. Dropping it without sending on
/// `reply` makes the connection task close the socket.
struct Job {
    id: u64,
    /// Commands not run yet.
    commands: VecDeque<Vec<Bytes>>,
    /// Replies to the commands run so far.
    out: BytesMut,
    reply: oneshot::Sender<BatchReply>,
}

/// A client waiting inside a blocking command.
struct Parked {
    job: Job,
    /// The blocking command, to run again when a key is ready.
    args: Vec<Bytes>,
    /// The database the keys belong to.
    db: usize,
    request: BlockRequest,
    timer: Option<JoinHandle<()>>,
}

/// What running one command led to.
enum Step {
    /// The reply was appended to the job's output.
    Done,
    /// As `Done`, and the client's connection is to be closed.
    Close,
    /// The command wants to wait; nothing was appended.
    Blocked(BlockRequest),
}

type Timers = (Handle, mpsc::WeakUnboundedSender<Message>);

struct State {
    server: Server,
    /// Every connected client, except the one whose command is running at
    /// this moment (it is passed to the command separately).
    clients: HashMap<u64, ClientState>,
    /// Closes a client's connection when sent to or dropped.
    kill_switches: HashMap<u64, oneshot::Sender<()>>,
    keyspace: Keyspace,
    /// Blocked clients by id.
    parked: HashMap<u64, Parked>,
    /// For each `(database, key)` somebody waits for, the waiting clients,
    /// longest-waiting first.
    waiters: HashMap<(usize, Bytes), VecDeque<u64>>,
    /// `(deadline, client id)` of every blocked client that has a timeout.
    deadlines: BTreeSet<(u64, u64)>,
    /// Jobs ready to continue.
    runnable: VecDeque<Job>,
    timers: Option<Timers>,
    /// Tells the accept loop to stop the server.
    stop: Option<oneshot::Sender<()>>,
    /// A background save still has keyspace to walk.
    save_has_work: bool,
    /// Replies of finished batches, held until the append-only file has
    /// been written.
    outbox: Vec<(oneshot::Sender<BatchReply>, BatchReply)>,
}

impl State {
    fn new(
        server: Server,
        keyspace: Keyspace,
        timers: Option<Timers>,
        stop: oneshot::Sender<()>,
    ) -> Self {
        Self {
            keyspace,
            stop: Some(stop),
            save_has_work: false,
            outbox: Vec::new(),
            server,
            clients: HashMap::new(),
            kill_switches: HashMap::new(),
            parked: HashMap::new(),
            waiters: HashMap::new(),
            deadlines: BTreeSet::new(),
            runnable: VecDeque::new(),
            timers,
        }
    }

    fn run(mut self, mailbox: &Mailbox) {
        loop {
            // While a background save has keyspace it could walk right now,
            // use the time between messages for that instead of sleeping.
            // When the save is waiting for its writer there is nothing to
            // do, so the thread waits for a message like at any other time;
            // the writer sends one (`SaveProgress`) when that changes.
            let message = if self.save_has_work {
                match mailbox.try_next() {
                    Some(message) => message,
                    None => {
                        self.background_save_slice();
                        continue;
                    }
                }
            } else {
                match mailbox.next() {
                    Some(message) => message,
                    None => return,
                }
            };
            self.handle(message);
            // Take whatever else has already arrived, so that one write (and
            // one sync) of the append-only file covers all of it.
            for _ in 1..MAX_MESSAGES_PER_COMMIT {
                match mailbox.try_next() {
                    Some(message) => self.handle(message),
                    None => break,
                }
            }
            self.commit();
            self.background_save_slice();
        }
    }

    /// Acts on one message, running any commands it brings.
    fn handle(&mut self, message: Message) {
        {
            match message {
                Message::Tick => {
                    let now = now_ms();
                    persistence::autosave(&mut self.server, &mut self.keyspace, now);
                    aof::tick(&mut self.server, &mut self.keyspace, now);
                    let expired = self.keyspace.cron(now, MAX_EXPIRED_PER_TICK);
                    self.server.stats.expired_keys += expired as u64;
                    self.server.stats.sample_rate(now);
                    self.expire_waits();
                }
                Message::Timeouts => self.expire_waits(),
                // The look itself happens after every message.
                Message::SaveProgress => {}
                Message::Connect {
                    id,
                    addr,
                    laddr,
                    kill,
                } => {
                    let mut client = ClientState::new(id, addr);
                    rexyra_commands::acl::start_session(&self.server, &mut client);
                    client.laddr = laddr;
                    client.connected_at = now_ms();
                    client.last_active = client.connected_at;
                    self.clients.insert(id, client);
                    self.kill_switches.insert(id, kill);
                    self.server.stats.connections += 1;
                }
                Message::Disconnect { id } => {
                    self.unpark(id);
                    self.kill_switches.remove(&id);
                    if let Some(mut client) = self.clients.remove(&id) {
                        unwatch_all(&mut client, &mut self.keyspace);
                    }
                }
                Message::Hangup { id } => {
                    // Dropping the job fails the connection's `execute`.
                    self.unpark(id);
                }
                Message::SaveIfDirty { done } => {
                    let outcome =
                        persistence::prepare_exit(&mut self.server, &mut self.keyspace, now_ms());
                    let _ = done.send(outcome);
                }
                Message::Batch {
                    id,
                    commands,
                    reply,
                } => self.runnable.push_back(Job {
                    id,
                    out: BytesMut::with_capacity(64 * commands.len()),
                    commands: commands.into(),
                    reply,
                }),
            }
            while let Some(job) = self.runnable.pop_front() {
                self.run_job(job);
            }
        }
    }

    /// Makes what the finished batches changed durable, then lets their
    /// clients know: the append-only file is written (and synced, with
    /// `appendfsync always`) before any reply leaves.
    fn commit(&mut self) {
        if let Some(aof) = &mut self.server.aof {
            aof.flush(now_ms());
        }
        for (reply, batch) in self.outbox.drain(..) {
            let _ = reply.send(batch);
        }
    }

    /// Advances a background save, if one is running, and writes out
    /// anything the server has logged.
    fn background_save_slice(&mut self) {
        self.save_has_work =
            persistence::bgsave_step(&mut self.server, &mut self.keyspace, now_ms());
        self.release_captured();
        self.write_logs();
    }

    /// Lets the clients go on that were held back from writing to a large
    /// collection until the background save had captured it.
    fn release_captured(&mut self) {
        let captured = persistence::take_captured(&mut self.server);
        let held = |parked: &Parked| parked.request.kind == BlockKind::Capture;
        if !self.parked.values().any(held) {
            return;
        }
        // In the order they arrived, for each key.
        let mut ready: Vec<u64> = Vec::new();
        let mut add = |queue: &VecDeque<u64>, parked: &HashMap<u64, Parked>| {
            for id in queue {
                if parked.get(id).is_some_and(held) && !ready.contains(id) {
                    ready.push(*id);
                }
            }
        };
        if persistence::capturing(&self.server) {
            for slot in captured {
                if let Some(queue) = self.waiters.get(&slot) {
                    add(queue, &self.parked);
                }
            }
        } else {
            // The save is over or was abandoned: nothing is left to wait
            // for.
            for queue in self.waiters.values() {
                add(queue, &self.parked);
            }
        }
        if ready.is_empty() {
            return;
        }
        for id in ready {
            let Some(mut parked) = self.unpark(id) else {
                continue;
            };
            let Some(mut client) = self.clients.remove(&id) else {
                continue;
            };
            let step = self.step(&mut client, &parked.args, &mut parked.job.out);
            self.clients.insert(id, client);
            match step {
                Step::Done => self.runnable.push_back(parked.job),
                Step::Close => self.finish(parked.job),
                // Another of its keys is still to be captured, or it is a
                // blocking command that now waits in the ordinary way.
                Step::Blocked(request) => self.park(parked.job, parked.args, request),
            }
        }
        while let Some(job) = self.runnable.pop_front() {
            self.run_job(job);
        }
        self.commit();
    }

    fn write_logs(&mut self) {
        for (level, line) in self.server.logs.drain(..) {
            match level {
                0 | 1 => debug!("{line}"),
                2 => info!("{line}"),
                _ => warn!("{line}"),
            }
        }
    }

    /// Runs a job's commands until they are finished or one blocks.
    fn run_job(&mut self, mut job: Job) {
        // Dropping `job.reply` without sending tells the connection task
        // the client is unknown, and it closes the socket.
        let Some(mut client) = self.clients.remove(&job.id) else {
            return;
        };
        while let Some(args) = job.commands.pop_front() {
            match self.step(&mut client, &args, &mut job.out) {
                Step::Done => {}
                Step::Close => break,
                Step::Blocked(request) => {
                    self.clients.insert(job.id, client);
                    return self.park(job, args, request);
                }
            }
            // Blocked clients are served before this client's next command,
            // so `LPUSH k v` followed by `LLEN k` in one pipeline sees the
            // element already handed over.
            if !self.parked.is_empty() {
                self.clients.insert(job.id, client);
                self.serve_written_keys();
                // A served client cannot disconnect this one: only messages
                // from connections do that, and none is handled meanwhile.
                let Some(back) = self.clients.remove(&job.id) else {
                    return;
                };
                client = back;
            }
        }
        self.clients.insert(job.id, client);
        self.finish(job);
    }

    /// Queues a finished batch's replies; [`State::commit`] sends them.
    fn finish(&mut self, job: Job) {
        let close = self
            .clients
            .get(&job.id)
            .is_some_and(|client| client.close_after_reply);
        self.outbox.push((
            job.reply,
            BatchReply {
                data: job.out.freeze(),
                close,
            },
        ));
    }

    /// Runs one command for `client`, which must have been taken out of
    /// `self.clients`, appending its reply to `out`.
    fn step(&mut self, client: &mut ClientState, args: &[Bytes], out: &mut BytesMut) -> Step {
        let mut ctx = Ctx {
            client: &mut *client,
            clients: &mut self.clients,
            server: &mut self.server,
            keyspace: &mut self.keyspace,
            // Sampled per command, not per batch: a long pipeline must still
            // see time pass.
            now: now_ms(),
            block: None,
        };
        let frame = dispatch(&mut ctx, args);
        let block = ctx.block;
        if !self.server.effects.is_empty() {
            self.apply_effects();
        }
        // Lines written by a script with `redis.log`.
        self.write_logs();
        if let Some(request) = block {
            return Step::Blocked(request);
        }
        // Encode with the protocol in effect *after* the command, so the
        // reply to `HELLO 3` is already RESP3.
        encode(&frame, client.proto, out);
        if client.close_after_reply {
            Step::Close
        } else {
            Step::Done
        }
    }

    /// Carries out what the last command asked to be done to other clients.
    fn apply_effects(&mut self) {
        for effect in std::mem::take(&mut self.server.effects) {
            match effect {
                Effect::Kill(id) => {
                    // The connection task closes the socket and then reports
                    // the disconnect, which removes the client's state.
                    if let Some(kill) = self.kill_switches.remove(&id) {
                        let _ = kill.send(());
                    }
                    self.unpark(id);
                }
                Effect::Shutdown => {
                    if let Some(stop) = self.stop.take() {
                        let _ = stop.send(());
                    }
                }
                Effect::Unblock { id, error } => {
                    let Some(mut parked) = self.unpark(id) else {
                        continue;
                    };
                    if let Some(client) = self.clients.get(&id) {
                        let frame = if error {
                            Frame::error("UNBLOCKED client unblocked via CLIENT UNBLOCK")
                        } else {
                            Frame::NullArray
                        };
                        encode(&frame, client.proto, &mut parked.job.out);
                        self.runnable.push_back(parked.job);
                    }
                }
            }
        }
    }

    // --- blocking -----------------------------------------------------------

    /// Sets `job` aside until one of the requested keys is ready or the
    /// deadline passes. `args` is the command that asked to wait.
    fn park(&mut self, job: Job, args: Vec<Bytes>, request: BlockRequest) {
        let id = job.id;
        let mut db = 0;
        if let Some(client) = self.clients.get_mut(&id) {
            db = client.db;
            // Held back for a moment by a background save is not "blocked"
            // as `CLIENT LIST` and `CLIENT UNBLOCK` mean it.
            client.blocked = request.kind != BlockKind::Capture;
        }
        if self.parked.is_empty() {
            self.keyspace.track_writes(true);
        }
        for key in &request.keys {
            let queue = self.waiters.entry((db, key.clone())).or_default();
            // `BLPOP k k 0` names the same key twice.
            if !queue.contains(&id) {
                queue.push_back(id);
            }
        }
        let timer = request.deadline.and_then(|deadline| {
            self.deadlines.insert((deadline, id));
            self.start_timer(deadline)
        });
        self.parked.insert(
            id,
            Parked {
                job,
                args,
                db,
                request,
                timer,
            },
        );
        self.server.blocked_clients = self.blocked_count();
    }

    /// How many clients are waiting inside a blocking command, not counting
    /// those merely held back by a background save.
    fn blocked_count(&self) -> usize {
        self.parked
            .values()
            .filter(|parked| parked.request.kind != BlockKind::Capture)
            .count()
    }

    /// Wakes this thread at `deadline`. Without a runtime the periodic tick
    /// notices the deadline instead, a little late.
    fn start_timer(&self, deadline: u64) -> Option<JoinHandle<()>> {
        let (runtime, tx) = self.timers.as_ref()?;
        let tx = tx.clone();
        let wait = Duration::from_millis(deadline.saturating_sub(now_ms()));
        Some(runtime.spawn(async move {
            tokio::time::sleep(wait).await;
            if let Some(tx) = tx.upgrade() {
                let _ = tx.send(Message::Timeouts);
            }
        }))
    }

    /// Stops client `id` from waiting and hands back its job.
    fn unpark(&mut self, id: u64) -> Option<Parked> {
        let parked = self.parked.remove(&id)?;
        for key in &parked.request.keys {
            let slot = (parked.db, key.clone());
            if let Some(queue) = self.waiters.get_mut(&slot) {
                queue.retain(|waiting| *waiting != id);
                if queue.is_empty() {
                    self.waiters.remove(&slot);
                }
            }
        }
        if let Some(deadline) = parked.request.deadline {
            self.deadlines.remove(&(deadline, id));
        }
        if let Some(timer) = &parked.timer {
            timer.abort();
        }
        if self.parked.is_empty() {
            self.keyspace.track_writes(false);
        }
        if let Some(client) = self.clients.get_mut(&id) {
            client.blocked = false;
        }
        self.server.blocked_clients = self.blocked_count();
        Some(parked)
    }

    /// Hands newly created keys to the clients waiting for them. Serving a
    /// client can itself create a key (`BLMOVE` pushes to its destination),
    /// so this repeats until nothing new was written.
    fn serve_written_keys(&mut self) {
        loop {
            let written = self.keyspace.take_written();
            if written.is_empty() {
                return;
            }
            for (db, key) in written {
                self.serve_key(db, key);
            }
        }
    }

    /// Runs the blocked commands of clients waiting for `key`, oldest first,
    /// for as long as the key holds something they can take.
    fn serve_key(&mut self, db: usize, key: Bytes) {
        let slot = (db, key);
        loop {
            let kind = match self.keyspace.db(db).get(&slot.1, now_ms()) {
                Some(Value::List(_)) => BlockKind::List,
                Some(Value::ZSet(_)) => BlockKind::ZSet,
                _ => return,
            };
            // A client waiting for another type stays blocked, as in Redis.
            let next = self.waiters.get(&slot).and_then(|queue| {
                queue.iter().copied().find(|id| {
                    self.parked
                        .get(id)
                        .is_some_and(|parked| parked.request.kind == kind)
                })
            });
            let Some(mut parked) = next.and_then(|id| self.unpark(id)) else {
                return;
            };
            let id = parked.job.id;
            let Some(mut client) = self.clients.remove(&id) else {
                continue;
            };
            let step = self.step(&mut client, &parked.args, &mut parked.job.out);
            self.clients.insert(id, client);
            match step {
                Step::Done => self.runnable.push_back(parked.job),
                Step::Close => self.finish(parked.job),
                Step::Blocked(mut request) => {
                    // Not expected, since the key was just seen to hold the
                    // right type. Keep waiting under the original deadline,
                    // and stop so this cannot loop.
                    request.deadline = parked.request.deadline;
                    self.park(parked.job, parked.args, request);
                    return;
                }
            }
        }
    }

    /// Replies null to every blocked client whose timeout has passed.
    fn expire_waits(&mut self) {
        let now = now_ms();
        while let Some(&(deadline, id)) = self.deadlines.first() {
            if deadline > now {
                break;
            }
            let Some(mut parked) = self.unpark(id) else {
                self.deadlines.pop_first();
                continue;
            };
            if let Some(client) = self.clients.get(&id) {
                encode(&Frame::NullArray, client.proto, &mut parked.job.out);
                self.runnable.push_back(parked.job);
            }
        }
    }
}
