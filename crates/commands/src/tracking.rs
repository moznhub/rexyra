//! Client-side caching: `CLIENT TRACKING`, `CLIENT CACHING`, `CLIENT
//! GETREDIR` and `CLIENT TRACKINGINFO`, and the invalidation messages.
//!
//! A client with tracking on keeps local copies of the values it reads, and
//! the server tells it when one of them may have changed. There are two
//! modes, as in Redis:
//!
//! * **Default**: the server remembers which keys each client read, in one
//!   table from key name to client ids (with no database in it, as in
//!   Redis: a write in database 1 invalidates a key read in database 0).
//!   When a key changes, every client that read it gets an invalidation
//!   `[key]`, and the key leaves the table until it is read again.
//! * **Broadcast** (`BCAST`): the server remembers nothing per key. Each
//!   client names prefixes, and every change of a key under one of them is
//!   sent, batched: once per pass of the execution loop, every prefix sends
//!   the keys changed since, sorted.
//!
//! The keyspace notes changed keys while anyone could care (the table is
//! not empty, or some client broadcasts); see [`Note::Changed`]. Those
//! notes are read with the others in [`crate::notify::flush`], so an
//! invalidation goes out in the same place in the stream of pushed messages
//! as in Redis: before the keyspace notification of the write that caused
//! it, after the `expired` notification of an expiry. The running client's
//! own invalidations wait until after its reply and its own other messages
//! ([`after_command`]).
//!
//! A message for a client goes, depending on that client and its redirect
//! target at that moment, as a RESP3 push, as a Pub/Sub message on
//! `__redis__:invalidate` (a RESP2 redirect target that is subscribed to
//! something), as `tracking-redir-broken` (the redirect target has gone),
//! or nowhere (a RESP2 client without a redirect).
//!
//! While no client has ever turned tracking on, all this costs one test of
//! a flag per write and a test of an empty map per command.
//!
//! [`Note::Changed`]: rexyra_engine::Note::Changed

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bytes::Bytes;
use rexyra_engine::Keyspace;
use rexyra_resp::{Frame, Protocol};

use crate::{
    copied, int, is, lossy, wrong_subcommand_arity, ClientState, CommandSpec, Ctx, Server,
    ERR_SYNTAX,
};

/// The default of `tracking-table-max-keys`.
pub const DEFAULT_MAX_KEYS: u64 = 1_000_000;

/// The channel a RESP2 redirect target receives invalidations on.
const CHANNEL: &[u8] = b"__redis__:invalidate";

/// What one client asked for with `CLIENT TRACKING ON`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClientTracking {
    /// The client that receives this one's invalidations; 0 for itself.
    pub redirect: u64,
    pub bcast: bool,
    pub optin: bool,
    pub optout: bool,
    pub noloop: bool,
    /// `CLIENT CACHING YES` (with `OPTIN`) or `NO` (with `OPTOUT`) was
    /// given for the next command.
    pub caching: bool,
    /// The prefixes it broadcasts for, with `BCAST`.
    pub prefixes: BTreeSet<Bytes>,
    /// The redirect target was found gone.
    pub broken: bool,
}

/// One broadcast prefix.
#[derive(Debug, Default)]
struct Prefix {
    clients: BTreeSet<u64>,
    /// Keys under the prefix changed since the last broadcast, with the
    /// client that changed them: `None` if the server did, or several
    /// clients did (for `NOLOOP`, which leaves out a client's own changes).
    pending: BTreeMap<Bytes, Option<u64>>,
}

/// Every client's tracking state, the table of keys read and the broadcast
/// prefixes; [`Server::tracking`].
#[derive(Debug)]
pub struct Tracking {
    /// Clients with tracking on, by id.
    clients: BTreeMap<u64, ClientTracking>,
    /// Key name to the clients that read it. A client that has since
    /// turned tracking off or gone is not removed: it is skipped when the
    /// key is invalidated, as in Redis.
    table: HashMap<Bytes, BTreeSet<u64>>,
    /// Number of (key, client) pairs in `table`.
    items: usize,
    prefixes: BTreeMap<Bytes, Prefix>,
    /// Invalidations for the client whose command is running, sent when
    /// the command is over (`None`: everything, after a flush).
    deferred: Vec<Option<Bytes>>,
    /// Whether the keyspace is noting changed keys.
    recording: bool,
    /// `tracking-table-max-keys`: the most keys the table may hold before
    /// some are invalidated to make room. Zero is no limit.
    pub max_keys: u64,
    /// Consecutive ticks that found the table over the limit; each such
    /// tick does more to bring it back.
    over_limit: u64,
}

impl Default for Tracking {
    fn default() -> Self {
        Self {
            clients: BTreeMap::new(),
            table: HashMap::new(),
            items: 0,
            prefixes: BTreeMap::new(),
            deferred: Vec::new(),
            recording: false,
            max_keys: DEFAULT_MAX_KEYS,
            over_limit: 0,
        }
    }
}

impl Tracking {
    /// The tracking state of client `id`; `None` if tracking is off.
    pub fn client(&self, id: u64) -> Option<&ClientTracking> {
        if self.clients.is_empty() {
            return None;
        }
        self.clients.get(&id)
    }

    /// Clients with tracking on (`tracking_clients` in `INFO`).
    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    /// Keys in the table (`tracking_total_keys`).
    pub fn key_count(&self) -> usize {
        self.table.len()
    }

    /// (Key, client) pairs in the table (`tracking_total_items`).
    pub fn item_count(&self) -> usize {
        self.items
    }

    /// Broadcast prefixes in use (`tracking_total_prefixes`).
    pub fn prefix_count(&self) -> usize {
        self.prefixes.len()
    }

    /// Whether changes to keys matter to anyone right now.
    fn wanted(&self) -> bool {
        !self.table.is_empty() || !self.prefixes.is_empty()
    }

    /// Records `key` as changed under every prefix it starts with.
    fn note_for_broadcast(&mut self, key: &Bytes, changer: Option<u64>) {
        for (prefix, state) in self.prefixes.range_mut(..=key.clone()) {
            if !key.starts_with(prefix) || state.clients.is_empty() {
                continue;
            }
            match state.pending.get_mut(key) {
                Some(slot) => {
                    if *slot != changer {
                        *slot = None;
                    }
                }
                None => {
                    state.pending.insert(key.clone(), changer);
                }
            }
        }
    }
}

/// The clients a message may go to: every connected client but the one
/// running a command, and that one.
pub struct Peers<'a> {
    pub others: &'a HashMap<u64, ClientState>,
    pub current: Option<&'a ClientState>,
}

impl<'a> Peers<'a> {
    /// Every connected client, while no command is running.
    pub fn none(others: &'a HashMap<u64, ClientState>) -> Peers<'a> {
        Peers {
            others,
            current: None,
        }
    }

    fn get(&self, id: u64) -> Option<&'a ClientState> {
        match self.current {
            Some(client) if client.id == id => Some(client),
            _ => self.others.get(&id),
        }
    }
}

/// Turns the keyspace's recording of changed keys on when someone may
/// care. It is turned off again only by [`tick`], so that a key read and
/// written over and over does not switch it each time.
fn start_recording(server: &mut Server, keyspace: &mut Keyspace) {
    if !server.tracking.recording && server.tracking.wanted() {
        server.tracking.recording = true;
        keyspace.track_changes(true);
    }
}

/// Whether the keyspace notes changed keys, so that
/// [`crate::notify::flush`] has to run after every command.
pub fn recording(server: &Server) -> bool {
    server.tracking.recording
}

/// Sends client `id` an invalidation of `keys` (`None`: of everything),
/// wherever its settings say it goes.
fn send(server: &mut Server, peers: &Peers<'_>, id: u64, keys: Option<Vec<Bytes>>) {
    let Some(tracker) = peers.get(id) else {
        return;
    };
    let Some(state) = server.tracking.clients.get_mut(&id) else {
        return;
    };
    let redirected = state.redirect != 0;
    let target = if redirected {
        match peers.get(state.redirect) {
            Some(target) => target,
            None => {
                state.broken = true;
                if tracker.proto == Protocol::Resp3 {
                    let redirect = state.redirect;
                    server.pushes.push((
                        id,
                        Frame::Push(vec![
                            Frame::bulk("tracking-redir-broken"),
                            Frame::Integer(redirect as i64),
                        ]),
                    ));
                }
                return;
            }
        }
    } else {
        tracker
    };
    let payload = match keys {
        Some(keys) => Frame::Array(keys.into_iter().map(Frame::Bulk).collect()),
        None => Frame::Null,
    };
    let message = if target.proto == Protocol::Resp3 {
        vec![Frame::bulk("invalidate"), payload]
    } else if redirected && target.subscriptions.any() {
        // RESP2 has no pushes, but a subscribed connection takes messages.
        vec![
            Frame::bulk("message"),
            Frame::Bulk(Bytes::from_static(CHANNEL)),
            payload,
        ]
    } else {
        // A RESP2 connection that is not subscribed cannot be told.
        return;
    };
    server.pushes.push((target.id, Frame::Push(message)));
}

/// Invalidates `key`, which changed. Called for each key the keyspace
/// noted as changed, in order. `by_server`: expiry or eviction, which for
/// `NOLOOP` broadcasting counts as nobody's change.
pub(crate) fn changed(server: &mut Server, peers: &Peers<'_>, key: Bytes, by_server: bool) {
    let current = peers.current.map(|client| client.id);
    let tracking = &mut server.tracking;
    if !tracking.prefixes.is_empty() {
        tracking.note_for_broadcast(&key, if by_server { None } else { current });
    }
    invalidate(server, peers, key);
}

/// Sends an invalidation of `key` to every client that read it, and
/// forgets that they did.
fn invalidate(server: &mut Server, peers: &Peers<'_>, key: Bytes) {
    let current = peers.current.map(|client| client.id);
    let tracking = &mut server.tracking;
    let Some(ids) = tracking.table.remove(&key) else {
        return;
    };
    tracking.items -= ids.len();
    for id in ids {
        let Some(state) = server.tracking.clients.get(&id) else {
            continue;
        };
        let (bcast, noloop) = (state.bcast, state.noloop);
        // A client that has since switched to broadcasting gets nothing
        // for what it read before.
        if bcast {
            continue;
        }
        if Some(id) == current {
            // Its own change, with `NOLOOP`, is not reported. Otherwise
            // the message waits until after the reply, as in Redis.
            if !noloop {
                server.tracking.deferred.push(Some(key.clone()));
            }
            continue;
        }
        send(server, peers, id, Some(vec![key.clone()]));
    }
}

/// Remembers the keys a command read, for the client running it, when it
/// tracks them. Called after the command ran and after what it changed was
/// invalidated, so a key the command found expired is tracked again.
pub(crate) fn remember(ctx: &mut Ctx<'_>, spec: &CommandSpec, args: &[Bytes]) {
    let id = ctx.client.id;
    let Some(state) = ctx.server.tracking.client(id) else {
        return;
    };
    if state.bcast
        || (state.optin && !state.caching)
        || (state.optout && state.caching)
        || !spec.flags.contains(&"readonly")
        // A read-only script's own keys are not remembered, only what its
        // calls read, as in Redis.
        || matches!(spec.name, "eval_ro" | "evalsha_ro" | "fcall_ro")
    {
        return;
    }
    let keys = crate::acl::command_keys(spec, args);
    if keys.is_empty() {
        return;
    }
    let tracking = &mut ctx.server.tracking;
    for key in keys {
        if !tracking.table.contains_key(&key[..]) {
            tracking.table.insert(copied(key), BTreeSet::new());
        }
        let readers = tracking
            .table
            .get_mut(&key[..])
            .expect("inserted on the line above");
        if readers.insert(id) {
            tracking.items += 1;
        }
    }
    start_recording(ctx.server, ctx.keyspace);
}

/// What follows every command a client sends, whatever became of it: its
/// own invalidations are sent, and `CLIENT CACHING` stops applying unless
/// the command was a `CLIENT` subcommand or the client is in a
/// transaction (both as in Redis).
pub(crate) fn after_command(ctx: &mut Ctx<'_>, args: &[Bytes]) {
    if ctx.server.tracking.clients.is_empty() && ctx.server.tracking.deferred.is_empty() {
        return;
    }
    // Anything changed and not yet invalidated (keys evicted to make
    // room for a command that was then refused).
    if ctx.server.tracking.recording {
        let peers = Peers {
            others: ctx.clients,
            current: Some(ctx.client),
        };
        crate::notify::flush(ctx.server, ctx.keyspace, &peers);
    }
    let id = ctx.client.id;
    for keys in std::mem::take(&mut ctx.server.tracking.deferred) {
        let peers = Peers {
            others: ctx.clients,
            current: Some(ctx.client),
        };
        send(ctx.server, &peers, id, keys.map(|key| vec![key]));
    }
    let client_command = args.first().is_some_and(|name| is(name, "CLIENT"));
    if !client_command && ctx.client.multi.is_none() {
        if let Some(state) = ctx.server.tracking.clients.get_mut(&id) {
            state.caching = false;
        }
    }
}

/// `FLUSHDB` and `FLUSHALL`: every client with tracking on is told to drop
/// everything, in id order, and the table is emptied.
pub(crate) fn flushed(ctx: &mut Ctx<'_>) {
    let tracking = &mut ctx.server.tracking;
    if tracking.clients.is_empty() && tracking.table.is_empty() {
        return;
    }
    tracking.table.clear();
    tracking.items = 0;
    let ids: Vec<u64> = tracking.clients.keys().copied().collect();
    for id in ids {
        if id == ctx.client.id {
            ctx.server.tracking.deferred.push(None);
        } else {
            let peers = Peers {
                others: ctx.clients,
                current: Some(ctx.client),
            };
            send(ctx.server, &peers, id, None);
        }
    }
}

/// Sends every broadcasting client the keys under its prefixes changed
/// since the last call. The execution loop calls this once per pass, after
/// the commands it ran and before their output leaves, as Redis does
/// before it sleeps.
pub fn broadcast(server: &mut Server, peers: &Peers<'_>) {
    if server.tracking.prefixes.is_empty() {
        return;
    }
    let mut messages: Vec<(u64, Vec<Bytes>)> = Vec::new();
    let tracking = &mut server.tracking;
    for state in tracking.prefixes.values_mut() {
        if state.pending.is_empty() {
            continue;
        }
        let pending = std::mem::take(&mut state.pending);
        for &id in &state.clients {
            let noloop = tracking
                .clients
                .get(&id)
                .is_some_and(|client| client.noloop);
            let keys: Vec<Bytes> = pending
                .iter()
                .filter(|(_, changer)| !(noloop && **changer == Some(id)))
                .map(|(key, _)| key.clone())
                .collect();
            if !keys.is_empty() {
                messages.push((id, keys));
            }
        }
    }
    for (id, keys) in messages {
        send(server, peers, id, Some(keys));
    }
}

/// Periodic work: keeps the table within `tracking-table-max-keys` by
/// invalidating keys picked at random, a bounded number per call (more
/// each consecutive call that finds the table still over), and stops the
/// keyspace's recording of changes once nobody needs it.
pub fn tick(server: &mut Server, keyspace: &mut Keyspace, peers: &Peers<'_>) {
    let tracking = &mut server.tracking;
    let limit = tracking.max_keys;
    if limit == 0 || tracking.table.len() as u64 <= limit {
        tracking.over_limit = 0;
    } else {
        let budget = 100 * (tracking.over_limit + 1);
        let excess = tracking.table.len() as u64 - limit;
        // A map's order is as good as random here, and taking the keys in
        // one go keeps this linear.
        let victims: Vec<Bytes> = tracking
            .table
            .keys()
            .take(budget.min(excess) as usize)
            .cloned()
            .collect();
        for key in victims {
            invalidate(server, peers, key);
        }
        let tracking = &mut server.tracking;
        if tracking.table.len() as u64 <= limit {
            tracking.over_limit = 0;
        } else {
            tracking.over_limit += 1;
        }
    }
    let tracking = &mut server.tracking;
    if tracking.recording && !tracking.wanted() {
        tracking.recording = false;
        keyspace.track_changes(false);
    }
}

/// Turns tracking off for client `id`: `CLIENT TRACKING OFF`, `RESET`, or
/// the client disconnecting. Keys it read stay in the table until they are
/// invalidated, and are then skipped.
pub fn forget(server: &mut Server, id: u64) {
    let tracking = &mut server.tracking;
    let Some(state) = tracking.clients.remove(&id) else {
        return;
    };
    for prefix in state.prefixes {
        if let Some(entry) = tracking.prefixes.get_mut(&prefix) {
            entry.clients.remove(&id);
            if entry.clients.is_empty() {
                tracking.prefixes.remove(&prefix);
            }
        }
    }
}

// --- commands ---------------------------------------------------------------

/// Whether one prefix is a prefix of the other.
fn overlap(a: &[u8], b: &[u8]) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

fn overlap_error(prefix: &[u8], kind: &str, other: &[u8]) -> Frame {
    Frame::error(format!(
        "ERR Prefix '{}' overlaps with {kind} prefix '{}'. Prefixes for a single client must \
         not overlap.",
        lossy(prefix),
        lossy(other)
    ))
}

/// `CLIENT TRACKING <ON | OFF> [REDIRECT id] [PREFIX prefix ...] [BCAST]
/// [OPTIN] [OPTOUT] [NOLOOP]`, with Redis's order of checks and errors.
pub(crate) fn tracking_command(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 3 {
        return wrong_subcommand_arity("client", &args[1]);
    }
    ctx.client.last_command = "client|tracking";
    let mut redirect: i64 = 0;
    let (mut bcast, mut optin, mut optout, mut noloop) = (false, false, false, false);
    let mut prefixes: Vec<Bytes> = Vec::new();
    let mut index = 3;
    while index < args.len() {
        let option = &args[index];
        let value = args.get(index + 1);
        match value {
            Some(value) if is(option, "REDIRECT") => {
                if redirect != 0 {
                    return Frame::error("ERR A client can only redirect to a single other client");
                }
                redirect = match int(value) {
                    Ok(id) => id,
                    Err(error) => return error,
                };
                // The target must exist now, even if it may go later.
                let exists = u64::try_from(redirect)
                    .is_ok_and(|id| id == ctx.client.id || ctx.clients.contains_key(&id));
                if !exists {
                    return Frame::error("ERR The client ID you want redirect to does not exist");
                }
                index += 1;
            }
            Some(value) if is(option, "PREFIX") => {
                prefixes.push(copied(value));
                index += 1;
            }
            _ if is(option, "BCAST") => bcast = true,
            _ if is(option, "OPTIN") => optin = true,
            _ if is(option, "OPTOUT") => optout = true,
            _ if is(option, "NOLOOP") => noloop = true,
            _ => return Frame::error(ERR_SYNTAX),
        }
        index += 1;
    }
    let id = ctx.client.id;
    if is(&args[2], "OFF") {
        forget(ctx.server, id);
        return Frame::ok();
    }
    if !is(&args[2], "ON") {
        return Frame::error(ERR_SYNTAX);
    }
    if !prefixes.is_empty() && !bcast {
        return Frame::error("ERR PREFIX option requires BCAST mode to be enabled");
    }
    let current = ctx.server.tracking.clients.get(&id);
    if current.is_some_and(|state| state.bcast != bcast) {
        return Frame::error(
            "ERR You can't switch BCAST mode on/off before disabling tracking for this client, \
             and then re-enabling it with a different mode.",
        );
    }
    if bcast && (optin || optout) {
        return Frame::error("ERR OPTIN and OPTOUT are not compatible with BCAST");
    }
    if optin && optout {
        return Frame::error("ERR You can't specify both OPTIN mode and OPTOUT mode");
    }
    if current.is_some_and(|state| (optin && state.optout) || (optout && state.optin)) {
        return Frame::error(
            "ERR You can't switch OPTIN/OPTOUT mode before disabling tracking for this client, \
             and then re-enabling it with a different mode.",
        );
    }
    if bcast {
        let existing = current.map(|state| &state.prefixes);
        for (i, prefix) in prefixes.iter().enumerate() {
            if let Some(other) = existing
                .into_iter()
                .flatten()
                .find(|other| overlap(other, prefix))
            {
                return overlap_error(prefix, "an existing", other);
            }
            if let Some(other) = prefixes[i + 1..]
                .iter()
                .find(|other| overlap(prefix, other))
            {
                return overlap_error(prefix, "another provided", other);
            }
        }
    }

    let tracking = &mut ctx.server.tracking;
    let state = tracking.clients.entry(id).or_default();
    state.redirect = redirect as u64;
    state.broken = false;
    state.bcast = bcast;
    state.optin = optin;
    state.optout = optout;
    state.noloop = noloop;
    if bcast {
        if prefixes.is_empty() {
            prefixes.push(Bytes::new());
        }
        for prefix in prefixes {
            state.prefixes.insert(prefix.clone());
            tracking
                .prefixes
                .entry(prefix)
                .or_default()
                .clients
                .insert(id);
        }
    }
    start_recording(ctx.server, ctx.keyspace);
    Frame::ok()
}

/// `CLIENT CACHING <YES | NO>`: whether the next command's reads are
/// tracked, in `OPTIN` or `OPTOUT` mode.
pub(crate) fn caching_command(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    if args.len() != 3 {
        return wrong_subcommand_arity("client", &args[1]);
    }
    ctx.client.last_command = "client|caching";
    let Some(state) = ctx.server.tracking.clients.get_mut(&ctx.client.id) else {
        return Frame::error(
            "ERR CLIENT CACHING can be called only when the client is in tracking mode with \
             OPTIN or OPTOUT mode enabled",
        );
    };
    let value = &args[2];
    if is(value, "YES") {
        if !state.optin {
            return Frame::error(
                "ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.",
            );
        }
    } else if is(value, "NO") {
        if !state.optout {
            return Frame::error(
                "ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.",
            );
        }
    } else {
        return Frame::error(ERR_SYNTAX);
    }
    state.caching = true;
    Frame::ok()
}

/// The redirect as `CLIENT GETREDIR` and `CLIENT LIST` show it: −1 with
/// tracking off, 0 without a redirect.
pub(crate) fn redirect_of(state: Option<&ClientTracking>) -> i64 {
    state.map_or(-1, |state| state.redirect as i64)
}

/// `CLIENT GETREDIR`
pub(crate) fn getredir_command(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    if args.len() != 2 {
        return wrong_subcommand_arity("client", &args[1]);
    }
    ctx.client.last_command = "client|getredir";
    Frame::Integer(redirect_of(ctx.server.tracking.client(ctx.client.id)))
}

/// `CLIENT TRACKINGINFO`
pub(crate) fn trackinginfo_command(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    if args.len() != 2 {
        return wrong_subcommand_arity("client", &args[1]);
    }
    ctx.client.last_command = "client|trackinginfo";
    let state = ctx.server.tracking.client(ctx.client.id);
    let mut flags = vec![if state.is_some() { "on" } else { "off" }];
    if let Some(state) = state {
        for (set, flag) in [
            (state.bcast, "bcast"),
            (state.optin, "optin"),
            (state.optin && state.caching, "caching-yes"),
            (state.optout, "optout"),
            (state.optout && state.caching, "caching-no"),
            (state.noloop, "noloop"),
            (state.broken, "broken_redirect"),
        ] {
            if set {
                flags.push(flag);
            }
        }
    }
    let prefixes = state
        .map(|state| state.prefixes.iter().cloned().map(Frame::Bulk).collect())
        .unwrap_or_default();
    Frame::Map(vec![
        (
            Frame::bulk("flags"),
            Frame::Set(flags.into_iter().map(Frame::bulk).collect()),
        ),
        (Frame::bulk("redirect"), Frame::Integer(redirect_of(state))),
        (Frame::bulk("prefixes"), Frame::Array(prefixes)),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{args, err, Harness};
    use crate::{dispatch, ClientState};

    /// A harness whose own client (7) speaks RESP3, with more clients:
    /// `(id, RESP3?)`.
    fn harness(others: &[(u64, bool)]) -> Harness {
        let mut h = Harness::new();
        h.client.proto = Protocol::Resp3;
        for &(id, resp3) in others {
            let mut client = ClientState::new(id, format!("127.0.0.1:{}", 50_000 + id));
            crate::acl::start_session(&h.server, &mut client);
            if resp3 {
                client.proto = Protocol::Resp3;
            }
            h.clients.insert(id, client);
        }
        h
    }

    /// Runs `line` as client `id`.
    fn on(h: &mut Harness, id: u64, line: &str) -> Frame {
        let command = args(&line.split_whitespace().collect::<Vec<_>>());
        if id == h.client.id {
            return h.raw(&command);
        }
        let mut client = h.clients.remove(&id).expect("no such client");
        // The harness's own client is one of the others meanwhile.
        let own = std::mem::replace(&mut h.client, ClientState::new(0, String::new()));
        h.clients.insert(own.id, own);
        let frame = {
            let mut ctx = Ctx {
                client: &mut client,
                clients: &mut h.clients,
                server: &mut h.server,
                keyspace: &mut h.keyspace,
                now: h.now,
                block: None,
            };
            dispatch(&mut ctx, &command)
        };
        h.client = h.clients.remove(&7).expect("the harness's client");
        h.clients.insert(id, client);
        frame
    }

    fn show(frame: &Frame) -> String {
        let list = |items: &[Frame]| items.iter().map(show).collect::<Vec<_>>().join(" ");
        match frame {
            Frame::Bulk(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            Frame::Verbatim { data, .. } => String::from_utf8_lossy(data).into_owned(),
            Frame::Simple(text) | Frame::Error(text) => text.clone(),
            Frame::Integer(n) => n.to_string(),
            Frame::Null | Frame::NullArray => "null".to_owned(),
            Frame::Array(items) | Frame::Set(items) => format!("[{}]", list(items)),
            Frame::Push(items) => list(items),
            Frame::Map(pairs) => format!(
                "{{{}}}",
                pairs
                    .iter()
                    .map(|(k, v)| format!("{}: {}", show(k), show(v)))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            other => format!("{other:?}"),
        }
    }

    /// The messages queued since the last call, as `(receiver, text)`.
    fn pushes(h: &mut Harness) -> Vec<(u64, String)> {
        std::mem::take(&mut h.server.pushes)
            .iter()
            .map(|(id, frame)| (*id, show(frame)))
            .collect()
    }

    fn sent(list: &[(u64, &str)]) -> Vec<(u64, String)> {
        list.iter()
            .map(|(id, text)| (*id, (*text).to_owned()))
            .collect()
    }

    fn info(h: &mut Harness) -> String {
        show(&h.run("CLIENT TRACKINGINFO"))
    }

    /// What the execution loop does once per pass.
    fn run_broadcast(h: &mut Harness) {
        between_commands(h, |server, _, peers| broadcast(server, peers));
    }

    /// The periodic tick.
    fn tick(h: &mut Harness) {
        between_commands(h, super::tick);
    }

    /// Runs `work` with every client, the harness's own included, among
    /// the others, as the execution loop does between commands.
    fn between_commands(
        h: &mut Harness,
        work: impl FnOnce(&mut Server, &mut Keyspace, &Peers<'_>),
    ) {
        let own = std::mem::replace(&mut h.client, ClientState::new(0, String::new()));
        h.clients.insert(own.id, own);
        work(&mut h.server, &mut h.keyspace, &Peers::none(&h.clients));
        h.client = h.clients.remove(&7).expect("the harness's client");
    }

    #[test]
    fn a_read_key_is_invalidated_once_when_another_client_writes_it() {
        let mut h = harness(&[(8, false)]);
        assert_eq!(h.run("CLIENT TRACKING ON"), Frame::ok());
        h.run("GET k");
        assert_eq!(on(&mut h, 8, "SET k 1"), Frame::ok());
        assert_eq!(pushes(&mut h), sent(&[(7, "invalidate [k]")]));
        // Not read again: nothing more.
        on(&mut h, 8, "SET k 2");
        assert!(pushes(&mut h).is_empty());
        // What another client reads is not this one's business.
        on(&mut h, 8, "GET other");
        on(&mut h, 8, "SET other 1");
        assert!(pushes(&mut h).is_empty());
    }

    #[test]
    fn what_counts_as_a_read() {
        let mut h = harness(&[(8, false)]);
        h.run("CLIENT TRACKING ON");
        on(&mut h, 8, "RPUSH list x");
        // Missing keys, a WRONGTYPE error, and commands that only look.
        h.run("MGET m1 m2");
        assert!(h.run("GET list").is_error());
        for line in ["EXISTS e", "TYPE t", "TTL l", "STRLEN s"] {
            h.run(line);
        }
        on(&mut h, 8, "MSET m1 1 m2 2 e 1 t 1 l 1 s 1");
        on(&mut h, 8, "RPUSH list y");
        // One message per key, never several keys in one.
        assert_eq!(
            pushes(&mut h),
            sent(&[
                (7, "invalidate [m1]"),
                (7, "invalidate [m2]"),
                (7, "invalidate [e]"),
                (7, "invalidate [t]"),
                (7, "invalidate [l]"),
                (7, "invalidate [s]"),
                (7, "invalidate [list]"),
            ])
        );
        // A write is not a read.
        h.run("SET w 1");
        pushes(&mut h);
        on(&mut h, 8, "SET w 2");
        assert!(pushes(&mut h).is_empty());
    }

    #[test]
    fn the_table_ignores_databases() {
        let mut h = harness(&[(8, false)]);
        h.run("CLIENT TRACKING ON");
        h.run("GET k");
        on(&mut h, 8, "SELECT 3");
        on(&mut h, 8, "SET k 1");
        assert_eq!(pushes(&mut h), sent(&[(7, "invalidate [k]")]));
    }

    #[test]
    fn writes_that_change_nothing_invalidate_nothing() {
        let mut h = harness(&[(8, false)]);
        on(&mut h, 8, "SADD s x");
        on(&mut h, 8, "RPUSH l x");
        h.run("CLIENT TRACKING ON");
        h.run("SMEMBERS s");
        h.run("LRANGE l 0 -1");
        on(&mut h, 8, "SADD s x");
        on(&mut h, 8, "SREM s nope");
        on(&mut h, 8, "INCR s");
        on(&mut h, 8, "LREM l 0 nope");
        assert!(pushes(&mut h).is_empty());
        on(&mut h, 8, "SADD s y");
        assert_eq!(pushes(&mut h), sent(&[(7, "invalidate [s]")]));
    }

    #[test]
    fn every_kind_of_change_invalidates() {
        let mut h = harness(&[(8, false)]);
        h.run("CLIENT TRACKING ON");
        for (setup, read, write, keys) in [
            ("SET a 1", "GET a", "DEL a", "a"),
            ("SET a 1", "MGET a b", "RENAME a b", "a b"),
            ("SET a 1", "GET a", "EXPIRE a 100", "a"),
            ("SET a 1 EX 100", "GET a", "PERSIST a", "a"),
            (
                "RPUSH src 3 1 2",
                "LRANGE dst 0 -1",
                "SORT src STORE dst",
                "dst",
            ),
            (
                "HSET h f 1 g 2",
                "HGETALL h",
                "HPEXPIRE h 100 FIELDS 1 f",
                "h",
            ),
            ("SET a 1", "GET c", "COPY a c", "c"),
            ("SET a 1", "GET a", "FLUSHDB", ""),
        ] {
            on(&mut h, 8, setup);
            on(&mut h, 8, "DEL c b");
            h.run(read);
            pushes(&mut h);
            on(&mut h, 8, write);
            let expected: Vec<(u64, String)> = if keys.is_empty() {
                sent(&[(7, "invalidate null")])
            } else {
                keys.split(' ')
                    .map(|key| (7, format!("invalidate [{key}]")))
                    .collect()
            };
            assert_eq!(pushes(&mut h), expected, "{write}");
        }
    }

    #[test]
    fn own_writes_are_reported_after_the_reply_and_its_own_notifications() {
        let mut h = harness(&[]);
        h.run("CONFIG SET notify-keyspace-events KEA");
        h.run("CLIENT TRACKING ON");
        h.run("PSUBSCRIBE __keyspace@0__:*");
        pushes(&mut h);
        h.run("GET own");
        h.run("SET own 1");
        assert_eq!(
            pushes(&mut h),
            sent(&[
                (7, "pmessage __keyspace@0__:* __keyspace@0__:own set"),
                (7, "invalidate [own]"),
            ])
        );
    }

    #[test]
    fn invalidations_go_before_the_writes_event_and_after_an_expiry() {
        let mut h = harness(&[(8, false)]);
        h.run("CONFIG SET notify-keyspace-events KEA");
        h.run("CLIENT TRACKING ON");
        h.run("PSUBSCRIBE __keyspace@0__:*");
        h.run("GET k");
        pushes(&mut h);
        on(&mut h, 8, "SET k 1");
        assert_eq!(
            pushes(&mut h),
            sent(&[
                (7, "invalidate [k]"),
                (7, "pmessage __keyspace@0__:* __keyspace@0__:k set"),
            ])
        );
        on(&mut h, 8, "SET x v PX 100");
        h.run("GET x");
        pushes(&mut h);
        h.advance(200);
        // Found expired by another client's write: `expired`, the
        // invalidation, then the write's own event.
        on(&mut h, 8, "SET x v2");
        assert_eq!(
            pushes(&mut h),
            sent(&[
                (7, "pmessage __keyspace@0__:* __keyspace@0__:x expired"),
                (7, "invalidate [x]"),
                (7, "pmessage __keyspace@0__:* __keyspace@0__:x set"),
            ])
        );
    }

    #[test]
    fn a_key_found_expired_by_its_reader_is_invalidated_and_tracked_again() {
        let mut h = harness(&[(8, false)]);
        h.run("CLIENT TRACKING ON");
        on(&mut h, 8, "SET k v PX 100");
        h.run("GET k");
        h.advance(200);
        assert_eq!(h.run("GET k"), Frame::Null);
        assert_eq!(pushes(&mut h), sent(&[(7, "invalidate [k]")]));
        on(&mut h, 8, "SET k v");
        assert_eq!(pushes(&mut h), sent(&[(7, "invalidate [k]")]));
        // With NOLOOP its own read finding it expired says nothing.
        h.run("CLIENT TRACKING ON NOLOOP");
        on(&mut h, 8, "SET k v PX 100");
        h.run("GET k");
        h.advance(200);
        h.run("GET k");
        assert!(pushes(&mut h).is_empty());
    }

    #[test]
    fn transactions_and_scripts_report_after_their_reply() {
        let mut h = harness(&[(8, false)]);
        h.run("CLIENT TRACKING ON");
        h.run("GET t");
        h.run("MULTI");
        h.run("GET t");
        h.run("SET t 1");
        assert_eq!(h.run("EXEC"), Frame::Array(vec![Frame::Null, Frame::ok()]));
        assert_eq!(pushes(&mut h), sent(&[(7, "invalidate [t]")]));
        let read_then_write =
            "redis.call('GET', KEYS[1]); redis.call('SET', KEYS[1], 'x'); return 1";
        h.cmd(&["EVAL", read_then_write, "1", "k8"]);
        assert_eq!(pushes(&mut h), sent(&[(7, "invalidate [k8]")]));
        let write_then_read = "redis.call('SET', KEYS[1], 'x'); return redis.call('GET', KEYS[1])";
        h.cmd(&["EVAL", write_then_read, "1", "k9"]);
        assert!(pushes(&mut h).is_empty());
        on(&mut h, 8, "SET k9 y");
        assert_eq!(pushes(&mut h), sent(&[(7, "invalidate [k9]")]));
    }

    #[test]
    fn a_read_only_scripts_own_keys_are_not_remembered_only_its_reads() {
        let mut h = harness(&[(8, false)]);
        h.run("CLIENT TRACKING ON");
        h.cmd(&[
            "EVAL_RO",
            "return redis.call('GET', KEYS[1])",
            "2",
            "read",
            "named",
        ]);
        on(&mut h, 8, "MSET read 1 named 1");
        assert_eq!(pushes(&mut h), sent(&[(7, "invalidate [read]")]));
    }

    #[test]
    fn noloop_leaves_out_own_changes() {
        let mut h = harness(&[(8, false)]);
        h.run("CLIENT TRACKING ON NOLOOP");
        h.run("GET n");
        h.run("SET n 1");
        assert!(pushes(&mut h).is_empty());
        h.run("GET n");
        on(&mut h, 8, "SET n 2");
        assert_eq!(pushes(&mut h), sent(&[(7, "invalidate [n]")]));
    }

    #[test]
    fn where_messages_go() {
        // 7 tracks; 8 RESP2 subscribed, 9 RESP2 not subscribed, 10 RESP3.
        let mut h = harness(&[(8, false), (9, false), (10, true), (11, false)]);
        on(&mut h, 8, "SUBSCRIBE something-else");
        pushes(&mut h);
        let through = |h: &mut Harness, redirect: Option<u64>, resp3: bool| {
            h.client.proto = if resp3 {
                Protocol::Resp3
            } else {
                Protocol::Resp2
            };
            let on_command = match redirect {
                Some(id) => format!("CLIENT TRACKING ON REDIRECT {id}"),
                None => "CLIENT TRACKING ON".to_owned(),
            };
            assert_eq!(h.run(&on_command), Frame::ok());
            h.run("GET k");
            on(h, 11, "SET k 1");
            pushes(h)
        };
        assert_eq!(
            through(&mut h, Some(8), false),
            sent(&[(8, "message __redis__:invalidate [k]")])
        );
        assert!(through(&mut h, Some(9), true).is_empty());
        assert_eq!(
            through(&mut h, Some(10), false),
            sent(&[(10, "invalidate [k]")])
        );
        assert!(through(&mut h, None, false).is_empty());
        assert_eq!(through(&mut h, None, true), sent(&[(7, "invalidate [k]")]));
        // Redirecting to itself is allowed.
        assert_eq!(
            through(&mut h, Some(7), true),
            sent(&[(7, "invalidate [k]")])
        );
    }

    #[test]
    fn a_redirect_target_that_has_gone_is_reported_on_every_message() {
        let mut h = harness(&[(8, true), (9, false)]);
        h.run("CLIENT TRACKING ON REDIRECT 8");
        h.run("GET k");
        h.clients.remove(&8);
        on(&mut h, 9, "SET k 1");
        assert_eq!(pushes(&mut h), sent(&[(7, "tracking-redir-broken 8")]));
        assert_eq!(
            info(&mut h),
            "{flags: [on broken_redirect], redirect: 8, prefixes: []}"
        );
        let line = show(&h.run("CLIENT INFO"));
        assert!(
            line.contains(" flags=tR ") && line.contains(" redir=8 "),
            "{line}"
        );
        h.run("GET k");
        on(&mut h, 9, "SET k 2");
        assert_eq!(pushes(&mut h), sent(&[(7, "tracking-redir-broken 8")]));
        // Flushes too.
        on(&mut h, 9, "FLUSHALL");
        assert_eq!(pushes(&mut h), sent(&[(7, "tracking-redir-broken 8")]));
        // A RESP2 client cannot be told.
        h.client.proto = Protocol::Resp2;
        h.run("GET k");
        on(&mut h, 9, "SET k 3");
        assert!(pushes(&mut h).is_empty());
        // A new redirect repairs it.
        assert_eq!(
            h.run("CLIENT TRACKING ON REDIRECT 8"),
            err("ERR The client ID you want redirect to does not exist")
        );
        assert_eq!(h.run("CLIENT TRACKING ON REDIRECT 9"), Frame::ok());
        assert_eq!(info(&mut h), "{flags: [on], redirect: 9, prefixes: []}");
    }

    #[test]
    fn option_errors_match_redis() {
        let mut h = harness(&[(8, false)]);
        let overlap = "Prefixes for a single client must not overlap.";
        for (line, error) in [
            (
                "CLIENT TRACKING",
                "ERR wrong number of arguments for 'client|tracking' command".to_owned(),
            ),
            ("CLIENT TRACKING maybe", "ERR syntax error".to_owned()),
            ("CLIENT TRACKING maybe FOO", "ERR syntax error".to_owned()),
            ("CLIENT TRACKING ON FOO", "ERR syntax error".to_owned()),
            ("CLIENT TRACKING ON REDIRECT", "ERR syntax error".to_owned()),
            (
                "CLIENT TRACKING ON BCAST PREFIX",
                "ERR syntax error".to_owned(),
            ),
            (
                "CLIENT TRACKING ON FOO REDIRECT 999",
                "ERR syntax error".to_owned(),
            ),
            (
                "CLIENT TRACKING ON REDIRECT abc",
                "ERR value is not an integer or out of range".to_owned(),
            ),
            (
                "CLIENT TRACKING ON REDIRECT -1",
                "ERR The client ID you want redirect to does not exist".to_owned(),
            ),
            (
                "CLIENT TRACKING ON REDIRECT 0",
                "ERR The client ID you want redirect to does not exist".to_owned(),
            ),
            (
                "CLIENT TRACKING ON REDIRECT 999 FOO",
                "ERR The client ID you want redirect to does not exist".to_owned(),
            ),
            (
                "CLIENT TRACKING OFF REDIRECT 999",
                "ERR The client ID you want redirect to does not exist".to_owned(),
            ),
            (
                "CLIENT TRACKING maybe REDIRECT 999",
                "ERR The client ID you want redirect to does not exist".to_owned(),
            ),
            (
                "CLIENT TRACKING ON REDIRECT 8 REDIRECT 8",
                "ERR A client can only redirect to a single other client".to_owned(),
            ),
            (
                "CLIENT TRACKING ON PREFIX a",
                "ERR PREFIX option requires BCAST mode to be enabled".to_owned(),
            ),
            (
                "CLIENT TRACKING ON OPTIN OPTOUT PREFIX a",
                "ERR PREFIX option requires BCAST mode to be enabled".to_owned(),
            ),
            (
                "CLIENT TRACKING ON OPTIN OPTOUT",
                "ERR You can't specify both OPTIN mode and OPTOUT mode".to_owned(),
            ),
            (
                "CLIENT TRACKING ON BCAST OPTIN",
                "ERR OPTIN and OPTOUT are not compatible with BCAST".to_owned(),
            ),
            (
                "CLIENT TRACKING ON BCAST OPTOUT",
                "ERR OPTIN and OPTOUT are not compatible with BCAST".to_owned(),
            ),
            (
                "CLIENT TRACKING ON BCAST OPTIN OPTOUT",
                "ERR OPTIN and OPTOUT are not compatible with BCAST".to_owned(),
            ),
            (
                "CLIENT TRACKING ON BCAST PREFIX ab PREFIX a",
                format!("ERR Prefix 'ab' overlaps with another provided prefix 'a'. {overlap}"),
            ),
            (
                "CLIENT TRACKING ON BCAST PREFIX a PREFIX b PREFIX ab",
                format!("ERR Prefix 'a' overlaps with another provided prefix 'ab'. {overlap}"),
            ),
            (
                "CLIENT CACHING",
                "ERR wrong number of arguments for 'client|caching' command".to_owned(),
            ),
            (
                "CLIENT CACHING yes extra",
                "ERR wrong number of arguments for 'client|caching' command".to_owned(),
            ),
            (
                "CLIENT CACHING YES",
                "ERR CLIENT CACHING can be called only when the client is in tracking mode \
                 with OPTIN or OPTOUT mode enabled"
                    .to_owned(),
            ),
            (
                "CLIENT CACHING maybe",
                "ERR CLIENT CACHING can be called only when the client is in tracking mode \
                 with OPTIN or OPTOUT mode enabled"
                    .to_owned(),
            ),
            (
                "CLIENT TRACKINGINFO extra",
                "ERR wrong number of arguments for 'client|trackinginfo' command".to_owned(),
            ),
            (
                "CLIENT GETREDIR extra",
                "ERR wrong number of arguments for 'client|getredir' command".to_owned(),
            ),
        ] {
            assert_eq!(h.run(line), err(&error), "{line}");
        }
        // Nothing of a refused command applied.
        assert_eq!(info(&mut h), "{flags: [off], redirect: -1, prefixes: []}");
        // OFF takes and ignores options.
        assert_eq!(h.run("CLIENT TRACKING OFF BCAST NOLOOP"), Frame::ok());
        let switch_bcast = "ERR You can't switch BCAST mode on/off before disabling tracking \
                            for this client, and then re-enabling it with a different mode.";
        h.run("CLIENT TRACKING ON");
        for (line, error) in [
            (
                "CLIENT CACHING YES",
                "ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.",
            ),
            (
                "CLIENT CACHING NO",
                "ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.",
            ),
            ("CLIENT CACHING maybe", "ERR syntax error"),
            ("CLIENT TRACKING ON BCAST", switch_bcast),
            ("CLIENT TRACKING ON BCAST OPTIN", switch_bcast),
        ] {
            assert_eq!(h.run(line), err(error), "{line}");
        }
        h.run("CLIENT TRACKING ON OPTIN");
        assert_eq!(
            h.run("CLIENT TRACKING ON OPTOUT"),
            err(
                "ERR You can't switch OPTIN/OPTOUT mode before disabling tracking for this \
                 client, and then re-enabling it with a different mode."
            )
        );
        h.run("CLIENT TRACKING OFF");
        h.run("CLIENT TRACKING ON BCAST PREFIX p");
        assert_eq!(
            h.run("CLIENT TRACKING ON BCAST PREFIX q PREFIX pq"),
            err(&format!(
                "ERR Prefix 'pq' overlaps with an existing prefix 'p'. {overlap}"
            ))
        );
        assert_eq!(h.run("CLIENT TRACKING ON"), err(switch_bcast));
        assert_eq!(
            info(&mut h),
            "{flags: [on bcast], redirect: 0, prefixes: [p]}"
        );
    }

    #[test]
    fn changing_options_while_on() {
        let mut h = harness(&[(8, false)]);
        h.run("CLIENT TRACKING ON REDIRECT 8");
        assert_eq!(h.run("CLIENT GETREDIR"), Frame::Integer(8));
        h.run("CLIENT TRACKING ON OPTIN");
        assert_eq!(h.run("CLIENT GETREDIR"), Frame::Integer(0));
        assert_eq!(
            info(&mut h),
            "{flags: [on optin], redirect: 0, prefixes: []}"
        );
        h.run("CLIENT TRACKING ON NOLOOP");
        assert_eq!(
            info(&mut h),
            "{flags: [on noloop], redirect: 0, prefixes: []}"
        );
        h.run("CLIENT TRACKING OFF");
        assert_eq!(h.run("CLIENT GETREDIR"), Frame::Integer(-1));
        h.run("CLIENT TRACKING ON BCAST PREFIX p");
        h.run("CLIENT TRACKING ON BCAST PREFIX q NOLOOP");
        assert_eq!(
            info(&mut h),
            "{flags: [on bcast noloop], redirect: 0, prefixes: [p q]}"
        );
        h.run("CLIENT TRACKING ON BCAST");
        assert_eq!(
            info(&mut h),
            "{flags: [on bcast], redirect: 0, prefixes: [ p q]}"
        );
        assert!(show(&h.run("INFO stats")).contains("tracking_total_prefixes:3\r\n"));
        h.run("CLIENT TRACKING OFF");
        assert_eq!(info(&mut h), "{flags: [off], redirect: -1, prefixes: []}");
        assert!(show(&h.run("INFO stats")).contains("tracking_total_prefixes:0\r\n"));
    }

    #[test]
    fn trackinginfo_flags_in_redis_order() {
        let mut h = harness(&[(8, false)]);
        h.run("CLIENT TRACKING ON OPTIN NOLOOP REDIRECT 8");
        h.run("CLIENT CACHING YES");
        assert_eq!(
            info(&mut h),
            "{flags: [on optin caching-yes noloop], redirect: 8, prefixes: []}"
        );
        h.run("CLIENT TRACKING OFF");
        h.run("CLIENT TRACKING ON OPTOUT");
        h.run("CLIENT CACHING NO");
        assert_eq!(
            info(&mut h),
            "{flags: [on optout caching-no], redirect: 0, prefixes: []}"
        );
        // RESP2: a flat list.
        h.client.proto = Protocol::Resp2;
        let mut out = bytes::BytesMut::new();
        rexyra_resp::encode(&h.run("CLIENT TRACKINGINFO"), Protocol::Resp2, &mut out);
        assert!(
            out.starts_with(b"*6\r\n$5\r\nflags\r\n*3\r\n$2\r\non\r\n$6\r\noptout\r\n"),
            "{out:?}"
        );
    }

    /// Deletes `keys` as client 8 and returns the invalidations it caused.
    fn invalidated(h: &mut Harness, keys: &str) -> String {
        on(h, 8, &format!("DEL {keys}"));
        pushes(h)
            .into_iter()
            .map(|(_, text)| text)
            .collect::<Vec<_>>()
            .join(",")
    }

    #[test]
    fn optin_tracks_only_the_command_after_caching_yes() {
        let mut h = harness(&[(8, false)]);
        h.run("CLIENT TRACKING ON OPTIN");
        on(&mut h, 8, "MSET a 1 b 1");
        h.run("GET a");
        assert_eq!(invalidated(&mut h, "a"), "");
        h.run("CLIENT CACHING YES");
        h.run("GET a");
        h.run("GET b");
        on(&mut h, 8, "MSET a 1 b 1");
        assert_eq!(invalidated(&mut h, "a b"), "invalidate [a]");
        // A transaction counts as one command.
        h.run("CLIENT CACHING YES");
        h.run("MULTI");
        h.run("GET a");
        h.run("GET b");
        h.run("EXEC");
        on(&mut h, 8, "MSET a 1 b 1");
        assert_eq!(invalidated(&mut h, "a b"), "invalidate [a],invalidate [b]");
        // Any other command uses it up, even a refused or unknown one...
        for between in ["PING", "GET", "NOSUCHCOMMAND"] {
            h.run("CLIENT CACHING YES");
            h.run(between);
            h.run("GET a");
            on(&mut h, 8, "SET a 1");
            assert_eq!(invalidated(&mut h, "a"), "", "{between}");
        }
        // ... but not another CLIENT subcommand.
        h.run("CLIENT CACHING YES");
        h.run("CLIENT ID");
        h.run("GET a");
        on(&mut h, 8, "SET a 1");
        assert_eq!(invalidated(&mut h, "a"), "invalidate [a]");
        // Inside a script, the reads count; the next script's do not.
        h.run("CLIENT CACHING YES");
        let read = "return redis.call('GET', KEYS[1])";
        h.cmd(&["EVAL", read, "1", "a"]);
        h.cmd(&["EVAL", read, "1", "b"]);
        on(&mut h, 8, "MSET a 1 b 1");
        assert_eq!(invalidated(&mut h, "a b"), "invalidate [a]");
    }

    #[test]
    fn optout_skips_only_the_command_after_caching_no() {
        let mut h = harness(&[(8, false)]);
        h.run("CLIENT TRACKING ON OPTOUT");
        h.run("GET p1");
        h.run("CLIENT CACHING NO");
        h.run("GET p2");
        h.run("GET p3");
        on(&mut h, 8, "MSET p1 1 p2 1 p3 1");
        assert_eq!(
            pushes(&mut h),
            sent(&[(7, "invalidate [p1]"), (7, "invalidate [p3]")])
        );
    }

    #[test]
    fn the_caching_flag_outlives_a_change_of_mode() {
        let mut h = harness(&[]);
        h.run("CLIENT TRACKING ON OPTIN");
        h.run("CLIENT CACHING YES");
        h.run("CLIENT TRACKING ON");
        assert_eq!(info(&mut h), "{flags: [on], redirect: 0, prefixes: []}");
        h.run("CLIENT TRACKING ON OPTOUT");
        assert_eq!(
            info(&mut h),
            "{flags: [on optout caching-no], redirect: 0, prefixes: []}"
        );
    }

    #[test]
    fn flushes_tell_every_tracking_client_in_id_order() {
        // 7 RESP3; 8 broadcasts; 9 RESP2 redirecting to 10, subscribed.
        let mut h = harness(&[(8, true), (9, false), (10, false), (11, false)]);
        on(&mut h, 10, "SUBSCRIBE __redis__:invalidate");
        on(&mut h, 8, "CLIENT TRACKING ON BCAST");
        on(&mut h, 9, "CLIENT TRACKING ON REDIRECT 10");
        h.run("CLIENT TRACKING ON");
        h.run("GET k");
        pushes(&mut h);
        on(&mut h, 11, "FLUSHDB");
        assert_eq!(
            pushes(&mut h),
            sent(&[
                (7, "invalidate null"),
                (8, "invalidate null"),
                (10, "message __redis__:invalidate null"),
            ])
        );
        assert!(show(&h.run("INFO stats")).contains("tracking_total_keys:0\r\n"));
        // The table was emptied: a later write of k is only broadcast.
        on(&mut h, 11, "SET k 1");
        run_broadcast(&mut h);
        assert_eq!(pushes(&mut h), sent(&[(8, "invalidate [k]")]));
        // Its own flush reaches the caller after the reply; ASYNC too.
        h.run("FLUSHALL ASYNC");
        assert_eq!(
            pushes(&mut h),
            sent(&[
                (8, "invalidate null"),
                (10, "message __redis__:invalidate null"),
                (7, "invalidate null"),
            ])
        );
        // SWAPDB says nothing.
        on(&mut h, 11, "SET k 1");
        run_broadcast(&mut h);
        pushes(&mut h);
        h.run("GET k");
        on(&mut h, 11, "SWAPDB 0 1");
        run_broadcast(&mut h);
        assert!(pushes(&mut h).is_empty());
    }

    #[test]
    fn broadcasting_batches_sorted_keys_per_prefix() {
        let mut h = harness(&[(8, true), (9, false), (10, true)]);
        on(&mut h, 8, "CLIENT TRACKING ON BCAST PREFIX a PREFIX b");
        on(&mut h, 10, "CLIENT TRACKING ON BCAST");
        for line in ["SET a2 1", "SET b1 1", "SET a1 1", "SET c1 1", "SET a2 2"] {
            on(&mut h, 9, line);
        }
        assert!(pushes(&mut h).is_empty());
        run_broadcast(&mut h);
        assert_eq!(
            pushes(&mut h),
            sent(&[
                (10, "invalidate [a1 a2 b1 c1]"),
                (8, "invalidate [a1 a2]"),
                (8, "invalidate [b1]"),
            ])
        );
        run_broadcast(&mut h);
        assert!(pushes(&mut h).is_empty());
        // Own writes, without NOLOOP, are sent too.
        on(&mut h, 8, "SET a9 1");
        run_broadcast(&mut h);
        assert_eq!(
            pushes(&mut h),
            sent(&[(10, "invalidate [a9]"), (8, "invalidate [a9]")])
        );
    }

    #[test]
    fn broadcasting_with_noloop_leaves_out_keys_only_the_client_changed() {
        let mut h = harness(&[(8, true), (9, false)]);
        on(&mut h, 8, "CLIENT TRACKING ON BCAST NOLOOP PREFIX z");
        on(&mut h, 8, "SET z1 1");
        on(&mut h, 8, "SET z2 1");
        run_broadcast(&mut h);
        assert!(pushes(&mut h).is_empty());
        on(&mut h, 8, "SET z1 1");
        on(&mut h, 8, "SET z2 1");
        on(&mut h, 9, "SET z3 1");
        on(&mut h, 9, "SET z1 2");
        run_broadcast(&mut h);
        assert_eq!(pushes(&mut h), sent(&[(8, "invalidate [z1 z3]")]));
        // Changed by the server (expiry found by its own read): nobody's
        // change, so it is sent.
        on(&mut h, 8, "SET z4 v PX 100");
        run_broadcast(&mut h);
        h.advance(200);
        on(&mut h, 8, "GET z4");
        run_broadcast(&mut h);
        assert_eq!(pushes(&mut h), sent(&[(8, "invalidate [z4]")]));
    }

    #[test]
    fn keys_read_before_switching_to_broadcast_or_off_are_dropped() {
        let mut h = harness(&[(8, false)]);
        h.run("CLIENT TRACKING ON");
        h.run("GET a");
        h.run("CLIENT TRACKING OFF");
        on(&mut h, 8, "SET a 1");
        h.run("CLIENT TRACKING ON");
        on(&mut h, 8, "SET a 2");
        assert!(pushes(&mut h).is_empty());
        h.run("GET b");
        h.run("CLIENT TRACKING OFF");
        h.run("CLIENT TRACKING ON BCAST PREFIX x");
        on(&mut h, 8, "SET b 1");
        run_broadcast(&mut h);
        assert!(pushes(&mut h).is_empty());
    }

    #[test]
    fn counts_for_info() {
        let mut h = harness(&[(8, true), (9, false)]);
        h.run("CLIENT TRACKING ON");
        on(&mut h, 8, "CLIENT TRACKING ON");
        h.run("MGET c1 c1 c2");
        on(&mut h, 8, "GET c1");
        let stats = |h: &mut Harness| {
            let text = show(&h.run("INFO stats")) + &show(&h.run("INFO clients"));
            [
                "tracking_clients",
                "tracking_total_keys",
                "tracking_total_items",
            ]
            .iter()
            .map(|name| {
                text.lines()
                    .find_map(|line| line.strip_prefix(&format!("{name}:")))
                    .unwrap()
                    .trim()
                    .to_owned()
            })
            .collect::<Vec<_>>()
            .join(" ")
        };
        assert_eq!(stats(&mut h), "2 2 3");
        on(&mut h, 8, "CLIENT TRACKING OFF");
        assert_eq!(stats(&mut h), "1 2 3");
        on(&mut h, 9, "SET c1 1");
        assert_eq!(stats(&mut h), "1 1 1");
        h.run("RESET");
        assert_eq!(stats(&mut h), "0 1 1");
    }

    #[test]
    fn client_list_shows_the_flags_and_redirect() {
        let mut h = harness(&[(8, true), (9, true), (10, false)]);
        on(&mut h, 8, "CLIENT TRACKING ON BCAST");
        on(&mut h, 9, "CLIENT TRACKING ON OPTIN NOLOOP REDIRECT 10");
        let list = show(&h.run("CLIENT LIST"));
        let line = |id: u64| {
            list.lines()
                .find(|line| line.starts_with(&format!("id={id} ")))
                .unwrap()
                .to_owned()
        };
        assert!(line(7).contains(" flags=N ") && line(7).contains(" redir=-1 "));
        assert!(line(8).contains(" flags=tB ") && line(8).contains(" redir=0 "));
        assert!(line(9).contains(" flags=t ") && line(9).contains(" redir=10 "));
    }

    #[test]
    fn reset_and_disconnect_turn_tracking_off() {
        let mut h = harness(&[(8, false)]);
        h.run("CLIENT TRACKING ON BCAST PREFIX p");
        h.run("RESET");
        assert_eq!(h.client.proto, Protocol::Resp2);
        assert_eq!(info(&mut h), "{flags: [off], redirect: -1, prefixes: []}");
        assert_eq!(h.server.tracking.prefix_count(), 0);
        on(&mut h, 8, "CLIENT TRACKING ON");
        forget(&mut h.server, 8);
        assert_eq!(h.server.tracking.client_count(), 0);
    }

    #[test]
    fn the_table_is_kept_within_its_limit() {
        let mut h = harness(&[(8, false)]);
        assert_eq!(
            show(&h.run("CONFIG GET tracking-table-max-keys")),
            "{tracking-table-max-keys: 1000000}"
        );
        h.run("CLIENT TRACKING ON");
        h.run("MGET t1 t2 t3 t4 t5 t6");
        assert_eq!(h.run("CONFIG SET tracking-table-max-keys 2"), Frame::ok());
        tick(&mut h);
        let dropped = pushes(&mut h);
        assert_eq!(dropped.len(), 4, "{dropped:?}");
        assert!(dropped
            .iter()
            .all(|(id, text)| *id == 7 && text.starts_with("invalidate [t")));
        assert_eq!(h.server.tracking.key_count(), 2);
        // No limit.
        h.run("CONFIG SET tracking-table-max-keys 0");
        h.run("MGET u1 u2 u3 u4 u5");
        tick(&mut h);
        assert!(pushes(&mut h).is_empty());
        assert_eq!(h.server.tracking.key_count(), 7);
        assert_eq!(
            h.run("CONFIG SET tracking-table-max-keys -1"),
            err(
                "ERR CONFIG SET failed (possibly related to argument 'tracking-table-max-keys') \
                 - argument must be between 0 and 9223372036854775807 inclusive"
            )
        );
    }

    #[test]
    fn a_tick_does_a_bounded_amount_of_work_more_each_time() {
        let mut h = harness(&[]);
        h.run("CLIENT TRACKING ON");
        let keys: Vec<String> = (0..1000).map(|i| format!("k{i}")).collect();
        let mut command = vec!["MGET"];
        command.extend(keys.iter().map(String::as_str));
        h.cmd(&command);
        h.run("CONFIG SET tracking-table-max-keys 1");
        let mut left = Vec::new();
        for _ in 0..4 {
            tick(&mut h);
            left.push(h.server.tracking.key_count());
        }
        assert_eq!(left, [900, 700, 400, 1]);
        assert_eq!(pushes(&mut h).len(), 999);
    }

    #[test]
    fn eviction_invalidates_before_its_event() {
        let mut h = harness(&[(8, false)]);
        h.run("CONFIG SET notify-keyspace-events KEA");
        h.run("CLIENT TRACKING ON");
        h.run("PSUBSCRIBE __keyspace@0__:*");
        on(&mut h, 8, "SET ev 1");
        h.run("GET ev");
        pushes(&mut h);
        on(&mut h, 8, "CONFIG SET maxmemory-policy allkeys-random");
        // A lower limit is enforced at once, by the command that sets it.
        on(&mut h, 8, "CONFIG SET maxmemory 1");
        assert_eq!(
            pushes(&mut h),
            sent(&[
                (7, "invalidate [ev]"),
                (7, "pmessage __keyspace@0__:* __keyspace@0__:ev evicted"),
            ])
        );
    }

    #[test]
    fn nothing_is_recorded_until_someone_tracks() {
        let mut h = harness(&[(8, false)]);
        h.run("SET a 1");
        assert!(!recording(&h.server));
        h.run("CLIENT TRACKING ON");
        assert!(!recording(&h.server));
        h.run("GET a");
        assert!(recording(&h.server));
        on(&mut h, 8, "SET a 2");
        assert_eq!(pushes(&mut h), sent(&[(7, "invalidate [a]")]));
        // Once the table is empty and nobody broadcasts, the tick stops
        // the recording.
        tick(&mut h);
        assert!(!recording(&h.server));
        h.run("SET a 3");
        assert!(h.keyspace.take_notes().is_empty());
    }

    #[test]
    fn keys_are_copied_out_of_the_request() {
        let mut h = harness(&[]);
        h.run("CLIENT TRACKING ON");
        let buffer = Bytes::from_static(b"GETkey-in-a-big-buffer");
        let key = buffer.slice(3..);
        h.raw(&[buffer.slice(0..3), key.clone()]);
        let stored = h.server.tracking.table.keys().next().unwrap().clone();
        assert_eq!(stored, key);
        assert_ne!(stored.as_ptr(), key.as_ptr());
    }
}
