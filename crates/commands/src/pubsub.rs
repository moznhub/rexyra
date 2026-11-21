//! Pub/Sub: `SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`, their `UNSUBSCRIBE`
//! counterparts, `PUBLISH`, `SPUBLISH` and `PUBSUB`.
//!
//! The server keeps, for every channel and pattern, the clients subscribed
//! to it ([`PubSub`]); each client keeps what it is subscribed to
//! ([`Subscriptions`]). Publishing does not write to anyone directly: it
//! leaves the messages in [`Server::pushes`], and the execution loop
//! appends each to its receiver's output, behind whatever that client has
//! been sent already.
//!
//! Sharded channels exist for cluster mode. On a single server they behave
//! like ordinary channels, kept apart: a message sent with `SPUBLISH`
//! reaches only `SSUBSCRIBE`rs, and the other way round.
//!
//! Messages are not stored anywhere. A message published while nobody is
//! subscribed is lost, and one published to a client that has gone is
//! dropped, as in Redis.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use rexyra_engine::glob_match;
use rexyra_resp::{Frame, Protocol};

use crate::{is, lossy, unknown_subcommand, wrong_subcommand_arity, ClientState, Ctx, Server};

/// What one client is subscribed to.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Subscriptions {
    pub channels: BTreeSet<Bytes>,
    pub patterns: BTreeSet<Bytes>,
    pub shard: BTreeSet<Bytes>,
}

impl Subscriptions {
    /// Subscribed to anything at all: in RESP2 the connection is then in
    /// subscribed mode, and `CLIENT LIST` shows it as a Pub/Sub client.
    pub fn any(&self) -> bool {
        !(self.channels.is_empty() && self.patterns.is_empty() && self.shard.is_empty())
    }
}

/// The clients subscribed to each channel (or pattern), in order of id.
type Registry = BTreeMap<Bytes, BTreeSet<u64>>;

/// Every subscription on the server, by channel and by pattern.
#[derive(Debug, Default)]
pub struct PubSub {
    channels: Registry,
    patterns: Registry,
    shard: Registry,
}

impl PubSub {
    /// Channels with at least one subscriber, for `INFO`.
    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    /// Patterns with at least one subscriber, for `INFO` and `PUBSUB NUMPAT`.
    pub fn pattern_count(&self) -> usize {
        self.patterns.len()
    }

    /// Shard channels with at least one subscriber, for `INFO`.
    pub fn shard_channel_count(&self) -> usize {
        self.shard.len()
    }
}

/// The three kinds of subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Channel,
    Pattern,
    Shard,
}

impl Kind {
    fn subscribe(self) -> &'static str {
        match self {
            Kind::Channel => "subscribe",
            Kind::Pattern => "psubscribe",
            Kind::Shard => "ssubscribe",
        }
    }

    fn unsubscribe(self) -> &'static str {
        match self {
            Kind::Channel => "unsubscribe",
            Kind::Pattern => "punsubscribe",
            Kind::Shard => "sunsubscribe",
        }
    }

    fn mine(self, subs: &mut Subscriptions) -> &mut BTreeSet<Bytes> {
        match self {
            Kind::Channel => &mut subs.channels,
            Kind::Pattern => &mut subs.patterns,
            Kind::Shard => &mut subs.shard,
        }
    }

    /// The count a confirmation reports: shard subscriptions are counted on
    /// their own, the other two together.
    fn count(self, subs: &Subscriptions) -> i64 {
        match self {
            Kind::Channel | Kind::Pattern => (subs.channels.len() + subs.patterns.len()) as i64,
            Kind::Shard => subs.shard.len() as i64,
        }
    }

    /// Adds or removes `id` in the server's registry for `name`.
    fn register(self, pubsub: &mut PubSub, name: &Bytes, id: u64, add: bool) {
        let registry = match self {
            Kind::Channel => &mut pubsub.channels,
            Kind::Pattern => &mut pubsub.patterns,
            Kind::Shard => &mut pubsub.shard,
        };
        if add {
            registry.entry(name.clone()).or_default().insert(id);
        } else if let Some(ids) = registry.get_mut(name) {
            ids.remove(&id);
            if ids.is_empty() {
                registry.remove(name);
            }
        }
    }
}

fn bulk_str(text: &'static str) -> Frame {
    Frame::Bulk(Bytes::from_static(text.as_bytes()))
}

/// One confirmation: `[kind, name, count]`, a push in RESP3.
fn confirmation(verb: &'static str, name: Option<&Bytes>, count: i64) -> Frame {
    Frame::Push(vec![
        bulk_str(verb),
        name.map_or(Frame::Null, |name| Frame::Bulk(name.clone())),
        Frame::Integer(count),
    ])
}

fn subscribe(ctx: &mut Ctx<'_>, args: &[Bytes], kind: Kind) -> Frame {
    let id = ctx.client.id;
    let mut replies = Vec::with_capacity(args.len() - 1);
    for name in &args[1..] {
        if kind
            .mine(&mut ctx.client.subscriptions)
            .insert(name.clone())
        {
            kind.register(&mut ctx.server.pubsub, name, id, true);
        }
        replies.push(confirmation(
            kind.subscribe(),
            Some(name),
            kind.count(&ctx.client.subscriptions),
        ));
    }
    Frame::Sequence(replies)
}

fn unsubscribe(ctx: &mut Ctx<'_>, args: &[Bytes], kind: Kind) -> Frame {
    let id = ctx.client.id;
    let names: Vec<Bytes> = if args.len() > 1 {
        args[1..].to_vec()
    } else {
        kind.mine(&mut ctx.client.subscriptions)
            .iter()
            .cloned()
            .collect()
    };
    if names.is_empty() {
        // Nothing to unsubscribe from still gets one answer.
        return Frame::Sequence(vec![confirmation(
            kind.unsubscribe(),
            None,
            kind.count(&ctx.client.subscriptions),
        )]);
    }
    let mut replies = Vec::with_capacity(names.len());
    for name in &names {
        if kind.mine(&mut ctx.client.subscriptions).remove(name) {
            kind.register(&mut ctx.server.pubsub, name, id, false);
        }
        replies.push(confirmation(
            kind.unsubscribe(),
            Some(name),
            kind.count(&ctx.client.subscriptions),
        ));
    }
    Frame::Sequence(replies)
}

/// `SUBSCRIBE channel [channel ...]`
pub(crate) fn subscribe_cmd(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    subscribe(ctx, args, Kind::Channel)
}

/// `PSUBSCRIBE pattern [pattern ...]`
pub(crate) fn psubscribe(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    subscribe(ctx, args, Kind::Pattern)
}

/// `SSUBSCRIBE shardchannel [shardchannel ...]`
pub(crate) fn ssubscribe(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    subscribe(ctx, args, Kind::Shard)
}

/// `UNSUBSCRIBE [channel ...]`
pub(crate) fn unsubscribe_cmd(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    unsubscribe(ctx, args, Kind::Channel)
}

/// `PUNSUBSCRIBE [pattern ...]`
pub(crate) fn punsubscribe(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    unsubscribe(ctx, args, Kind::Pattern)
}

/// `SUNSUBSCRIBE [shardchannel ...]`
pub(crate) fn sunsubscribe(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    unsubscribe(ctx, args, Kind::Shard)
}

/// Sends `message` on `channel` to every subscriber; returns how many
/// received it (a client subscribed to the channel and to a matching
/// pattern counts, and receives it, twice).
pub fn publish_message(server: &mut Server, channel: &Bytes, message: &Bytes, shard: bool) -> i64 {
    let mut receivers = 0;
    let registry = if shard {
        &server.pubsub.shard
    } else {
        &server.pubsub.channels
    };
    if let Some(ids) = registry.get(channel) {
        let verb = if shard { "smessage" } else { "message" };
        for &id in ids {
            server.pushes.push((
                id,
                Frame::Push(vec![
                    bulk_str(verb),
                    Frame::Bulk(channel.clone()),
                    Frame::Bulk(message.clone()),
                ]),
            ));
            receivers += 1;
        }
    }
    if !shard {
        for (pattern, ids) in &server.pubsub.patterns {
            if !glob_match(pattern, channel) {
                continue;
            }
            for &id in ids {
                server.pushes.push((
                    id,
                    Frame::Push(vec![
                        bulk_str("pmessage"),
                        Frame::Bulk(pattern.clone()),
                        Frame::Bulk(channel.clone()),
                        Frame::Bulk(message.clone()),
                    ]),
                ));
                receivers += 1;
            }
        }
    }
    receivers
}

/// `PUBLISH channel message`
pub(crate) fn publish(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    Frame::Integer(publish_message(ctx.server, &args[1], &args[2], false))
}

/// `SPUBLISH shardchannel message`
pub(crate) fn spublish(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    Frame::Integer(publish_message(ctx.server, &args[1], &args[2], true))
}

/// Ends every subscription of a client: it is disconnecting, or `RESET`.
pub fn forget(server: &mut Server, client: &mut ClientState) {
    let id = client.id;
    let subs = std::mem::take(&mut client.subscriptions);
    for (kind, names) in [
        (Kind::Channel, subs.channels),
        (Kind::Pattern, subs.patterns),
        (Kind::Shard, subs.shard),
    ] {
        for name in &names {
            kind.register(&mut server.pubsub, name, id, false);
        }
    }
}

/// `PING` from a RESP2 connection in subscribed mode: `[pong, message]`.
pub(crate) fn subscribed_ping(client: &ClientState, message: Option<&Bytes>) -> Option<Frame> {
    if client.proto != Protocol::Resp2 || !client.subscriptions.any() {
        return None;
    }
    Some(Frame::Array(vec![
        bulk_str("pong"),
        Frame::Bulk(message.cloned().unwrap_or_default()),
    ]))
}

/// Commands a RESP2 connection may still send once it has subscribed.
pub(crate) fn allowed_while_subscribed(name: &str) -> bool {
    matches!(
        name,
        "subscribe"
            | "psubscribe"
            | "ssubscribe"
            | "unsubscribe"
            | "punsubscribe"
            | "sunsubscribe"
            | "ping"
            | "quit"
            | "reset"
    )
}

pub(crate) fn subscribed_mode_error(name: &str) -> Frame {
    Frame::error(format!(
        "ERR Can't execute '{name}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / \
         RESET are allowed in this context"
    ))
}

fn names_matching<'a>(names: impl Iterator<Item = &'a Bytes>, pattern: Option<&Bytes>) -> Frame {
    Frame::Array(
        names
            .filter(|name| pattern.is_none_or(|pattern| glob_match(pattern, name)))
            .map(|name| Frame::Bulk(name.clone()))
            .collect(),
    )
}

fn subscriber_counts(registry: &Registry, names: &[Bytes]) -> Frame {
    let mut items = Vec::with_capacity(names.len() * 2);
    for name in names {
        items.push(Frame::Bulk(name.clone()));
        items.push(Frame::Integer(
            registry.get(name).map_or(0, |ids| ids.len() as i64),
        ));
    }
    Frame::Array(items)
}

const HELP: &[&str] = &[
    "PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "CHANNELS [<pattern>]",
    "    Return the currently active channels matching a <pattern> (default: '*').",
    "NUMPAT",
    "    Return number of subscriptions to patterns.",
    "NUMSUB [<channel> ...]",
    "    Return the number of subscribers for the specified channels, excluding",
    "    pattern subscriptions(default: no channels).",
    "SHARDCHANNELS [<pattern>]",
    "    Return the currently active shard level channels matching a <pattern> (default: '*').",
    "SHARDNUMSUB [<shardchannel> ...]",
    "    Return the number of subscribers for the specified shard level channel(s)",
    "HELP",
    "    Print this help.",
];

/// `PUBSUB CHANNELS | NUMSUB | NUMPAT | SHARDCHANNELS | SHARDNUMSUB | HELP`
pub(crate) fn pubsub(ctx: &mut Ctx<'_>, args: &[Bytes]) -> Frame {
    let sub = &args[1];
    let rest = &args[2..];
    let pubsub = &ctx.server.pubsub;
    if is(sub, "CHANNELS") && rest.len() <= 1 {
        names_matching(pubsub.channels.keys(), rest.first())
    } else if is(sub, "SHARDCHANNELS") && rest.len() <= 1 {
        names_matching(pubsub.shard.keys(), rest.first())
    } else if is(sub, "NUMSUB") {
        subscriber_counts(&pubsub.channels, rest)
    } else if is(sub, "SHARDNUMSUB") {
        subscriber_counts(&pubsub.shard, rest)
    } else if is(sub, "NUMPAT") && rest.is_empty() {
        Frame::Integer(pubsub.pattern_count() as i64)
    } else if is(sub, "HELP") && rest.is_empty() {
        Frame::Array(HELP.iter().map(|line| Frame::simple(*line)).collect())
    } else if is(sub, "CHANNELS") || is(sub, "SHARDCHANNELS") {
        Frame::error(format!(
            "ERR unknown subcommand or wrong number of arguments for '{}'. Try PUBSUB HELP.",
            lossy(sub)
        ))
    } else if is(sub, "NUMPAT") || is(sub, "HELP") {
        wrong_subcommand_arity("pubsub", sub)
    } else {
        unknown_subcommand(&lossy(&args[0]).to_ascii_uppercase(), sub)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{bulk, err, Harness};

    fn push(items: &[&str]) -> Frame {
        Frame::Push(items.iter().map(|item| bulk(item)).collect())
    }

    fn confirm(verb: &str, name: &str, count: i64) -> Frame {
        Frame::Push(vec![bulk(verb), bulk(name), Frame::Integer(count)])
    }

    /// Another client, id 8, subscribed to `channels`.
    fn subscriber(h: &mut Harness, channels: &[&str], patterns: &[&str]) {
        let mut other = ClientState::new(8, "127.0.0.1:50001".to_owned());
        for channel in channels {
            let name = Bytes::copy_from_slice(channel.as_bytes());
            other.subscriptions.channels.insert(name.clone());
            Kind::Channel.register(&mut h.server.pubsub, &name, 8, true);
        }
        for pattern in patterns {
            let name = Bytes::copy_from_slice(pattern.as_bytes());
            other.subscriptions.patterns.insert(name.clone());
            Kind::Pattern.register(&mut h.server.pubsub, &name, 8, true);
        }
        h.clients.insert(8, other);
    }

    #[test]
    fn subscribe_confirms_each_channel_with_a_running_count() {
        let mut h = Harness::new();
        assert_eq!(
            h.run("SUBSCRIBE a b a"),
            Frame::Sequence(vec![
                confirm("subscribe", "a", 1),
                confirm("subscribe", "b", 2),
                confirm("subscribe", "a", 2),
            ])
        );
        assert_eq!(
            h.run("PSUBSCRIBE p*"),
            Frame::Sequence(vec![confirm("psubscribe", "p*", 3)])
        );
        // Shard subscriptions are counted apart.
        assert_eq!(
            h.run("SSUBSCRIBE s"),
            Frame::Sequence(vec![confirm("ssubscribe", "s", 1)])
        );
        assert_eq!(h.server.pubsub.channel_count(), 2);
        assert_eq!(h.server.pubsub.pattern_count(), 1);
        assert_eq!(h.server.pubsub.shard_channel_count(), 1);
    }

    #[test]
    fn unsubscribe_without_arguments_leaves_everything_of_its_kind() {
        let mut h = Harness::new();
        h.run("SUBSCRIBE a b");
        h.run("PSUBSCRIBE p*");
        assert_eq!(
            h.run("UNSUBSCRIBE"),
            Frame::Sequence(vec![
                confirm("unsubscribe", "a", 2),
                confirm("unsubscribe", "b", 1),
            ])
        );
        assert_eq!(h.server.pubsub.channel_count(), 0);
        assert_eq!(
            h.run("PUNSUBSCRIBE nope"),
            Frame::Sequence(vec![confirm("punsubscribe", "nope", 1)])
        );
        assert_eq!(
            h.run("PUNSUBSCRIBE"),
            Frame::Sequence(vec![confirm("punsubscribe", "p*", 0)])
        );
        // Nothing left: still one answer, with a null name.
        assert_eq!(
            h.run("UNSUBSCRIBE"),
            Frame::Sequence(vec![Frame::Push(vec![
                bulk("unsubscribe"),
                Frame::Null,
                Frame::Integer(0)
            ])])
        );
        assert!(!h.client.subscriptions.any());
    }

    #[test]
    fn publish_reaches_channel_and_pattern_subscribers() {
        let mut h = Harness::new();
        subscriber(&mut h, &["news"], &["n*", "x*"]);
        assert_eq!(h.run("PUBLISH news hi"), Frame::Integer(2));
        assert_eq!(
            h.server.pushes,
            [
                (8, push(&["message", "news", "hi"])),
                (8, push(&["pmessage", "n*", "news", "hi"])),
            ]
        );
        h.server.pushes.clear();
        assert_eq!(h.run("PUBLISH other hi"), Frame::Integer(0));
        assert!(h.server.pushes.is_empty());
        // Shard channels are a world of their own.
        assert_eq!(h.run("SPUBLISH news hi"), Frame::Integer(0));
    }

    #[test]
    fn spublish_reaches_shard_subscribers_only() {
        let mut h = Harness::new();
        // In RESP3, so that a subscribed client may publish too.
        h.run("HELLO 3");
        h.run("SSUBSCRIBE s");
        subscriber(&mut h, &["s"], &["*"]);
        assert_eq!(h.run("SPUBLISH s m"), Frame::Integer(1));
        assert_eq!(h.server.pushes, [(7, push(&["smessage", "s", "m"]))]);
    }

    #[test]
    fn resp2_subscribed_mode_allows_only_a_few_commands() {
        let mut h = Harness::new();
        h.run("SUBSCRIBE a");
        assert_eq!(
            h.run("GET k"),
            err("ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context")
        );
        assert_eq!(h.run("PING"), Frame::Array(vec![bulk("pong"), bulk("")]));
        assert_eq!(
            h.run("PING hello"),
            Frame::Array(vec![bulk("pong"), bulk("hello")])
        );
        assert_eq!(
            h.run("CLIENT INFO"),
            err("ERR Can't execute 'client|info': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context")
        );
        h.run("UNSUBSCRIBE");
        assert_eq!(h.run("PING"), Frame::simple("PONG"));
        assert_eq!(h.run("GET k"), Frame::Null);
    }

    #[test]
    fn resp3_keeps_every_command() {
        let mut h = Harness::new();
        h.run("HELLO 3");
        h.run("SUBSCRIBE a");
        assert_eq!(h.run("GET k"), Frame::Null);
        assert_eq!(h.run("PING"), Frame::simple("PONG"));
    }

    #[test]
    fn reset_and_forget_end_all_subscriptions() {
        let mut h = Harness::new();
        h.run("SUBSCRIBE a");
        h.run("PSUBSCRIBE b*");
        h.run("SSUBSCRIBE c");
        assert_eq!(h.run("RESET"), Frame::simple("RESET"));
        assert!(!h.client.subscriptions.any());
        assert_eq!(h.server.pubsub.channel_count(), 0);
        assert_eq!(h.server.pubsub.pattern_count(), 0);
        assert_eq!(h.server.pubsub.shard_channel_count(), 0);
    }

    #[test]
    fn channel_rules_of_the_user_apply() {
        let mut h = Harness::new();
        h.run("ACL SETUSER alice on >pw +@all ~* resetchannels &news* &exact");
        assert_eq!(h.run("AUTH alice pw"), Frame::ok());
        h.run("HELLO 3");
        assert_eq!(h.run("PUBLISH news1 m"), Frame::Integer(0));
        assert_eq!(
            h.run("PUBLISH other m"),
            err("NOPERM No permissions to access a channel")
        );
        // One channel not allowed refuses the whole command.
        assert_eq!(
            h.run("SUBSCRIBE news1 other"),
            err("NOPERM No permissions to access a channel")
        );
        assert!(!h.client.subscriptions.any());
        // A pattern must be allowed as it is written.
        assert_eq!(
            h.run("PSUBSCRIBE news*"),
            Frame::Sequence(vec![confirm("psubscribe", "news*", 1)])
        );
        assert_eq!(
            h.run("PSUBSCRIBE n*"),
            err("NOPERM No permissions to access a channel")
        );
        assert_eq!(
            h.run("SSUBSCRIBE exact"),
            Frame::Sequence(vec![confirm("ssubscribe", "exact", 1)])
        );
    }

    #[test]
    fn losing_a_channel_disconnects_its_subscribers() {
        let mut h = Harness::new();
        h.run("ACL SETUSER alice on >pw +@all ~* resetchannels &news*");
        subscriber(&mut h, &["news1"], &[]);
        h.clients.get_mut(&8).unwrap().user = "alice".to_owned();
        // Still allowed: nothing happens.
        h.run("ACL SETUSER alice &other");
        assert!(h.server.effects.is_empty());
        h.run("ACL SETUSER alice resetchannels &other");
        assert_eq!(h.server.effects, [crate::Effect::Kill(8)]);
    }

    #[test]
    fn pubsub_introspection() {
        let mut h = Harness::new();
        subscriber(&mut h, &["news", "nope", "sport"], &["n*"]);
        h.run("HELLO 3");
        h.run("SUBSCRIBE news");
        h.run("UNSUBSCRIBE");
        h.run("SSUBSCRIBE shard1");
        assert_eq!(
            h.run("PUBSUB CHANNELS"),
            Frame::Array(vec![bulk("news"), bulk("nope"), bulk("sport")])
        );
        assert_eq!(
            h.run("PUBSUB CHANNELS n*"),
            Frame::Array(vec![bulk("news"), bulk("nope")])
        );
        assert_eq!(
            h.run("PUBSUB NUMSUB news missing"),
            Frame::Array(vec![
                bulk("news"),
                Frame::Integer(1),
                bulk("missing"),
                Frame::Integer(0)
            ])
        );
        assert_eq!(h.run("PUBSUB NUMSUB"), Frame::Array(vec![]));
        assert_eq!(h.run("PUBSUB NUMPAT"), Frame::Integer(1));
        assert_eq!(
            h.run("PUBSUB SHARDCHANNELS"),
            Frame::Array(vec![bulk("shard1")])
        );
        assert_eq!(
            h.run("PUBSUB SHARDNUMSUB shard1"),
            Frame::Array(vec![bulk("shard1"), Frame::Integer(1)])
        );
        assert_eq!(
            h.run("PUBSUB NUMPAT x"),
            err("ERR wrong number of arguments for 'pubsub|numpat' command")
        );
        assert_eq!(
            h.run("PUBSUB CHANNELS a b"),
            err("ERR unknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP.")
        );
        assert_eq!(
            h.run("PUBSUB FOO"),
            err("ERR unknown subcommand 'FOO'. Try PUBSUB HELP.")
        );
    }
}
