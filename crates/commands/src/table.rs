//! The command table. One line per command: name, arity, flags, legacy key
//! positions `(first, last, step)`, key specifications, group, version
//! introduced, summary and handler.
//!
//! The key specifications are Redis 7's (see [`crate::keyspec`]); the
//! legacy positions are what `COMMAND INFO` also reports for clients that
//! predate them. Both, and the flags, are checked against Redis 7.4 by
//! `tools/compare-command-info.py`. `COMMAND INFO` adds `movablekeys` to the
//! flags where the key specifications call for it.

use crate::keyspec::{
    at, keynum, range, spec, BeginSearch, FindKeys, KeySpec, KeySpecs, ACCESS, DELETE, INSERT,
    NOT_KEY, ONE, OW, REST, RM, RO, RW, UPDATE, VARIABLE_FLAGS,
};
use crate::{
    acl, admin, aof, bitmaps, clients, connection, hashes, introspection, keys, lcs, lists, object,
    persistence, pubsub, scripting, sets, sort, strings, transactions, zsets, CommandSpec, Handler,
};

type Flags = &'static [&'static str];

const NO_KEYS: (i64, i64, i64) = (0, 0, 0);
/// A single key as the first argument.
const KEY1: (i64, i64, i64) = (1, 1, 1);
/// Every argument is a key.
const ALL_KEYS: (i64, i64, i64) = (1, -1, 1);

const NONE: KeySpecs = &[];
const RO_1: KeySpecs = &[spec(RO, at(1), ONE)];
const RO_ACCESS_1: KeySpecs = &[spec(RO | ACCESS, at(1), ONE)];
const RW_UPDATE_1: KeySpecs = &[spec(RW | UPDATE, at(1), ONE)];
const RW_ACCESS_UPDATE_1: KeySpecs = &[spec(RW | ACCESS | UPDATE, at(1), ONE)];
const RW_INSERT_1: KeySpecs = &[spec(RW | INSERT, at(1), ONE)];
const RW_DELETE_1: KeySpecs = &[spec(RW | DELETE, at(1), ONE)];
const RW_ACCESS_DELETE_1: KeySpecs = &[spec(RW | ACCESS | DELETE, at(1), ONE)];
const OW_UPDATE_1: KeySpecs = &[spec(OW | UPDATE, at(1), ONE)];
const OW_INSERT_1: KeySpecs = &[spec(OW | INSERT, at(1), ONE)];
const RO_ALL: KeySpecs = &[spec(RO, at(1), REST)];
const RO_ACCESS_ALL: KeySpecs = &[spec(RO | ACCESS, at(1), REST)];
const RM_DELETE_ALL: KeySpecs = &[spec(RM | DELETE, at(1), REST)];
/// Pops from the first key and pushes to the second.
const POP_PUSH: KeySpecs = &[
    spec(RW | ACCESS | DELETE, at(1), ONE),
    spec(RW | INSERT, at(2), ONE),
];
/// Every argument but the last, the timeout.
const BLOCKING_POP: KeySpecs = &[spec(RW | ACCESS | DELETE, at(1), range(-2, 1, 0))];
/// `numkeys key [key ...]`.
const COUNTED_POP: KeySpecs = &[spec(RW | ACCESS | DELETE, at(1), keynum(0, 1, 1))];
/// `timeout numkeys key [key ...]`.
const BLOCKING_COUNTED_POP: KeySpecs = &[spec(RW | ACCESS | DELETE, at(2), keynum(0, 1, 1))];
const COUNTED_RO_ACCESS: KeySpecs = &[spec(RO | ACCESS, at(1), keynum(0, 1, 1))];
/// `destination key [key ...]`.
const STORE_FROM_ALL: KeySpecs = &[
    spec(OW | UPDATE, at(1), ONE),
    spec(RO | ACCESS, at(2), REST),
];
/// `destination numkeys key [key ...]`.
const STORE_FROM_COUNTED: KeySpecs = &[
    spec(OW | UPDATE, at(1), ONE),
    spec(RO | ACCESS, at(2), keynum(0, 1, 1)),
];
/// `script numkeys key [key ...] arg ...`.
const SCRIPT_KEYS: KeySpecs = &[spec(RW | ACCESS | UPDATE, at(2), keynum(0, 1, 1))
    .notes("We cannot tell how the keys will be used so we assume the worst, RW and UPDATE")];
const SCRIPT_KEYS_RO: KeySpecs = &[spec(RO | ACCESS, at(2), keynum(0, 1, 1))
    .notes("We cannot tell how the keys will be used so we assume the worst, RO and ACCESS")];
/// `BY` and `GET` patterns make key names out of the sorted values.
const SORT_BY_GET: KeySpec = spec(RO | ACCESS, BeginSearch::Unknown, FindKeys::Unknown).notes(
    "For the optional BY/GET keyword. It is marked 'unknown' because the key names derive from \
     the content of the key we sort",
);
const SORT_RO_SPECS: KeySpecs = &[spec(RO | ACCESS, at(1), ONE), SORT_BY_GET];
const SORT_SPECS: KeySpecs = &[
    spec(RO | ACCESS, at(1), ONE),
    SORT_BY_GET,
    spec(OW | UPDATE, BeginSearch::Unknown, FindKeys::Unknown).notes(
        "For the optional STORE keyword. It is marked 'unknown' because the keyword can appear \
         anywhere in the argument array",
    ),
];

const READ: Flags = &["readonly"];
const READ_FAST: Flags = &["readonly", "fast"];
const WRITE: Flags = &["write"];
const WRITE_FAST: Flags = &["write", "fast"];
const WRITE_OOM: Flags = &["write", "denyoom"];
const WRITE_OOM_FAST: Flags = &["write", "denyoom", "fast"];
const TRANSACTION: Flags = &["noscript", "loading", "stale", "fast", "allow_busy"];
const SCRIPT_RUN: Flags = &["noscript", "stale", "skip_monitor", "no_mandatory_keys"];
const SCRIPT_RUN_RO: Flags = &[
    "readonly",
    "noscript",
    "stale",
    "skip_monitor",
    "no_mandatory_keys",
];
const PUBSUB_SUBSCRIBE: Flags = &["pubsub", "noscript", "loading", "stale"];
const PUBSUB_PUBLISH: Flags = &["pubsub", "loading", "stale", "fast"];
const WRITE_BLOCKING: Flags = &["write", "blocking"];
const WRITE_FAST_BLOCKING: Flags = &["write", "fast", "blocking"];
const WRITE_OOM_BLOCKING: Flags = &["write", "denyoom", "blocking"];

#[allow(clippy::too_many_arguments)]
const fn cmd(
    name: &'static str,
    arity: i32,
    flags: Flags,
    keys: (i64, i64, i64),
    key_specs: KeySpecs,
    group: &'static str,
    since: &'static str,
    summary: &'static str,
    handler: Handler,
) -> CommandSpec {
    CommandSpec {
        name,
        arity,
        flags,
        first_key: keys.0,
        last_key: keys.1,
        key_step: keys.2,
        key_specs,
        group,
        since,
        summary,
        handler,
    }
}

/// Every command the server understands.
#[rustfmt::skip]
pub static COMMANDS: &[CommandSpec] = &[
    // --- connection -------------------------------------------------------
    cmd("ping", -1, &["fast"], NO_KEYS, NONE, "connection", "1.0.0", "Returns the server's liveliness response.", connection::ping),
    cmd("echo", 2, &["loading", "stale", "fast"], NO_KEYS, NONE, "connection", "1.0.0", "Returns the given string.", connection::echo),
    cmd("auth", -2, &["noscript", "loading", "stale", "fast", "no_auth", "allow_busy"], NO_KEYS, NONE, "connection", "1.0.0", "Authenticates the connection.", acl::auth),
    cmd("acl", -2, &["noscript", "loading", "stale", "skip_slowlog"], NO_KEYS, NONE, "server", "6.0.0", "A container for Access List Control commands.", acl::acl),
    cmd("hello", -1, &["noscript", "loading", "stale", "fast", "no_auth", "allow_busy"], NO_KEYS, NONE, "connection", "6.0.0", "Handshakes with the server.", connection::hello),
    cmd("select", 2, &["loading", "stale", "fast"], NO_KEYS, NONE, "connection", "1.0.0", "Changes the selected database.", connection::select),
    cmd("quit", -1, &["noscript", "loading", "stale", "fast", "no_auth", "allow_busy"], NO_KEYS, NONE, "connection", "1.0.0", "Closes the connection.", connection::quit),
    cmd("client", -2, &["noscript", "loading", "stale"], NO_KEYS, NONE, "connection", "2.4.0", "A container for client connection commands.", connection::client),

    // --- server -----------------------------------------------------------
    cmd("command", -1, &["loading", "stale"], NO_KEYS, NONE, "server", "2.8.13", "Returns detailed information about all commands.", introspection::command),
    cmd("dbsize", 1, READ_FAST, NO_KEYS, NONE, "server", "1.0.0", "Returns the number of keys in the currently selected database.", keys::dbsize),
    cmd("flushdb", -1, WRITE, NO_KEYS, NONE, "server", "1.0.0", "Removes all keys from the current database.", keys::flushdb),
    cmd("flushall", -1, WRITE, NO_KEYS, NONE, "server", "1.0.0", "Removes all keys from all databases.", keys::flushall),

    // --- generic ----------------------------------------------------------
    cmd("del", -2, WRITE, ALL_KEYS, RM_DELETE_ALL, "generic", "1.0.0", "Deletes one or more keys.", keys::del),
    cmd("unlink", -2, WRITE_FAST, ALL_KEYS, RM_DELETE_ALL, "generic", "4.0.0", "Asynchronously deletes one or more keys.", keys::unlink),
    cmd("exists", -2, READ_FAST, ALL_KEYS, RO_ALL, "generic", "1.0.0", "Determines whether one or more keys exist.", keys::exists),
    cmd("touch", -2, READ_FAST, ALL_KEYS, RO_ALL, "generic", "3.2.1", "Returns the number of existing keys out of those specified after updating the time they were last accessed.", keys::exists),
    cmd("type", 2, READ_FAST, KEY1, RO_1, "generic", "1.0.0", "Determines the type of value stored at a key.", keys::type_),
    cmd("rename", 3, WRITE, (1, 2, 1), &[spec(RW | ACCESS | DELETE, at(1), ONE), spec(OW | UPDATE, at(2), ONE)], "generic", "1.0.0", "Renames a key and overwrites the destination.", keys::rename),
    cmd("renamenx", 3, WRITE_FAST, (1, 2, 1), &[spec(RW | ACCESS | DELETE, at(1), ONE), spec(OW | INSERT, at(2), ONE)], "generic", "1.0.0", "Renames a key only when the target key name doesn't exist.", keys::renamenx),
    cmd("copy", -3, WRITE_OOM, (1, 2, 1), &[spec(RO | ACCESS, at(1), ONE), spec(OW | UPDATE, at(2), ONE)], "generic", "6.2.0", "Copies the value of a key to a new key.", keys::copy),
    cmd("move", 3, WRITE_FAST, KEY1, RW_ACCESS_UPDATE_1, "generic", "1.0.0", "Moves a key to another database.", keys::move_key),
    cmd("swapdb", 3, WRITE_FAST, NO_KEYS, NONE, "server", "4.0.0", "Swaps two Redis databases.", keys::swapdb),
    cmd("keys", 2, READ, NO_KEYS, NONE, "generic", "1.0.0", "Returns all key names that match a pattern.", keys::keys),
    cmd("scan", -2, READ, NO_KEYS, NONE, "generic", "2.8.0", "Iterates over the key names in the database.", keys::scan),
    cmd("randomkey", 1, READ, NO_KEYS, NONE, "generic", "1.0.0", "Returns a random key name from the database.", keys::randomkey),
    cmd("expire", -3, WRITE_FAST, KEY1, RW_UPDATE_1, "generic", "1.0.0", "Sets the expiration time of a key in seconds.", keys::expire),
    cmd("pexpire", -3, WRITE_FAST, KEY1, RW_UPDATE_1, "generic", "2.6.0", "Sets the expiration time of a key in milliseconds.", keys::pexpire),
    cmd("expireat", -3, WRITE_FAST, KEY1, RW_UPDATE_1, "generic", "1.2.0", "Sets the expiration time of a key to a Unix timestamp.", keys::expireat),
    cmd("pexpireat", -3, WRITE_FAST, KEY1, RW_UPDATE_1, "generic", "2.6.0", "Sets the expiration time of a key to a Unix milliseconds timestamp.", keys::pexpireat),
    cmd("ttl", 2, READ_FAST, KEY1, RO_ACCESS_1, "generic", "1.0.0", "Returns the expiration time in seconds of a key.", keys::ttl),
    cmd("pttl", 2, READ_FAST, KEY1, RO_ACCESS_1, "generic", "2.6.0", "Returns the expiration time in milliseconds of a key.", keys::pttl),
    cmd("expiretime", 2, READ_FAST, KEY1, RO_ACCESS_1, "generic", "7.0.0", "Returns the expiration time of a key as a Unix timestamp.", keys::expiretime),
    cmd("pexpiretime", 2, READ_FAST, KEY1, RO_ACCESS_1, "generic", "7.0.0", "Returns the expiration time of a key as a Unix milliseconds timestamp.", keys::pexpiretime),
    cmd("persist", 2, WRITE_FAST, KEY1, RW_UPDATE_1, "generic", "2.2.0", "Removes the expiration time of a key.", keys::persist),
    cmd("object", -2, &["slow"], NO_KEYS, NONE, "generic", "2.2.3", "A container for object introspection commands.", object::object),

    // --- string -----------------------------------------------------------
    cmd("get", 2, READ_FAST, KEY1, RO_ACCESS_1, "string", "1.0.0", "Returns the string value of a key.", strings::get),
    cmd("set", -3, WRITE_OOM, KEY1, &[spec(RW | ACCESS | UPDATE | VARIABLE_FLAGS, at(1), ONE).notes("RW and ACCESS due to the optional `GET` argument")], "string", "1.0.0", "Sets the string value of a key, ignoring its type. The key is created if it doesn't exist.", strings::set),
    cmd("setnx", 3, WRITE_OOM_FAST, KEY1, OW_INSERT_1, "string", "1.0.0", "Set the string value of a key only when the key doesn't exist.", strings::setnx),
    cmd("setex", 4, WRITE_OOM, KEY1, OW_UPDATE_1, "string", "2.0.0", "Sets the string value and expiration time of a key. Creates the key if it doesn't exist.", strings::setex),
    cmd("psetex", 4, WRITE_OOM, KEY1, OW_UPDATE_1, "string", "2.6.0", "Sets both string value and expiration time in milliseconds of a key. The key is created if it doesn't exist.", strings::psetex),
    cmd("getset", 3, WRITE_OOM_FAST, KEY1, RW_ACCESS_UPDATE_1, "string", "1.0.0", "Returns the previous string value of a key after setting it to a new value.", strings::getset),
    cmd("getdel", 2, WRITE_FAST, KEY1, RW_ACCESS_DELETE_1, "string", "6.2.0", "Returns the string value of a key after deleting the key.", strings::getdel),
    cmd("getex", -2, WRITE_FAST, KEY1, &[spec(RW | ACCESS | UPDATE, at(1), ONE).notes("RW and UPDATE because it changes the TTL")], "string", "6.2.0", "Returns the string value of a key after setting its expiration time.", strings::getex),
    cmd("mget", -2, READ_FAST, ALL_KEYS, RO_ACCESS_ALL, "string", "1.0.0", "Atomically returns the string values of one or more keys.", strings::mget),
    cmd("mset", -3, WRITE_OOM, (1, -1, 2), &[spec(OW | UPDATE, at(1), range(-1, 2, 0))], "string", "1.0.1", "Atomically creates or modifies the string values of one or more keys.", strings::mset),
    cmd("msetnx", -3, WRITE_OOM, (1, -1, 2), &[spec(OW | INSERT, at(1), range(-1, 2, 0))], "string", "1.0.1", "Atomically modifies the string values of one or more keys only when all keys don't exist.", strings::msetnx),
    cmd("incr", 2, WRITE_OOM_FAST, KEY1, RW_ACCESS_UPDATE_1, "string", "1.0.0", "Increments the integer value of a key by one. Uses 0 as initial value if the key doesn't exist.", strings::incr),
    cmd("decr", 2, WRITE_OOM_FAST, KEY1, RW_ACCESS_UPDATE_1, "string", "1.0.0", "Decrements the integer value of a key by one. Uses 0 as initial value if the key doesn't exist.", strings::decr),
    cmd("incrby", 3, WRITE_OOM_FAST, KEY1, RW_ACCESS_UPDATE_1, "string", "1.0.0", "Increments the integer value of a key by a number. Uses 0 as initial value if the key doesn't exist.", strings::incrby),
    cmd("decrby", 3, WRITE_OOM_FAST, KEY1, RW_ACCESS_UPDATE_1, "string", "1.0.0", "Decrements a number from the integer value of a key. Uses 0 as initial value if the key doesn't exist.", strings::decrby),
    cmd("incrbyfloat", 3, WRITE_OOM_FAST, KEY1, RW_ACCESS_UPDATE_1, "string", "2.6.0", "Increment the floating point value of a key by a number. Uses 0 as initial value if the key doesn't exist.", strings::incrbyfloat),
    cmd("append", 3, WRITE_OOM_FAST, KEY1, RW_INSERT_1, "string", "2.0.0", "Appends a string to the value of a key. Creates the key if it doesn't exist.", strings::append),
    cmd("strlen", 2, READ_FAST, KEY1, RO_1, "string", "2.2.0", "Returns the length of a string value.", strings::strlen),
    cmd("getrange", 4, READ, KEY1, RO_ACCESS_1, "string", "2.4.0", "Returns a substring of the string stored at a key.", strings::getrange),
    cmd("substr", 4, READ, KEY1, RO_ACCESS_1, "string", "1.0.0", "Returns a substring from a string value.", strings::getrange),
    cmd("setrange", 4, WRITE_OOM, KEY1, RW_UPDATE_1, "string", "2.2.0", "Overwrites a part of a string value with another by an offset. Creates the key if it doesn't exist.", strings::setrange),
    cmd("lcs", -3, READ, (1, 2, 1), &[spec(RO | ACCESS, at(1), range(1, 1, 0))], "string", "7.0.0", "Finds the longest common substring.", lcs::lcs),

    // --- bitmap -----------------------------------------------------------
    cmd("setbit", 4, WRITE_OOM, KEY1, RW_ACCESS_UPDATE_1, "bitmap", "2.2.0", "Sets or clears the bit at offset of the string value. Creates the key if it doesn't exist.", bitmaps::setbit),
    cmd("getbit", 3, READ_FAST, KEY1, RO_ACCESS_1, "bitmap", "2.2.0", "Returns a bit value by offset.", bitmaps::getbit),
    cmd("bitcount", -2, READ, KEY1, RO_ACCESS_1, "bitmap", "2.6.0", "Counts the number of set bits (population counting) in a string.", bitmaps::bitcount),
    cmd("bitpos", -3, READ, KEY1, RO_ACCESS_1, "bitmap", "2.8.7", "Finds the first set (1) or clear (0) bit in a string.", bitmaps::bitpos),
    cmd("bitop", -4, WRITE_OOM, (2, -1, 1), &[spec(OW | UPDATE, at(2), ONE), spec(RO | ACCESS, at(3), REST)], "bitmap", "2.6.0", "Performs bitwise operations on multiple strings, and stores the result.", bitmaps::bitop),
    cmd("bitfield", -2, WRITE_OOM, KEY1, &[spec(RW | ACCESS | UPDATE | VARIABLE_FLAGS, at(1), ONE).notes("This command allows both access and modification of the key")], "bitmap", "3.2.0", "Performs arbitrary bitfield integer operations on strings.", bitmaps::bitfield),
    cmd("bitfield_ro", -2, READ_FAST, KEY1, RO_ACCESS_1, "bitmap", "6.0.0", "Performs arbitrary read-only bitfield integer operations on strings.", bitmaps::bitfield_ro),

    // --- hash -------------------------------------------------------------
    cmd("hset", -4, WRITE_OOM_FAST, KEY1, RW_UPDATE_1, "hash", "2.0.0", "Creates or modifies the value of a field in a hash.", hashes::hset),
    cmd("hmset", -4, WRITE_OOM_FAST, KEY1, RW_UPDATE_1, "hash", "2.0.0", "Sets the values of multiple fields.", hashes::hmset),
    cmd("hsetnx", 4, WRITE_OOM_FAST, KEY1, RW_INSERT_1, "hash", "2.0.0", "Sets the value of a field in a hash only when the field doesn't exist.", hashes::hsetnx),
    cmd("hget", 3, READ_FAST, KEY1, RO_ACCESS_1, "hash", "2.0.0", "Returns the value of a field in a hash.", hashes::hget),
    cmd("hmget", -3, READ_FAST, KEY1, RO_ACCESS_1, "hash", "2.0.0", "Returns the values of all fields in a hash.", hashes::hmget),
    cmd("hdel", -3, WRITE_FAST, KEY1, RW_DELETE_1, "hash", "2.0.0", "Deletes one or more fields and their values from a hash. Deletes the hash if no fields remain.", hashes::hdel),
    cmd("hgetall", 2, READ, KEY1, RO_ACCESS_1, "hash", "2.0.0", "Returns all fields and values in a hash.", hashes::hgetall),
    cmd("hkeys", 2, READ, KEY1, RO_ACCESS_1, "hash", "2.0.0", "Returns all fields in a hash.", hashes::hkeys),
    cmd("hvals", 2, READ, KEY1, RO_ACCESS_1, "hash", "2.0.0", "Returns all values in a hash.", hashes::hvals),
    cmd("hlen", 2, READ_FAST, KEY1, RO_1, "hash", "2.0.0", "Returns the number of fields in a hash.", hashes::hlen),
    cmd("hexists", 3, READ_FAST, KEY1, RO_1, "hash", "2.0.0", "Determines whether a field exists in a hash.", hashes::hexists),
    cmd("hstrlen", 3, READ_FAST, KEY1, RO_1, "hash", "3.2.0", "Returns the length of the value of a field.", hashes::hstrlen),
    cmd("hincrby", 4, WRITE_OOM_FAST, KEY1, RW_ACCESS_UPDATE_1, "hash", "2.0.0", "Increments the integer value of a field in a hash by a number. Uses 0 as initial value if the field doesn't exist.", hashes::hincrby),
    cmd("hincrbyfloat", 4, WRITE_OOM_FAST, KEY1, RW_ACCESS_UPDATE_1, "hash", "2.6.0", "Increments the floating point value of a field by a number. Uses 0 as initial value if the field doesn't exist.", hashes::hincrbyfloat),
    cmd("hscan", -3, READ, KEY1, RO_ACCESS_1, "hash", "2.8.0", "Iterates over fields and values of a hash.", hashes::hscan),
    cmd("hrandfield", -2, READ, KEY1, RO_ACCESS_1, "hash", "6.2.0", "Returns one or more random fields from a hash.", hashes::hrandfield),
    cmd("hexpire", -6, WRITE_OOM_FAST, KEY1, RW_UPDATE_1, "hash", "7.4.0", "Set expiry for hash field using relative time to expire (seconds)", hashes::hexpire),
    cmd("hpexpire", -6, WRITE_OOM_FAST, KEY1, RW_UPDATE_1, "hash", "7.4.0", "Set expiry for hash field using relative time to expire (milliseconds)", hashes::hpexpire),
    cmd("hexpireat", -6, WRITE_OOM_FAST, KEY1, RW_UPDATE_1, "hash", "7.4.0", "Set expiry for hash field using an absolute Unix timestamp (seconds)", hashes::hexpireat),
    cmd("hpexpireat", -6, WRITE_OOM_FAST, KEY1, RW_UPDATE_1, "hash", "7.4.0", "Set expiry for hash field using an absolute Unix timestamp (milliseconds)", hashes::hpexpireat),
    cmd("httl", -5, READ_FAST, KEY1, RO_ACCESS_1, "hash", "7.4.0", "Returns the TTL in seconds of a hash field.", hashes::httl),
    cmd("hpttl", -5, READ_FAST, KEY1, RO_ACCESS_1, "hash", "7.4.0", "Returns the TTL in milliseconds of a hash field.", hashes::hpttl),
    cmd("hexpiretime", -5, READ_FAST, KEY1, RO_ACCESS_1, "hash", "7.4.0", "Returns the expiration time of a hash field as a Unix timestamp, in seconds.", hashes::hexpiretime),
    cmd("hpexpiretime", -5, READ_FAST, KEY1, RO_ACCESS_1, "hash", "7.4.0", "Returns the expiration time of a hash field as a Unix timestamp, in msec.", hashes::hpexpiretime),
    cmd("hpersist", -5, WRITE_FAST, KEY1, RW_UPDATE_1, "hash", "7.4.0", "Removes the expiration time for each specified field", hashes::hpersist),

    // --- list -------------------------------------------------------------
    cmd("lpush", -3, WRITE_OOM_FAST, KEY1, RW_INSERT_1, "list", "1.0.0", "Prepends one or more elements to a list. Creates the key if it doesn't exist.", lists::lpush),
    cmd("rpush", -3, WRITE_OOM_FAST, KEY1, RW_INSERT_1, "list", "1.0.0", "Appends one or more elements to a list. Creates the key if it doesn't exist.", lists::rpush),
    cmd("lpushx", -3, WRITE_OOM_FAST, KEY1, RW_INSERT_1, "list", "2.2.0", "Prepends one or more elements to a list only when the list exists.", lists::lpushx),
    cmd("rpushx", -3, WRITE_OOM_FAST, KEY1, RW_INSERT_1, "list", "2.2.0", "Appends an element to a list only when the list exists.", lists::rpushx),
    cmd("lpop", -2, WRITE_FAST, KEY1, RW_ACCESS_DELETE_1, "list", "1.0.0", "Returns the first elements in a list after removing it. Deletes the list if the last element was popped.", lists::lpop),
    cmd("rpop", -2, WRITE_FAST, KEY1, RW_ACCESS_DELETE_1, "list", "1.0.0", "Returns and removes the last elements of a list. Deletes the list if the last element was popped.", lists::rpop),
    cmd("lmpop", -4, WRITE, NO_KEYS, COUNTED_POP, "list", "7.0.0", "Returns multiple elements from a list after removing them. Deletes the list if the last element was popped.", lists::lmpop),
    cmd("lmove", 5, WRITE_OOM, (1, 2, 1), POP_PUSH, "list", "6.2.0", "Returns an element after popping it from one list and pushing it to another. Deletes the list if the last element was moved.", lists::lmove),
    cmd("rpoplpush", 3, WRITE_OOM, (1, 2, 1), POP_PUSH, "list", "1.2.0", "Returns the last element of a list after removing and pushing it to another list. Deletes the list if the last element was popped.", lists::rpoplpush),
    cmd("blpop", -3, WRITE_BLOCKING, (1, -2, 1), BLOCKING_POP, "list", "2.0.0", "Removes and returns the first element in a list. Blocks until an element is available otherwise. Deletes the list if the last element was popped.", lists::blpop),
    cmd("brpop", -3, WRITE_BLOCKING, (1, -2, 1), BLOCKING_POP, "list", "2.0.0", "Removes and returns the last element in a list. Blocks until an element is available otherwise. Deletes the list if the last element was popped.", lists::brpop),
    cmd("blmpop", -5, WRITE_BLOCKING, NO_KEYS, BLOCKING_COUNTED_POP, "list", "7.0.0", "Pops the first element from one of multiple lists. Blocks until an element is available otherwise. Deletes the list if the last element was popped.", lists::blmpop),
    cmd("blmove", 6, WRITE_OOM_BLOCKING, (1, 2, 1), POP_PUSH, "list", "6.2.0", "Pops an element from a list, pushes it to another list and returns it. Blocks until an element is available otherwise. Deletes the list if the last element was moved.", lists::blmove),
    cmd("brpoplpush", 4, WRITE_OOM_BLOCKING, (1, 2, 1), POP_PUSH, "list", "2.2.0", "Pops an element from a list, pushes it to another list and returns it. Block until an element is available otherwise. Deletes the list if the last element was popped.", lists::brpoplpush),
    cmd("llen", 2, READ_FAST, KEY1, RO_1, "list", "1.0.0", "Returns the length of a list.", lists::llen),
    cmd("lrange", 4, READ, KEY1, RO_ACCESS_1, "list", "1.0.0", "Returns a range of elements from a list.", lists::lrange),
    cmd("lindex", 3, READ, KEY1, RO_ACCESS_1, "list", "1.0.0", "Returns an element from a list by its index.", lists::lindex),
    cmd("lpos", -3, READ, KEY1, RO_ACCESS_1, "list", "6.0.6", "Returns the index of matching elements in a list.", lists::lpos),
    cmd("lset", 4, WRITE_OOM, KEY1, RW_UPDATE_1, "list", "1.0.0", "Sets the value of an element in a list by its index.", lists::lset),
    cmd("linsert", 5, WRITE_OOM, KEY1, RW_INSERT_1, "list", "2.2.0", "Inserts an element before or after another element in a list.", lists::linsert),
    cmd("lrem", 4, WRITE, KEY1, RW_DELETE_1, "list", "1.0.0", "Removes elements from a list. Deletes the list if the last element was removed.", lists::lrem),
    cmd("ltrim", 4, WRITE, KEY1, RW_DELETE_1, "list", "1.0.0", "Removes elements from both ends a list. Deletes the list if all elements were trimmed.", lists::ltrim),

    cmd("sort", -2, WRITE_OOM, KEY1, SORT_SPECS, "generic", "1.0.0", "Sorts the elements in a list, a set, or a sorted set, optionally storing the result.", sort::sort),
    cmd("sort_ro", -2, READ, KEY1, SORT_RO_SPECS, "generic", "7.0.0", "Returns the sorted elements of a list, a set, or a sorted set.", sort::sort_ro),

    // --- admin ------------------------------------------------------------
    cmd("info", -1, &["loading", "stale"], NO_KEYS, NONE, "server", "1.0.0", "Returns information and statistics about the server.", admin::info),
    cmd("config", -2, &["admin", "noscript", "loading", "stale"], NO_KEYS, NONE, "server", "2.0.0", "A container for server configuration commands.", admin::config),
    cmd("slowlog", -2, &["admin", "loading", "stale"], NO_KEYS, NONE, "server", "2.2.12", "A container for slow log commands.", admin::slowlog),
    cmd("memory", -2, READ, NO_KEYS, NONE, "server", "4.0.0", "A container for memory diagnostics commands.", admin::memory),
    cmd("time", 1, &["loading", "stale", "fast"], NO_KEYS, NONE, "server", "2.6.0", "Returns the server time.", admin::time),
    // --- pub/sub ----------------------------------------------------------
    cmd("subscribe", -2, PUBSUB_SUBSCRIBE, NO_KEYS, NONE, "pubsub", "2.0.0", "Listens for messages published to channels.", pubsub::subscribe_cmd),
    cmd("unsubscribe", -1, PUBSUB_SUBSCRIBE, NO_KEYS, NONE, "pubsub", "2.0.0", "Stops listening to messages posted to channels.", pubsub::unsubscribe_cmd),
    cmd("psubscribe", -2, PUBSUB_SUBSCRIBE, NO_KEYS, NONE, "pubsub", "2.0.0", "Listens for messages published to channels that match one or more patterns.", pubsub::psubscribe),
    cmd("punsubscribe", -1, PUBSUB_SUBSCRIBE, NO_KEYS, NONE, "pubsub", "2.0.0", "Stops listening to messages published to channels that match one or more patterns.", pubsub::punsubscribe),
    cmd("ssubscribe", -2, PUBSUB_SUBSCRIBE, ALL_KEYS, &[spec(NOT_KEY, at(1), REST)], "pubsub", "7.0.0", "Listens for messages published to shard channels.", pubsub::ssubscribe),
    cmd("sunsubscribe", -1, PUBSUB_SUBSCRIBE, ALL_KEYS, &[spec(NOT_KEY, at(1), REST)], "pubsub", "7.0.0", "Stops listening to messages posted to shard channels.", pubsub::sunsubscribe),
    cmd("publish", 3, PUBSUB_PUBLISH, NO_KEYS, NONE, "pubsub", "2.0.0", "Posts a message to a channel.", pubsub::publish),
    cmd("spublish", 3, PUBSUB_PUBLISH, KEY1, &[spec(NOT_KEY, at(1), ONE)], "pubsub", "7.0.0", "Post a message to a shard channel", pubsub::spublish),
    cmd("pubsub", -2, &[], NO_KEYS, NONE, "pubsub", "2.8.0", "A container for Pub/Sub commands.", pubsub::pubsub),
    cmd("debug", -2, &["admin", "noscript", "loading", "stale"], NO_KEYS, NONE, "server", "1.0.0", "A container for debugging commands.", admin::debug),
    cmd("reset", 1, &["noscript", "loading", "stale", "fast", "no_auth", "allow_busy"], NO_KEYS, NONE, "connection", "6.2.0", "Resets the connection.", clients::reset),

    // --- persistence ------------------------------------------------------
    cmd("save", 1, &["admin", "noscript", "no_async_loading", "no_multi"], NO_KEYS, NONE, "server", "1.0.0", "Synchronously saves the database(s) to disk.", persistence::save_command),
    cmd("bgsave", -1, &["admin", "noscript", "no_async_loading", "no_multi"], NO_KEYS, NONE, "server", "1.0.0", "Asynchronously saves the database(s) to disk.", persistence::bgsave),
    cmd("bgrewriteaof", 1, &["admin", "noscript", "no_async_loading", "no_multi"], NO_KEYS, NONE, "server", "1.0.0", "Asynchronously rewrites the append-only file to disk.", aof::bgrewriteaof),
    cmd("lastsave", 1, &["loading", "stale", "fast"], NO_KEYS, NONE, "server", "1.0.0", "Returns the Unix timestamp of the last successful save to disk.", persistence::lastsave),
    cmd("shutdown", -1, &["admin", "noscript", "loading", "stale", "no_multi", "allow_busy"], NO_KEYS, NONE, "server", "1.0.0", "Synchronously saves the database(s) to disk and shuts down the server.", persistence::shutdown),
    cmd("dump", 2, READ, KEY1, RO_ACCESS_1, "generic", "2.6.0", "Returns a serialized representation of the value stored at a key.", persistence::dump),
    cmd("restore", -4, WRITE_OOM, KEY1, OW_UPDATE_1, "generic", "2.6.0", "Creates a key from the serialized representation of a value.", persistence::restore),

    // --- scripting --------------------------------------------------------
    cmd("eval", -3, SCRIPT_RUN, NO_KEYS, SCRIPT_KEYS, "scripting", "2.6.0", "Executes a server-side Lua script.", scripting::eval),
    cmd("evalsha", -3, SCRIPT_RUN, NO_KEYS, &[spec(RW | ACCESS | UPDATE, at(2), keynum(0, 1, 1))], "scripting", "2.6.0", "Executes a server-side Lua script by SHA1 digest.", scripting::evalsha),
    cmd("eval_ro", -3, SCRIPT_RUN_RO, NO_KEYS, SCRIPT_KEYS_RO, "scripting", "7.0.0", "Executes a read-only server-side Lua script.", scripting::eval_ro),
    cmd("evalsha_ro", -3, SCRIPT_RUN_RO, NO_KEYS, &[spec(RO | ACCESS, at(2), keynum(0, 1, 1))], "scripting", "7.0.0", "Executes a read-only server-side Lua script by SHA1 digest.", scripting::evalsha_ro),
    cmd("fcall", -3, SCRIPT_RUN, NO_KEYS, SCRIPT_KEYS, "scripting", "7.0.0", "Invokes a function.", scripting::fcall),
    cmd("fcall_ro", -3, SCRIPT_RUN_RO, NO_KEYS, SCRIPT_KEYS_RO, "scripting", "7.0.0", "Invokes a read-only function.", scripting::fcall_ro),
    cmd("function", -2, &["noscript"], NO_KEYS, NONE, "scripting", "7.0.0", "A container for function commands.", scripting::function),
    cmd("script", -2, &["noscript"], NO_KEYS, NONE, "scripting", "2.6.0", "A container for Lua scripts management commands.", scripting::script),

    // --- transactions -----------------------------------------------------
    cmd("multi", 1, TRANSACTION, NO_KEYS, NONE, "transactions", "1.2.0", "Starts a transaction.", transactions::multi),
    cmd("exec", 1, &["noscript", "loading", "stale", "skip_slowlog"], NO_KEYS, NONE, "transactions", "1.2.0", "Executes all commands in a transaction.", transactions::exec),
    cmd("discard", 1, TRANSACTION, NO_KEYS, NONE, "transactions", "2.0.0", "Discards a transaction.", transactions::discard),
    cmd("watch", -2, TRANSACTION, ALL_KEYS, RO_ALL, "transactions", "2.2.0", "Monitors changes to keys to determine the execution of a transaction.", transactions::watch),
    cmd("unwatch", 1, TRANSACTION, NO_KEYS, NONE, "transactions", "2.2.0", "Forgets about watched keys of a transaction.", transactions::unwatch),

    // --- set --------------------------------------------------------------
    cmd("sadd", -3, WRITE_OOM_FAST, KEY1, RW_INSERT_1, "set", "1.0.0", "Adds one or more members to a set. Creates the key if it doesn't exist.", sets::sadd),
    cmd("srem", -3, WRITE_FAST, KEY1, RW_DELETE_1, "set", "1.0.0", "Removes one or more members from a set. Deletes the set if the last member was removed.", sets::srem),
    cmd("scard", 2, READ_FAST, KEY1, RO_1, "set", "1.0.0", "Returns the number of members in a set.", sets::scard),
    cmd("sismember", 3, READ_FAST, KEY1, RO_1, "set", "1.0.0", "Determines whether a member belongs to a set.", sets::sismember),
    cmd("smismember", -3, READ_FAST, KEY1, RO_ACCESS_1, "set", "6.2.0", "Determines whether multiple members belong to a set.", sets::smismember),
    cmd("smembers", 2, READ, KEY1, RO_ACCESS_1, "set", "1.0.0", "Returns all members of a set.", sets::smembers),
    cmd("smove", 4, WRITE_FAST, (1, 2, 1), POP_PUSH, "set", "1.0.0", "Moves a member from one set to another.", sets::smove),
    cmd("spop", -2, WRITE_FAST, KEY1, RW_ACCESS_DELETE_1, "set", "1.0.0", "Returns one or more random members from a set after removing them. Deletes the set if the last member was popped.", sets::spop),
    cmd("srandmember", -2, READ, KEY1, RO_ACCESS_1, "set", "1.0.0", "Get one or multiple random members from a set", sets::srandmember),
    cmd("sscan", -3, READ, KEY1, RO_ACCESS_1, "set", "2.8.0", "Iterates over members of a set.", sets::sscan),
    cmd("sinter", -2, READ, ALL_KEYS, RO_ACCESS_ALL, "set", "1.0.0", "Returns the intersect of multiple sets.", sets::sinter),
    cmd("sunion", -2, READ, ALL_KEYS, RO_ACCESS_ALL, "set", "1.0.0", "Returns the union of multiple sets.", sets::sunion),
    cmd("sdiff", -2, READ, ALL_KEYS, RO_ACCESS_ALL, "set", "1.0.0", "Returns the difference of multiple sets.", sets::sdiff),
    cmd("sinterstore", -3, WRITE_OOM, ALL_KEYS, STORE_FROM_ALL, "set", "1.0.0", "Stores the intersect of multiple sets in a key.", sets::sinterstore),
    cmd("sunionstore", -3, WRITE_OOM, ALL_KEYS, STORE_FROM_ALL, "set", "1.0.0", "Stores the union of multiple sets in a key.", sets::sunionstore),
    cmd("sdiffstore", -3, WRITE_OOM, ALL_KEYS, STORE_FROM_ALL, "set", "1.0.0", "Stores the difference of multiple sets in a key.", sets::sdiffstore),
    cmd("sintercard", -3, READ, NO_KEYS, COUNTED_RO_ACCESS, "set", "7.0.0", "Returns the number of members of the intersect of multiple sets.", sets::sintercard),

    // --- sorted set -------------------------------------------------------
    cmd("zadd", -4, WRITE_OOM_FAST, KEY1, RW_UPDATE_1, "sorted-set", "1.2.0", "Adds one or more members to a sorted set, or updates their scores. Creates the key if it doesn't exist.", zsets::zadd),
    cmd("zincrby", 4, WRITE_OOM_FAST, KEY1, RW_ACCESS_UPDATE_1, "sorted-set", "1.2.0", "Increments the score of a member in a sorted set.", zsets::zincrby),
    cmd("zrem", -3, WRITE_FAST, KEY1, RW_DELETE_1, "sorted-set", "1.2.0", "Removes one or more members from a sorted set. Deletes the sorted set if all members were removed.", zsets::zrem),
    cmd("zcard", 2, READ_FAST, KEY1, RO_1, "sorted-set", "1.2.0", "Returns the number of members in a sorted set.", zsets::zcard),
    cmd("zscore", 3, READ_FAST, KEY1, RO_ACCESS_1, "sorted-set", "1.2.0", "Returns the score of a member in a sorted set.", zsets::zscore),
    cmd("zmscore", -3, READ_FAST, KEY1, RO_ACCESS_1, "sorted-set", "6.2.0", "Returns the score of one or more members in a sorted set.", zsets::zmscore),
    cmd("zrank", -3, READ_FAST, KEY1, RO_ACCESS_1, "sorted-set", "2.0.0", "Returns the index of a member in a sorted set ordered by ascending scores.", zsets::zrank),
    cmd("zrevrank", -3, READ_FAST, KEY1, RO_ACCESS_1, "sorted-set", "2.0.0", "Returns the index of a member in a sorted set ordered by descending scores.", zsets::zrevrank),
    cmd("zcount", 4, READ_FAST, KEY1, RO_ACCESS_1, "sorted-set", "2.0.0", "Returns the count of members in a sorted set that have scores within a range.", zsets::zcount),
    cmd("zlexcount", 4, READ_FAST, KEY1, RO_ACCESS_1, "sorted-set", "2.8.9", "Returns the number of members in a sorted set within a lexicographical range.", zsets::zlexcount),
    cmd("zrange", -4, READ, KEY1, RO_ACCESS_1, "sorted-set", "1.2.0", "Returns members in a sorted set within a range of indexes.", zsets::zrange),
    cmd("zrevrange", -4, READ, KEY1, RO_ACCESS_1, "sorted-set", "1.2.0", "Returns members in a sorted set within a range of indexes in reverse order.", zsets::zrevrange),
    cmd("zrangebyscore", -4, READ, KEY1, RO_ACCESS_1, "sorted-set", "1.0.5", "Returns members in a sorted set within a range of scores.", zsets::zrangebyscore),
    cmd("zrevrangebyscore", -4, READ, KEY1, RO_ACCESS_1, "sorted-set", "2.2.0", "Returns members in a sorted set within a range of scores in reverse order.", zsets::zrevrangebyscore),
    cmd("zrangebylex", -4, READ, KEY1, RO_ACCESS_1, "sorted-set", "2.8.9", "Returns members in a sorted set within a lexicographical range.", zsets::zrangebylex),
    cmd("zrevrangebylex", -4, READ, KEY1, RO_ACCESS_1, "sorted-set", "2.8.9", "Returns members in a sorted set within a lexicographical range in reverse order.", zsets::zrevrangebylex),
    cmd("zrangestore", -5, WRITE_OOM, (1, 2, 1), &[spec(OW | UPDATE, at(1), ONE), spec(RO | ACCESS, at(2), ONE)], "sorted-set", "6.2.0", "Stores a range of members from sorted set in a key.", zsets::zrangestore),
    cmd("zremrangebyrank", 4, WRITE, KEY1, RW_DELETE_1, "sorted-set", "2.0.0", "Removes members in a sorted set within a range of indexes. Deletes the sorted set if all members were removed.", zsets::zremrangebyrank),
    cmd("zremrangebyscore", 4, WRITE, KEY1, RW_DELETE_1, "sorted-set", "1.2.0", "Removes members in a sorted set within a range of scores. Deletes the sorted set if all members were removed.", zsets::zremrangebyscore),
    cmd("zremrangebylex", 4, WRITE, KEY1, RW_DELETE_1, "sorted-set", "2.8.9", "Removes members in a sorted set within a lexicographical range. Deletes the sorted set if all members were removed.", zsets::zremrangebylex),
    cmd("zpopmin", -2, WRITE_FAST, KEY1, RW_ACCESS_DELETE_1, "sorted-set", "5.0.0", "Returns the lowest-scoring members from a sorted set after removing them. Deletes the sorted set if the last member was popped.", zsets::zpopmin),
    cmd("zpopmax", -2, WRITE_FAST, KEY1, RW_ACCESS_DELETE_1, "sorted-set", "5.0.0", "Returns the highest-scoring members from a sorted set after removing them. Deletes the sorted set if the last member was popped.", zsets::zpopmax),
    cmd("zmpop", -4, WRITE, NO_KEYS, COUNTED_POP, "sorted-set", "7.0.0", "Returns the highest- or lowest-scoring members from one or more sorted sets after removing them. Deletes the sorted set if the last member was popped.", zsets::zmpop),
    cmd("bzpopmin", -3, WRITE_FAST_BLOCKING, (1, -2, 1), BLOCKING_POP, "sorted-set", "5.0.0", "Removes and returns the member with the lowest score from one or more sorted sets. Blocks until a member is available otherwise. Deletes the sorted set if the last element was popped.", zsets::bzpopmin),
    cmd("bzpopmax", -3, WRITE_FAST_BLOCKING, (1, -2, 1), BLOCKING_POP, "sorted-set", "5.0.0", "Removes and returns the member with the highest score from one or more sorted sets. Blocks until a member available otherwise. Deletes the sorted set if the last element was popped.", zsets::bzpopmax),
    cmd("bzmpop", -5, WRITE_BLOCKING, NO_KEYS, BLOCKING_COUNTED_POP, "sorted-set", "7.0.0", "Removes and returns a member by score from one or more sorted sets. Blocks until a member is available otherwise. Deletes the sorted set if the last element was popped.", zsets::bzmpop),
    cmd("zunion", -3, READ, NO_KEYS, COUNTED_RO_ACCESS, "sorted-set", "6.2.0", "Returns the union of multiple sorted sets.", zsets::zunion),
    cmd("zinter", -3, READ, NO_KEYS, COUNTED_RO_ACCESS, "sorted-set", "6.2.0", "Returns the intersect of multiple sorted sets.", zsets::zinter),
    cmd("zdiff", -3, READ, NO_KEYS, COUNTED_RO_ACCESS, "sorted-set", "6.2.0", "Returns the difference between multiple sorted sets.", zsets::zdiff),
    cmd("zunionstore", -4, WRITE_OOM, KEY1, STORE_FROM_COUNTED, "sorted-set", "2.0.0", "Stores the union of multiple sorted sets in a key.", zsets::zunionstore),
    cmd("zinterstore", -4, WRITE_OOM, KEY1, STORE_FROM_COUNTED, "sorted-set", "2.0.0", "Stores the intersect of multiple sorted sets in a key.", zsets::zinterstore),
    cmd("zdiffstore", -4, WRITE_OOM, KEY1, STORE_FROM_COUNTED, "sorted-set", "6.2.0", "Stores the difference of multiple sorted sets in a key.", zsets::zdiffstore),
    cmd("zintercard", -3, READ, NO_KEYS, COUNTED_RO_ACCESS, "sorted-set", "7.0.0", "Returns the number of members of the intersect of multiple sorted sets.", zsets::zintercard),
    cmd("zscan", -3, READ, KEY1, RO_ACCESS_1, "sorted-set", "2.8.0", "Iterates over members and scores of a sorted set.", zsets::zscan),
    cmd("zrandmember", -2, READ, KEY1, RO_ACCESS_1, "sorted-set", "6.2.0", "Returns one or more random members from a sorted set.", zsets::zrandmember),
];
