use std::collections::BTreeSet;

use bytes::Bytes;

use crate::dict::Dict;
use crate::{random_u64, COMPACT_MAX_VALUE, HASH_COMPACT_MAX_ENTRIES};

/// A hash: field → value, where each field may have a TTL of its own
/// (`HEXPIRE` and its family, Redis 7.4).
///
/// Like Redis, small hashes use a compact representation — a flat list that
/// is searched linearly and **keeps insertion order** — and switch to a real
/// hash table once they outgrow it. The switch is one-way.
///
/// # Field expiry
///
/// A hash nobody has set a field TTL on carries nothing extra but an empty
/// pointer, so it costs what it always did. The first field TTL adds the
/// times, and they stay for the life of the hash (empty, possibly), as
/// Redis's `listpackex` encoding does.
///
/// In the compact form, as in Redis's, the fields that have a TTL come
/// first, ordered by it (a field whose TTL is set goes before others with
/// the same time), followed by the others in the order they were added.
/// That order is what `HGETALL` shows, and it makes the times cheap to keep:
/// one number per field with a TTL, in a list beside the pairs. A hash
/// table keeps its fields' times in a table of its own and in time order.
///
/// A field whose TTL has passed is not removed by itself: the database
/// reclaims it (see [`crate::Db::active_expire_fields`]), or a command that
/// looks it up does. Until then it is still stored, and counted by
/// [`Hash::len`] (`HLEN` counts it too in Redis), but every method that
/// takes `now` treats it as missing.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Hash {
    repr: Repr,
    ttls: Option<Box<FieldTtls>>,
}

#[derive(Debug, Clone, PartialEq)]
enum Repr {
    /// At most [`HASH_COMPACT_MAX_ENTRIES`] pairs, each side at most
    /// [`COMPACT_MAX_VALUE`] bytes.
    Compact(Vec<(Bytes, Bytes)>),
    Table(Dict<Bytes>),
}

impl Default for Repr {
    fn default() -> Self {
        Repr::Compact(Vec::new())
    }
}

/// The TTLs of a hash's fields.
#[derive(Debug, Clone)]
pub(crate) struct FieldTtls {
    /// The time under which the database that holds the hash has it in its
    /// index of hashes with field TTLs; `None` if it is not in it. See
    /// [`crate::Db`].
    pub(crate) indexed_at: Option<u64>,
    times: Times,
}

impl PartialEq for FieldTtls {
    fn eq(&self, other: &Self) -> bool {
        self.times == other.times
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Times {
    /// For a compact hash: the TTL of each of its first fields, which are
    /// the ones that have one, earliest first. Absolute, in milliseconds
    /// since the Unix epoch.
    Prefix(Vec<u64>),
    /// For a hash table.
    Table(Box<TableTimes>),
}

#[derive(Debug, Clone, Default)]
struct TableTimes {
    /// Field → time.
    at: Dict<u64>,
    /// The same pairs, ordered by time.
    order: BTreeSet<(u64, Bytes)>,
}

impl PartialEq for TableTimes {
    fn eq(&self, other: &Self) -> bool {
        self.order == other.order
    }
}

impl TableTimes {
    fn set(&mut self, field: Bytes, at: u64) {
        if let Some(old) = self.at.insert(field.clone(), at) {
            self.order.remove(&(old, field.clone()));
        }
        self.order.insert((at, field));
    }

    fn remove(&mut self, field: &[u8]) -> Option<u64> {
        let (field, at) = self.at.remove(field)?;
        self.order.remove(&(at, field));
        Some(at)
    }
}

impl Hash {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of fields, including any whose TTL has passed but that have
    /// not been reclaimed yet.
    pub fn len(&self) -> usize {
        match &self.repr {
            Repr::Compact(pairs) => pairs.len(),
            Repr::Table(dict) => dict.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the compact representation is in use (`OBJECT ENCODING`).
    pub fn is_compact(&self) -> bool {
        matches!(self.repr, Repr::Compact(_))
    }

    /// Whether a field TTL has ever been set on this hash (Redis's
    /// `listpackex` encoding, for a compact one).
    pub fn has_field_ttls(&self) -> bool {
        self.ttls.is_some()
    }

    /// Number of fields that have a TTL.
    pub fn volatile_fields(&self) -> usize {
        match self.ttls.as_deref().map(|ttls| &ttls.times) {
            None => 0,
            Some(Times::Prefix(times)) => times.len(),
            Some(Times::Table(table)) => table.at.len(),
        }
    }

    pub(crate) fn ttls_mut(&mut self) -> Option<&mut FieldTtls> {
        self.ttls.as_deref_mut()
    }

    /// See [`FieldTtls::indexed_at`].
    pub(crate) fn indexed_at(&self) -> Option<u64> {
        self.ttls.as_ref().and_then(|ttls| ttls.indexed_at)
    }

    /// The earliest field TTL.
    pub fn next_field_expiry(&self) -> Option<u64> {
        match self.ttls.as_deref().map(|ttls| &ttls.times) {
            None => None,
            Some(Times::Prefix(times)) => times.first().copied(),
            Some(Times::Table(table)) => table.order.first().map(|(at, _)| *at),
        }
    }

    /// The stored value of `field`, whether or not its TTL has passed, and
    /// that TTL.
    fn lookup(&self, field: &[u8]) -> Option<(&Bytes, Option<u64>)> {
        match &self.repr {
            Repr::Compact(pairs) => {
                let index = pairs.iter().position(|(name, _)| name[..] == *field)?;
                let at = match self.ttls.as_deref().map(|ttls| &ttls.times) {
                    Some(Times::Prefix(times)) => times.get(index).copied(),
                    _ => None,
                };
                Some((&pairs[index].1, at))
            }
            Repr::Table(dict) => {
                let value = dict.get(field)?;
                let at = match self.ttls.as_deref().map(|ttls| &ttls.times) {
                    Some(Times::Table(table)) => table.at.get(field).copied(),
                    _ => None,
                };
                Some((value, at))
            }
        }
    }

    /// The value of `field`, unless it does not exist or its TTL has passed.
    pub fn get(&self, field: &[u8], now: u64) -> Option<&Bytes> {
        match self.lookup(field)? {
            (_, Some(at)) if at <= now => None,
            (value, _) => Some(value),
        }
    }

    pub fn contains(&self, field: &[u8], now: u64) -> bool {
        self.get(field, now).is_some()
    }

    /// Whether `field` is stored, even if its TTL has passed.
    pub fn contains_stored(&self, field: &[u8]) -> bool {
        self.lookup(field).is_some()
    }

    /// The TTL of a stored field: `None` if it has none (or is not there).
    pub fn field_expire_at(&self, field: &[u8]) -> Option<u64> {
        self.ttls.as_ref()?;
        self.lookup(field).and_then(|(_, at)| at)
    }

    /// Whether `field` is stored but its TTL has passed.
    pub fn is_field_expired(&self, field: &[u8], now: u64) -> bool {
        self.ttls.is_some() && self.field_expire_at(field).is_some_and(|at| at <= now)
    }

    /// Whether any field's TTL has passed.
    pub fn has_expired_fields(&self, now: u64) -> bool {
        self.next_field_expiry().is_some_and(|at| at <= now)
    }

    /// Sets `field`, returning `true` if it was not stored before. Like
    /// `HSET`, this removes any TTL the field had. The field name is copied
    /// into its own allocation when it is new.
    pub fn insert(&mut self, field: &[u8], value: Bytes) -> bool {
        if self.field_expire_at(field).is_some() {
            // Losing its TTL, a field of a compact hash moves to the end,
            // behind the fields without one.
            if let Some((name, _, _)) = self.remove_entry(field) {
                self.push(name, value);
            }
            return false;
        }
        self.store(field, value)
    }

    /// Sets the value of `field`, leaving any TTL it has as it is
    /// (`HINCRBY`). Returns `true` if the field was not stored before.
    pub fn set_value(&mut self, field: &[u8], value: Bytes) -> bool {
        self.store(field, value)
    }

    /// Sets the value of `field` where it is, or adds it at the end.
    fn store(&mut self, field: &[u8], value: Bytes) -> bool {
        match &mut self.repr {
            Repr::Compact(pairs) => {
                let fits = field.len() <= COMPACT_MAX_VALUE && value.len() <= COMPACT_MAX_VALUE;
                if let Some((_, slot)) = pairs.iter_mut().find(|(name, _)| name[..] == *field) {
                    *slot = value;
                    if !fits {
                        self.upgrade();
                    }
                    return false;
                }
                self.push(Bytes::copy_from_slice(field), value);
                true
            }
            Repr::Table(dict) => match dict.get_mut(field) {
                Some(slot) => {
                    *slot = value;
                    false
                }
                None => {
                    dict.insert(Bytes::copy_from_slice(field), value);
                    true
                }
            },
        }
    }

    /// Adds a field that is not stored yet, without a TTL, at the end.
    fn push(&mut self, field: Bytes, value: Bytes) {
        match &mut self.repr {
            Repr::Compact(pairs) => {
                let fits = field.len() <= COMPACT_MAX_VALUE && value.len() <= COMPACT_MAX_VALUE;
                pairs.push((field, value));
                if !fits || pairs.len() > HASH_COMPACT_MAX_ENTRIES {
                    self.upgrade();
                }
            }
            Repr::Table(dict) => {
                dict.insert(field, value);
            }
        }
    }

    /// Adds a field that is not stored yet with a TTL: in a compact hash,
    /// among the fields with one, before those with the same time or a
    /// later one, or with `after_equal` after those with the same time.
    fn push_with_ttl(&mut self, field: Bytes, value: Bytes, at: u64, after_equal: bool) {
        let fits = field.len() <= COMPACT_MAX_VALUE && value.len() <= COMPACT_MAX_VALUE;
        let ttls = self.ttls.get_or_insert_with(|| {
            Box::new(FieldTtls {
                indexed_at: None,
                times: match self.repr {
                    Repr::Compact(_) => Times::Prefix(Vec::new()),
                    Repr::Table(_) => Times::Table(Box::default()),
                },
            })
        });
        match (&mut self.repr, &mut ttls.times) {
            (Repr::Compact(pairs), Times::Prefix(times)) => {
                let index = times.partition_point(|&t| t < at || (after_equal && t == at));
                times.insert(index, at);
                pairs.insert(index, (field, value));
                if !fits || pairs.len() > HASH_COMPACT_MAX_ENTRIES {
                    self.upgrade();
                }
            }
            (Repr::Table(dict), Times::Table(table)) => {
                table.set(field.clone(), at);
                dict.insert(field, value);
            }
            _ => unreachable!("the times always match the representation"),
        }
    }

    fn upgrade(&mut self) {
        if let Repr::Compact(pairs) = &mut self.repr {
            let mut dict = Dict::new();
            if let Some(ttls) = &mut self.ttls {
                if let Times::Prefix(times) = &ttls.times {
                    let mut table = TableTimes::default();
                    for ((field, _), &at) in pairs.iter().zip(times) {
                        table.set(field.clone(), at);
                    }
                    ttls.times = Times::Table(Box::new(table));
                }
            }
            for (field, value) in pairs.drain(..) {
                dict.insert(field, value);
            }
            self.repr = Repr::Table(dict);
        }
    }

    /// Removes a stored field with its TTL, and returns them.
    fn remove_entry(&mut self, field: &[u8]) -> Option<(Bytes, Bytes, Option<u64>)> {
        match &mut self.repr {
            Repr::Compact(pairs) => {
                let index = pairs.iter().position(|(name, _)| name[..] == *field)?;
                let at = match self.ttls.as_deref_mut().map(|ttls| &mut ttls.times) {
                    Some(Times::Prefix(times)) if index < times.len() => Some(times.remove(index)),
                    _ => None,
                };
                // Order-preserving removal: later fields keep their place.
                let (name, value) = pairs.remove(index);
                Some((name, value, at))
            }
            Repr::Table(dict) => {
                let (name, value) = dict.remove(field)?;
                let at = match self.ttls.as_deref_mut().map(|ttls| &mut ttls.times) {
                    Some(Times::Table(table)) => table.remove(field),
                    _ => None,
                };
                Some((name, value, at))
            }
        }
    }

    /// Removes `field` and any TTL it has. Returns whether it was stored,
    /// even if its TTL had passed (`HDEL` counts such a field, as in Redis).
    pub fn remove(&mut self, field: &[u8]) -> bool {
        self.remove_entry(field).is_some()
    }

    /// Sets the TTL of a stored field to `at`. Returns `false` (and does
    /// nothing) if the field is not stored.
    pub fn set_field_expire(&mut self, field: &[u8], at: u64) -> bool {
        let Some((name, value, _)) = self.remove_entry(field) else {
            return false;
        };
        self.push_with_ttl(name, value, at, false);
        true
    }

    /// Removes the TTL of `field`. Returns whether it had one.
    pub fn persist_field(&mut self, field: &[u8]) -> bool {
        if self.field_expire_at(field).is_none() {
            return false;
        }
        match self.ttls.as_deref_mut().map(|ttls| &mut ttls.times) {
            Some(Times::Table(table)) => {
                table.remove(field);
            }
            // To the end, behind the fields without a TTL.
            _ => {
                if let Some((name, value, _)) = self.remove_entry(field) {
                    self.push(name, value);
                }
            }
        }
        true
    }

    /// Removes fields whose TTL is at or before `now`, earliest first, up to
    /// `max` of them, and returns their names.
    pub fn remove_expired(&mut self, now: u64, max: usize) -> Vec<Bytes> {
        match (
            &mut self.repr,
            self.ttls.as_deref_mut().map(|ttls| &mut ttls.times),
        ) {
            (Repr::Compact(pairs), Some(Times::Prefix(times))) => {
                let due = times.partition_point(|&at| at <= now).min(max);
                times.drain(..due);
                pairs.drain(..due).map(|(field, _)| field).collect()
            }
            (Repr::Table(dict), Some(Times::Table(table))) => {
                let mut removed = Vec::new();
                while removed.len() < max {
                    let field = match table.order.first() {
                        Some((at, field)) if *at <= now => field.clone(),
                        _ => break,
                    };
                    table.remove(&field);
                    dict.remove(&field);
                    removed.push(field);
                }
                removed
            }
            _ => Vec::new(),
        }
    }

    /// How many of the first fields of a compact hash have expired: being
    /// first and in time order, they are exactly those.
    fn expired_prefix(&self, now: u64) -> usize {
        match self.ttls.as_deref().map(|ttls| &ttls.times) {
            Some(Times::Prefix(times)) => times.partition_point(|&at| at <= now),
            _ => 0,
        }
    }

    /// The times of a hash table's fields, if any.
    fn table_times(&self) -> Option<&TableTimes> {
        match self.ttls.as_deref().map(|ttls| &ttls.times) {
            Some(Times::Table(table)) => Some(table),
            _ => None,
        }
    }

    /// Every stored pair, including fields whose TTL has passed. Compact
    /// hashes yield their order.
    pub fn iter_stored(&self) -> Box<dyn Iterator<Item = (&Bytes, &Bytes)> + '_> {
        match &self.repr {
            Repr::Compact(pairs) => Box::new(pairs.iter().map(|(field, value)| (field, value))),
            Repr::Table(dict) => Box::new(dict.iter()),
        }
    }

    /// Every pair whose field has not expired. Compact hashes yield their
    /// order.
    pub fn iter(&self, now: u64) -> Box<dyn Iterator<Item = (&Bytes, &Bytes)> + '_> {
        if !self.has_expired_fields(now) {
            return self.iter_stored();
        }
        match (&self.repr, self.table_times()) {
            (Repr::Compact(pairs), _) => Box::new(
                pairs[self.expired_prefix(now)..]
                    .iter()
                    .map(|(field, value)| (field, value)),
            ),
            (Repr::Table(dict), Some(table)) => Box::new(
                dict.iter()
                    .filter(move |(field, _)| !table.at.get(field).is_some_and(|&at| at <= now)),
            ),
            (Repr::Table(dict), None) => Box::new(dict.iter()),
        }
    }

    /// Visits the next part of a hash that does not change between calls;
    /// see [`Dict::visit_from`]. A compact hash is visited whole. Fields
    /// whose TTL has passed are visited too.
    pub fn visit_from(
        &self,
        position: u64,
        buckets: usize,
        mut visit: impl FnMut(&Bytes, &Bytes),
    ) -> Option<u64> {
        match &self.repr {
            Repr::Compact(pairs) => {
                for (field, value) in pairs {
                    visit(field, value);
                }
                None
            }
            Repr::Table(dict) => dict.visit_from(position, buckets, visit),
        }
    }

    /// Cursor iteration for `HSCAN`, leaving out fields whose TTL has
    /// passed. A compact hash is returned whole in one call (cursor `0`),
    /// as Redis does.
    pub fn scan(&self, cursor: u64, now: u64, mut visit: impl FnMut(&Bytes, &Bytes)) -> u64 {
        match &self.repr {
            Repr::Compact(pairs) => {
                for (field, value) in &pairs[self.expired_prefix(now)..] {
                    visit(field, value);
                }
                0
            }
            Repr::Table(dict) => {
                let table = self.table_times().filter(|_| self.has_expired_fields(now));
                dict.scan(cursor, |field, value| {
                    if !table.is_some_and(|table| table.at.get(field).is_some_and(|&at| at <= now))
                    {
                        visit(field, value);
                    }
                })
            }
        }
    }

    /// A pseudo-randomly chosen stored pair. Callers remove fields whose
    /// TTL has passed first (`HRANDFIELD` does, as in Redis).
    pub fn random(&self) -> Option<(&Bytes, &Bytes)> {
        match &self.repr {
            Repr::Compact(pairs) if pairs.is_empty() => None,
            Repr::Compact(pairs) => {
                let (field, value) = &pairs[random_u64() as usize % pairs.len()];
                Some((field, value))
            }
            Repr::Table(dict) => dict.random_entry(random_u64(), random_u64()),
        }
    }

    /// Builds a hash from stored fields, as a snapshot holds them: in
    /// order, each with its TTL if it has one. `field_ttls` keeps field
    /// TTLs on the hash even if no field has one (a hash Redis saved in its
    /// `listpackex` form). Fields with a TTL end up first in a compact hash,
    /// by time; among equal times, and among the others, in the order
    /// given.
    pub fn from_fields(
        fields: impl IntoIterator<Item = (Bytes, Bytes, Option<u64>)>,
        field_ttls: bool,
    ) -> Hash {
        let mut hash = Hash::new();
        if field_ttls {
            hash.ttls = Some(Box::new(FieldTtls {
                indexed_at: None,
                times: Times::Prefix(Vec::new()),
            }));
        }
        for (field, value, at) in fields {
            hash.remove_entry(&field);
            match at {
                Some(at) => hash.push_with_ttl(field, value, at, true),
                None => hash.push(field, value),
            }
        }
        hash
    }

    /// Removes every field whose TTL is at or before `now` and returns how
    /// many went (loading a snapshot, `RESTORE`).
    pub fn drop_expired(&mut self, now: u64) -> usize {
        self.remove_expired(now, usize::MAX).len()
    }

    /// Bytes of bookkeeping the field TTLs take, roughly; in constant time.
    pub fn field_ttl_overhead(&self) -> usize {
        match self.ttls.as_deref().map(|ttls| &ttls.times) {
            None => 0,
            Some(Times::Prefix(times)) => 64 + 8 * times.len(),
            // Two table entries per field, sharing the name with the hash.
            Some(Times::Table(table)) => 192 + 170 * table.at.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(text: &str) -> Bytes {
        Bytes::copy_from_slice(text.as_bytes())
    }

    fn fields(hash: &Hash) -> Vec<String> {
        hash.iter_stored()
            .map(|(field, _)| String::from_utf8(field.to_vec()).unwrap())
            .collect()
    }

    #[test]
    fn insert_get_remove() {
        let mut h = Hash::new();
        assert!(h.insert(b"a", b("1")));
        assert!(!h.insert(b"a", b("2")));
        assert_eq!(h.get(b"a", 0), Some(&b("2")));
        assert!(h.contains(b"a", 0));
        assert_eq!(h.len(), 1);
        assert!(h.remove(b"a"));
        assert!(!h.remove(b"a"));
        assert!(h.is_empty());
        assert!(!h.has_field_ttls());
    }

    #[test]
    fn compact_keeps_insertion_order_across_updates_and_removals() {
        let mut h = Hash::new();
        for field in ["c", "a", "d", "b"] {
            h.insert(field.as_bytes(), b("v"));
        }
        h.insert(b"a", b("updated"));
        assert_eq!(fields(&h), ["c", "a", "d", "b"]);
        h.remove(b"a");
        assert_eq!(fields(&h), ["c", "d", "b"]);
        assert!(h.is_compact());
    }

    #[test]
    fn upgrades_when_too_many_entries() {
        let mut h = Hash::new();
        for i in 0..HASH_COMPACT_MAX_ENTRIES {
            h.insert(format!("f{i}").as_bytes(), b("v"));
        }
        assert!(h.is_compact());
        h.insert(b"one-more", b("v"));
        assert!(!h.is_compact());
        assert_eq!(h.len(), HASH_COMPACT_MAX_ENTRIES + 1);
        for i in 0..HASH_COMPACT_MAX_ENTRIES {
            assert_eq!(h.get(format!("f{i}").as_bytes(), 0), Some(&b("v")));
        }
        // The upgrade is one-way.
        for i in 0..HASH_COMPACT_MAX_ENTRIES {
            h.remove(format!("f{i}").as_bytes());
        }
        assert!(!h.is_compact());
        assert_eq!(h.len(), 1);
    }

    #[test]
    fn upgrades_when_a_field_or_value_is_too_long() {
        let long = "x".repeat(COMPACT_MAX_VALUE + 1);
        let edge = "x".repeat(COMPACT_MAX_VALUE);

        let mut h = Hash::new();
        h.insert(b"f", b(&edge));
        assert!(h.is_compact());
        h.insert(b"f", b(&long));
        assert!(!h.is_compact(), "overwriting with a long value upgrades");
        assert_eq!(h.get(b"f", 0), Some(&b(&long)));

        let mut h = Hash::new();
        h.insert(long.as_bytes(), b("v"));
        assert!(!h.is_compact(), "a long field name upgrades");
    }

    #[test]
    fn scan_and_random() {
        let mut h = Hash::new();
        assert!(h.random().is_none());
        for i in 0..300 {
            h.insert(format!("f{i}").as_bytes(), b(&i.to_string()));
        }
        let mut seen = std::collections::HashSet::new();
        let mut cursor = 0;
        loop {
            cursor = h.scan(cursor, 0, |field, _| {
                seen.insert(field.clone());
            });
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(seen.len(), 300);
        let (field, value) = h.random().unwrap();
        assert_eq!(h.get(field, 0), Some(value));
    }

    #[test]
    fn expired_fields_are_hidden_but_stored_until_removed() {
        let mut h = Hash::new();
        for field in ["a", "b", "c"] {
            h.insert(field.as_bytes(), b("v"));
        }
        assert!(h.set_field_expire(b"b", 100));
        assert!(!h.set_field_expire(b"nope", 100));
        assert!(h.has_field_ttls());
        assert_eq!(h.field_expire_at(b"b"), Some(100));
        assert_eq!(h.next_field_expiry(), Some(100));
        assert_eq!(h.get(b"b", 99), Some(&b("v")));
        // The TTL instant itself counts as expired.
        assert_eq!(h.get(b"b", 100), None);
        assert!(h.contains_stored(b"b"));
        assert!(h.is_field_expired(b"b", 100));
        assert_eq!(h.len(), 3, "still stored");
        let live: Vec<&Bytes> = h.iter(100).map(|(field, _)| field).collect();
        assert_eq!(live, [&b("a"), &b("c")]);
        let mut scanned = Vec::new();
        h.scan(0, 100, |field, _| scanned.push(field.clone()));
        assert_eq!(scanned, [b("a"), b("c")]);

        assert_eq!(h.remove_expired(99, 10), Vec::<Bytes>::new());
        assert_eq!(h.remove_expired(100, 10), vec![b("b")]);
        assert_eq!(h.len(), 2);
        assert_eq!(h.next_field_expiry(), None);
        assert!(h.has_field_ttls(), "the table stays");
    }

    #[test]
    fn compact_order_puts_fields_with_a_ttl_first_by_time() {
        // The orders Redis 7.4 shows for the same steps.
        let mut h = Hash::new();
        for field in ["a", "b", "c", "d", "e"] {
            h.insert(field.as_bytes(), b("v"));
        }
        h.set_field_expire(b"c", 100);
        assert_eq!(fields(&h), ["c", "a", "b", "d", "e"]);
        h.set_field_expire(b"e", 100);
        assert_eq!(fields(&h), ["e", "c", "a", "b", "d"], "before equal times");
        h.set_field_expire(b"d", 50);
        assert_eq!(fields(&h), ["d", "e", "c", "a", "b"]);
        h.set_field_expire(b"a", 200);
        assert_eq!(fields(&h), ["d", "e", "c", "a", "b"]);
        // Losing a TTL sends a field to the end.
        h.persist_field(b"c");
        assert_eq!(fields(&h), ["d", "e", "a", "b", "c"]);
        h.insert(b"e", b("w"));
        assert_eq!(fields(&h), ["d", "a", "b", "c", "e"]);
        assert_eq!(h.field_expire_at(b"e"), None);
        // Writing a value without touching the TTL keeps the place.
        h.set_value(b"a", b("x"));
        assert_eq!(fields(&h), ["d", "a", "b", "c", "e"]);
        assert_eq!(h.field_expire_at(b"a"), Some(200));
        assert!(!h.persist_field(b"b"));
        assert!(h.remove(b"a"));
        assert_eq!(h.field_expire_at(b"a"), None);
        assert_eq!(h.volatile_fields(), 1);
    }

    #[test]
    fn field_ttls_survive_the_upgrade() {
        let mut h = Hash::new();
        h.insert(b"a", b("1"));
        h.set_field_expire(b"a", 100);
        h.insert(b"long", b(&"x".repeat(COMPACT_MAX_VALUE + 1)));
        assert!(!h.is_compact());
        assert_eq!(h.field_expire_at(b"a"), Some(100));
        assert_eq!(h.get(b"a", 100), None);
        h.set_field_expire(b"long", 50);
        assert_eq!(h.remove_expired(1_000, 1), vec![b("long")]);
        assert_eq!(h.remove_expired(1_000, 1), vec![b("a")]);
        assert!(h.remove_expired(1_000, 1).is_empty());
    }

    #[test]
    fn built_from_snapshot_fields() {
        let h = Hash::from_fields(
            [
                (b("x"), b("1"), None),
                (b("y"), b("2"), Some(200)),
                (b("z"), b("3"), Some(100)),
            ],
            false,
        );
        assert_eq!(fields(&h), ["z", "y", "x"]);
        assert_eq!(h.field_expire_at(b"y"), Some(200));
        let mut dropped = h.clone();
        assert_eq!(dropped.drop_expired(150), 1);
        assert_eq!(fields(&dropped), ["y", "x"]);

        let plain = Hash::from_fields([(b("x"), b("1"), None)], false);
        assert!(!plain.has_field_ttls());
        let ex = Hash::from_fields([(b("x"), b("1"), None)], true);
        assert!(ex.has_field_ttls());
        assert_eq!(plain.get(b"x", 0), ex.get(b"x", 0));
    }
}
