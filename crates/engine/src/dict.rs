use std::hash::{BuildHasher, RandomState};
use std::mem;

use bytes::Bytes;

const INITIAL_SIZE: usize = 4;
/// While rehashing, give up after visiting this many empty buckets per step
/// so one step stays O(1) even in a sparse table.
const EMPTY_VISITS_PER_STEP: usize = 10;

/// A hash table keyed by byte strings that resizes **incrementally**.
///
/// A conventional table rehashes every entry in one go when it grows, which
/// for millions of keys means a pause of hundreds of milliseconds inside
/// whichever command happened to trigger it. Here, growing allocates a second
/// table and entries migrate a bucket at a time, piggy-backed on later
/// operations (see [`Dict::rehash_step`]), so no single command pays.
///
/// It also provides [`Dict::scan`], a stateless cursor iteration that stays
/// correct while the table is resized between calls — the guarantee `SCAN`
/// needs and that `std::collections::HashMap` cannot offer.
///
/// Hashing uses SipHash with a per-table random key, so clients cannot craft
/// keys that collide.
#[derive(Clone)]
pub struct Dict<V> {
    /// `tables[0]` is the live table. `tables[1]` is the migration target and
    /// is empty unless a rehash is in progress.
    tables: [Table<V>; 2],
    /// Next bucket of `tables[0]` to migrate; `None` when not rehashing.
    /// Invariant: every bucket of `tables[0]` below this index is empty.
    rehash_idx: Option<usize>,
    hasher: RandomState,
    rng: u64,
}

/// Appends to a bucket, growing it by exactly one slot. Buckets hold one
/// entry on average, and `Vec`'s usual growth would reserve room for four:
/// with millions of keys that is most of the memory in use.
fn push_exact<T>(bucket: &mut Vec<T>, item: T) {
    if bucket.len() == bucket.capacity() {
        bucket.reserve_exact(1);
    }
    bucket.push(item);
}

#[derive(Clone)]
struct Table<V> {
    /// Always empty or a power of two in length.
    buckets: Vec<Vec<(Bytes, V)>>,
    used: usize,
}

impl<V> Table<V> {
    const fn empty() -> Self {
        Self {
            buckets: Vec::new(),
            used: 0,
        }
    }

    fn with_size(size: usize) -> Self {
        debug_assert!(size.is_power_of_two());
        Self {
            buckets: (0..size).map(|_| Vec::new()).collect(),
            used: 0,
        }
    }

    fn mask(&self) -> usize {
        self.buckets.len().wrapping_sub(1)
    }
}

/// Location of an entry, valid until the next mutation of the dictionary.
#[derive(Debug, Clone, Copy)]
pub struct Pos {
    table: usize,
    bucket: usize,
    slot: usize,
}

impl<V> std::fmt::Debug for Dict<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dict").field("len", &self.len()).finish()
    }
}

/// Two dictionaries are equal when they hold the same entries, regardless of
/// table size, hash seed or rehash progress.
impl<V: PartialEq> PartialEq for Dict<V> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .all(|(key, value)| other.get(key) == Some(value))
    }
}

impl<V> Default for Dict<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> Dict<V> {
    pub fn new() -> Self {
        let hasher = RandomState::new();
        // Any non-zero seed works for xorshift; derive one from the hash key.
        let rng = hasher.hash_one(0x9E37_79B9_7F4A_7C15_u64) | 1;
        Self {
            tables: [Table::empty(), Table::empty()],
            rehash_idx: None,
            hasher,
            rng,
        }
    }

    pub fn len(&self) -> usize {
        self.tables[0].used + self.tables[1].used
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_rehashing(&self) -> bool {
        self.rehash_idx.is_some()
    }

    fn hash(&self, key: &[u8]) -> usize {
        self.hasher.hash_one(key) as usize
    }

    /// Finds `key`. Use [`Dict::at`] / [`Dict::at_mut`] to reach the entry.
    pub fn find(&self, key: &[u8]) -> Option<Pos> {
        if self.is_empty() {
            return None;
        }
        let hash = self.hash(key);
        for (index, table) in self.tables.iter().enumerate() {
            if table.buckets.is_empty() {
                continue;
            }
            let bucket = hash & table.mask();
            if let Some(slot) = table.buckets[bucket]
                .iter()
                .position(|(k, _)| k[..] == *key)
            {
                return Some(Pos {
                    table: index,
                    bucket,
                    slot,
                });
            }
        }
        None
    }

    pub fn at(&self, pos: Pos) -> (&Bytes, &V) {
        let (key, value) = &self.tables[pos.table].buckets[pos.bucket][pos.slot];
        (key, value)
    }

    pub fn at_mut(&mut self, pos: Pos) -> (&Bytes, &mut V) {
        let (key, value) = &mut self.tables[pos.table].buckets[pos.bucket][pos.slot];
        (key, value)
    }

    pub fn get(&self, key: &[u8]) -> Option<&V> {
        self.find(key).map(|pos| self.at(pos).1)
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        self.find(key).map(|pos| self.at_mut(pos).1)
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.find(key).is_some()
    }

    /// Inserts or replaces, returning the previous value.
    pub fn insert(&mut self, key: Bytes, value: V) -> Option<V> {
        self.rehash_step(1);
        if let Some(pos) = self.find(&key) {
            return Some(mem::replace(self.at_mut(pos).1, value));
        }
        self.grow_if_needed();
        let hash = self.hash(&key);
        // New entries go straight to the target table during a rehash, so the
        // old table only ever shrinks.
        let table = &mut self.tables[usize::from(self.rehash_idx.is_some())];
        let bucket = hash & table.mask();
        push_exact(&mut table.buckets[bucket], (key, value));
        table.used += 1;
        None
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<(Bytes, V)> {
        self.rehash_step(1);
        let pos = self.find(key)?;
        let table = &mut self.tables[pos.table];
        let removed = table.buckets[pos.bucket].swap_remove(pos.slot);
        if table.buckets[pos.bucket].is_empty() {
            // Give the bucket's storage back rather than keep it for later.
            table.buckets[pos.bucket] = Vec::new();
        }
        table.used -= 1;
        self.shrink_if_needed();
        Some(removed)
    }

    pub fn clear(&mut self) {
        self.tables = [Table::empty(), Table::empty()];
        self.rehash_idx = None;
    }

    /// Every entry, in unspecified order.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &V)> {
        self.tables
            .iter()
            .flat_map(|table| table.buckets.iter())
            .flat_map(|bucket| bucket.iter().map(|(key, value)| (key, value)))
    }

    /// Visits the entries of the next `buckets` buckets, starting from
    /// `position` (`0` to begin). Returns where to carry on from, or `None`
    /// once every entry has been visited.
    ///
    /// Unlike [`Dict::scan`] this is only for a dictionary that does not
    /// change between calls. In return it goes through the table in the
    /// order it is laid out in memory, which on a large table is several
    /// times faster than the order a scan has to use.
    pub fn visit_from(
        &self,
        position: u64,
        buckets: usize,
        mut visit: impl FnMut(&Bytes, &V),
    ) -> Option<u64> {
        let first = self.tables[0].buckets.len();
        let total = first + self.tables[1].buckets.len();
        let start = usize::try_from(position).map_or(total, |start| start.min(total));
        let end = start.saturating_add(buckets.max(1)).min(total);
        for index in start..end {
            let bucket = match index.checked_sub(first) {
                None => &self.tables[0].buckets[index],
                Some(index) => &self.tables[1].buckets[index],
            };
            for (key, value) in bucket {
                visit(key, value);
            }
        }
        (end < total).then_some(end as u64)
    }

    fn grow_if_needed(&mut self) {
        if self.is_rehashing() {
            return;
        }
        let size = self.tables[0].buckets.len();
        if size == 0 {
            self.tables[0] = Table::with_size(INITIAL_SIZE);
        } else if self.tables[0].used >= size {
            self.start_rehash(size * 2);
        }
    }

    fn shrink_if_needed(&mut self) {
        if self.is_rehashing() {
            return;
        }
        let size = self.tables[0].buckets.len();
        let used = self.tables[0].used;
        if size > INITIAL_SIZE && used * 8 <= size {
            let target = used.next_power_of_two().max(INITIAL_SIZE);
            if target < size {
                self.start_rehash(target);
            }
        }
    }

    fn start_rehash(&mut self, new_size: usize) {
        self.tables[1] = Table::with_size(new_size);
        self.rehash_idx = Some(0);
    }

    /// Migrates up to `buckets` non-empty buckets to the new table. Called
    /// with `1` by every insert and remove, and in larger batches from the
    /// server's periodic maintenance so an idle table still finishes.
    pub fn rehash_step(&mut self, buckets: usize) {
        let Some(mut idx) = self.rehash_idx else {
            return;
        };
        let hasher = &self.hasher;
        let [old, new] = &mut self.tables;
        let new_mask = new.mask();
        let mut empty_visits = buckets.saturating_mul(EMPTY_VISITS_PER_STEP);

        'steps: for _ in 0..buckets {
            while old.used > 0 && old.buckets[idx].is_empty() {
                idx += 1;
                empty_visits -= 1;
                if empty_visits == 0 {
                    break 'steps;
                }
            }
            if old.used == 0 {
                break;
            }
            let bucket = mem::take(&mut old.buckets[idx]);
            old.used -= bucket.len();
            new.used += bucket.len();
            for (key, value) in bucket {
                let target = hasher.hash_one(&key[..]) as usize & new_mask;
                push_exact(&mut new.buckets[target], (key, value));
            }
            idx += 1;
        }

        if old.used == 0 {
            self.tables[0] = mem::replace(&mut self.tables[1], Table::empty());
            self.rehash_idx = None;
            // Deletes that arrived while this rehash was running may have
            // left the new table oversized too. Without this check it would
            // stay that way until the next delete.
            self.shrink_if_needed();
        } else {
            self.rehash_idx = Some(idx);
        }
    }

    /// Visits part of the table and returns the cursor for the next call.
    /// Start with cursor `0`; iteration is complete when `0` is returned.
    ///
    /// Guarantees, even if the table grows, shrinks or is mid-rehash between
    /// or during calls: every entry present for the whole iteration is visited
    /// at least once. An entry may be visited more than once.
    ///
    /// This is the reverse-binary-increment cursor from Redis's `dictScan`:
    /// the cursor counts with its *high* bits changing first, so buckets that
    /// split or merge on resize stay on the already-visited or still-to-visit
    /// side of the cursor together.
    pub fn scan(&self, cursor: u64, mut visit: impl FnMut(&Bytes, &V)) -> u64 {
        if self.is_empty() {
            return 0;
        }
        let mut emit = |table: &Table<V>, index: u64| {
            for (key, value) in &table.buckets[index as usize] {
                visit(key, value);
            }
        };
        let advance = |cursor: u64, mask: u64| -> u64 {
            // Set the bits above the mask so the increment carries into the
            // masked bits, then add one to the bit-reversed cursor.
            ((cursor | !mask).reverse_bits().wrapping_add(1)).reverse_bits()
        };
        let mut cursor = cursor;

        if !self.is_rehashing() {
            let mask = self.tables[0].mask() as u64;
            emit(&self.tables[0], cursor & mask);
            return advance(cursor, mask);
        }

        // Two tables: visit the bucket in the smaller one, then every bucket
        // of the larger one that it expands into.
        let (small, large) = if self.tables[0].buckets.len() <= self.tables[1].buckets.len() {
            (&self.tables[0], &self.tables[1])
        } else {
            (&self.tables[1], &self.tables[0])
        };
        let (small_mask, large_mask) = (small.mask() as u64, large.mask() as u64);
        emit(small, cursor & small_mask);
        loop {
            emit(large, cursor & large_mask);
            cursor = advance(cursor, large_mask);
            if cursor & (small_mask ^ large_mask) == 0 {
                break;
            }
        }
        cursor
    }

    /// A pseudo-randomly chosen key. Not perfectly uniform (keys in sparsely
    /// filled regions are slightly favoured), which matches what `RANDOMKEY`
    /// has always promised.
    pub fn random_key(&mut self) -> Option<&Bytes> {
        let (start, pick) = (self.next_random(), self.next_random());
        self.random_entry(start, pick).map(|(key, _)| key)
    }

    /// The entry selected by two caller-supplied random numbers: `start`
    /// chooses where to begin looking for a non-empty bucket and `pick`
    /// chooses within it. Takes `&self` so callers that only hold a shared
    /// reference can still sample.
    pub fn random_entry(&self, start: u64, pick: u64) -> Option<(&Bytes, &V)> {
        if self.is_empty() {
            return None;
        }
        let first = self.tables[0].buckets.len();
        let total = first + self.tables[1].buckets.len();
        let start = start as usize % total;
        (0..total)
            .map(|offset| (start + offset) % total)
            .map(|index| {
                if index < first {
                    &self.tables[0].buckets[index]
                } else {
                    &self.tables[1].buckets[index - first]
                }
            })
            .find(|bucket| !bucket.is_empty())
            .map(|bucket| {
                let (key, value) = &bucket[pick as usize % bucket.len()];
                (key, value)
            })
    }

    /// xorshift64: tiny, fast, and plenty for picking a random bucket.
    fn next_random(&mut self) -> u64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        self.rng
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::{HashMap, HashSet};

    fn key(i: usize) -> Bytes {
        Bytes::from(format!("key:{i}"))
    }

    #[test]
    fn insert_get_remove() {
        let mut d = Dict::new();
        assert_eq!(d.insert(key(1), 10), None);
        assert_eq!(d.insert(key(1), 11), Some(10));
        assert_eq!(d.get(b"key:1"), Some(&11));
        assert_eq!(d.len(), 1);
        assert!(d.contains(b"key:1"));
        assert_eq!(d.remove(b"key:1"), Some((key(1), 11)));
        assert_eq!(d.remove(b"key:1"), None);
        assert!(d.is_empty());
        assert_eq!(d.get(b"key:1"), None);
    }

    #[test]
    fn get_mut_changes_value() {
        let mut d = Dict::new();
        d.insert(key(1), 1);
        *d.get_mut(b"key:1").unwrap() += 5;
        assert_eq!(d.get(b"key:1"), Some(&6));
    }

    #[test]
    fn grows_incrementally_without_losing_entries() {
        let mut d = Dict::new();
        let mut saw_rehash = false;
        for i in 0..10_000 {
            d.insert(key(i), i);
            saw_rehash |= d.is_rehashing();
            // Every key inserted so far must be reachable at every moment,
            // including in the middle of a rehash.
            if i % 97 == 0 {
                for j in (0..=i).step_by(13) {
                    assert_eq!(d.get(&key(j)), Some(&j), "key {j} lost at {i}");
                }
            }
        }
        assert!(saw_rehash);
        assert_eq!(d.len(), 10_000);
        assert_eq!(d.iter().count(), 10_000);
    }

    #[test]
    fn visit_from_covers_everything_once() {
        // Sizes that leave the table settled, and ones that catch it
        // part-way through moving to a bigger one.
        for count in [0usize, 1, 5, 64, 65, 500, 1_025, 3_000] {
            let mut d = Dict::new();
            for i in 0..count {
                d.insert(key(i), i);
            }
            for step in [1, 7, 10_000] {
                let mut seen = Vec::new();
                let mut position = 0;
                let mut calls = 0;
                while let Some(next) =
                    d.visit_from(position, step, |k, v| seen.push((k.clone(), *v)))
                {
                    assert!(next > position);
                    position = next;
                    calls += 1;
                    assert!(calls < 100_000);
                }
                let mut all: Vec<(Bytes, usize)> = d.iter().map(|(k, v)| (k.clone(), *v)).collect();
                // In the same order as a plain iteration, each entry once.
                assert_eq!(seen, all, "{count} entries, {step} at a time");
                all.dedup();
                assert_eq!(all.len(), count);
            }
            // Past the end there is nothing.
            assert_eq!(d.visit_from(u64::MAX, 5, |_, _| panic!("visited")), None);
        }
    }

    #[test]
    fn shrinks_after_mass_delete() {
        let mut d = Dict::new();
        for i in 0..4_096 {
            d.insert(key(i), i);
        }
        while d.is_rehashing() {
            d.rehash_step(100);
        }
        let grown = d.tables[0].buckets.len();
        for i in 0..4_090 {
            assert!(d.remove(&key(i)).is_some());
        }
        while d.is_rehashing() {
            d.rehash_step(100);
        }
        assert!(d.tables[0].buckets.len() < grown / 8);
        for i in 4_090..4_096 {
            assert_eq!(d.get(&key(i)), Some(&i));
        }
    }

    #[test]
    fn rehash_step_finishes_an_idle_table() {
        let mut d = Dict::new();
        for i in 0..1_000 {
            d.insert(key(i), i);
        }
        let mut steps = 0;
        while d.is_rehashing() {
            d.rehash_step(1);
            steps += 1;
            assert!(steps < 100_000, "rehash never finished");
        }
        assert_eq!(d.tables[1].buckets.len(), 0);
        assert_eq!(d.len(), 1_000);
    }

    fn full_scan(d: &Dict<usize>) -> Vec<Bytes> {
        let mut seen = Vec::new();
        let mut cursor = 0;
        loop {
            cursor = d.scan(cursor, |k, _| seen.push(k.clone()));
            if cursor == 0 {
                return seen;
            }
        }
    }

    #[test]
    fn scan_visits_everything() {
        let mut d = Dict::new();
        assert_eq!(d.scan(0, |_, _: &usize| panic!("empty")), 0);
        for i in 0..1_000 {
            d.insert(key(i), i);
        }
        let seen: HashSet<Bytes> = full_scan(&d).into_iter().collect();
        assert_eq!(seen.len(), 1_000);
    }

    #[test]
    fn scan_survives_growth_and_shrink_between_calls() {
        let mut d = Dict::new();
        for i in 0..200 {
            d.insert(key(i), i);
        }
        let stable: HashSet<Bytes> = (0..200).map(key).collect();

        let mut seen = HashSet::new();
        let mut cursor = 0;
        let mut next = 200;
        let mut round = 0;
        loop {
            cursor = d.scan(cursor, |k, _| {
                seen.insert(k.clone());
            });
            if cursor == 0 {
                break;
            }
            // Churn the table hard between calls: grow for a while, then
            // delete the extras to force a shrink, repeatedly.
            if round % 2 == 0 {
                for _ in 0..300 {
                    d.insert(key(next), next);
                    next += 1;
                }
            } else {
                for i in 200..next {
                    d.remove(&key(i));
                }
            }
            round += 1;
            assert!(round < 100_000);
        }
        assert!(
            stable.is_subset(&seen),
            "scan missed a key that never moved"
        );
    }

    #[test]
    fn random_key_returns_existing_keys_and_covers_the_table() {
        let mut d: Dict<usize> = Dict::new();
        assert!(d.random_key().is_none());
        for i in 0..50 {
            d.insert(key(i), i);
        }
        let mut seen = HashSet::new();
        for _ in 0..5_000 {
            let k = d.random_key().unwrap().clone();
            assert!(d.contains(&k));
            seen.insert(k);
        }
        assert_eq!(seen.len(), 50);
    }

    #[derive(Debug, Clone)]
    enum Op {
        Insert(u8, u32),
        Remove(u8),
        Rehash(u8),
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            3 => (any::<u8>(), any::<u32>()).prop_map(|(k, v)| Op::Insert(k, v)),
            2 => any::<u8>().prop_map(Op::Remove),
            1 => (0u8..4).prop_map(Op::Rehash),
        ]
    }

    proptest! {
        /// The dictionary behaves exactly like `HashMap` under any sequence
        /// of operations, and a full scan always sees exactly its contents.
        #[test]
        fn behaves_like_hashmap(ops in prop::collection::vec(op(), 0..600)) {
            let mut dict = Dict::new();
            let mut model: HashMap<Vec<u8>, u32> = HashMap::new();
            for op in ops {
                match op {
                    Op::Insert(k, v) => {
                        let k = vec![k];
                        prop_assert_eq!(
                            dict.insert(Bytes::from(k.clone()), v),
                            model.insert(k, v)
                        );
                    }
                    Op::Remove(k) => {
                        let k = vec![k];
                        prop_assert_eq!(dict.remove(&k).map(|(_, v)| v), model.remove(&k));
                    }
                    Op::Rehash(n) => dict.rehash_step(usize::from(n)),
                }
                prop_assert_eq!(dict.len(), model.len());
            }
            for (k, v) in &model {
                prop_assert_eq!(dict.get(k), Some(v));
            }
            let mut scanned = HashMap::new();
            let mut cursor = 0;
            loop {
                cursor = dict.scan(cursor, |k, v| {
                    scanned.insert(k.to_vec(), *v);
                });
                if cursor == 0 {
                    break;
                }
            }
            prop_assert_eq!(scanned, model);
        }
    }
}
