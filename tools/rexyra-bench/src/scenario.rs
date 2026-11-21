//! The scenarios: what is loaded and what traffic runs.

use serde::Serialize;

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
pub enum Kind {
    /// Foreground clients (and maybe a toucher) before, during and after
    /// a BGSAVE.
    Traffic,
    /// BGSAVE with no traffic, then a blocking SAVE.
    Idle,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
pub enum Toucher {
    None,
    /// One connection that `SET`s the large strings round-robin, back to
    /// back, with new values of the same size.
    BigStrings,
    /// One connection that, every `pace_ms` and immediately when BGSAVE is
    /// acknowledged, does one `RPUSH` to each list and one `HSET` (of an
    /// existing field) to each hash.
    Collections {
        pace_ms: u64,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct Spec {
    pub name: &'static str,
    pub about: &'static str,
    pub kind: Kind,
    /// `s:0` .. : strings the foreground clients GET and SET.
    pub small_keys: usize,
    pub small_value: usize,
    /// If not zero, the `s:` keys are hashes of this many fields `f<j>`,
    /// each holding a `small_value`-byte value, and the foreground clients
    /// `HGET` and `HSET` a random field instead of `GET` and `SET`.
    pub small_fields: usize,
    /// `big:0` .. : large strings.
    pub big_keys: usize,
    pub big_size: usize,
    /// `list:0` .. : lists of `e<j>` elements.
    pub lists: usize,
    pub list_len: usize,
    /// `hash:0` .. : hashes of `f<j>` -> `v<j>`.
    pub hashes: usize,
    pub hash_fields: usize,
    pub toucher: Toucher,
    /// `--save-delay-us` must be given.
    pub needs_delay: bool,
}

impl Spec {
    pub fn key_count(&self) -> usize {
        self.small_keys + self.big_keys + self.lists + self.hashes
    }
}

const MIB: usize = 1 << 20;

fn base(name: &'static str, about: &'static str) -> Spec {
    Spec {
        name,
        about,
        kind: Kind::Traffic,
        small_keys: 100_000,
        small_value: 1000,
        small_fields: 0,
        big_keys: 0,
        big_size: 0,
        lists: 0,
        list_len: 0,
        hashes: 0,
        hash_fields: 0,
        toucher: Toucher::None,
        needs_delay: false,
    }
}

pub fn all() -> Vec<Spec> {
    let small = Spec {
        small_keys: 1_000_000,
        ..base(
            "small",
            "1,000,000 strings of 1,000 bytes; foreground GET/SET on them",
        )
    };
    let big = |name, about, keys, size| Spec {
        big_keys: keys,
        big_size: size,
        toucher: Toucher::BigStrings,
        ..base(name, about)
    };
    let str10m = big(
        "str10m",
        "100 strings of 10 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings",
        100,
        10 * MIB,
    );
    vec![
        small.clone(),
        big(
            "str1m",
            "1,000 strings of 1 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings",
            1000,
            MIB,
        ),
        str10m.clone(),
        big(
            "str100m",
            "10 strings of 100 MiB + 100,000 small strings for foreground traffic; a toucher overwrites the large strings",
            10,
            100 * MIB,
        ),
        Spec {
            lists: 2,
            list_len: 2_000_000,
            hashes: 2,
            hash_fields: 1_000_000,
            toucher: Toucher::Collections { pace_ms: 10 },
            ..base(
                "coll",
                "2 lists of 2,000,000 short elements, 2 hashes of 1,000,000 fields + 100,000 small strings; a toucher RPUSHes/HSETs each collection",
            )
        },
        Spec {
            small_keys: 100_000,
            small_value: 100,
            small_fields: 10,
            ..base(
                "hashes",
                "100,000 hashes of 10 fields of 100 bytes; foreground HGET/HSET of a random field",
            )
        },
        Spec {
            name: "slow-small",
            about: "as 'small', with rdb-key-save-delay set (slow disk stand-in)",
            needs_delay: true,
            ..small.clone()
        },
        Spec {
            name: "slow-str10m",
            about: "as 'str10m', with rdb-key-save-delay set (slow disk stand-in)",
            needs_delay: true,
            ..str10m
        },
        Spec {
            name: "idle",
            about: "the 'small' dataset; BGSAVE with no traffic, then a blocking SAVE",
            kind: Kind::Idle,
            ..small
        },
    ]
}

pub fn find(name: &str) -> Option<Spec> {
    all().into_iter().find(|s| s.name == name)
}
