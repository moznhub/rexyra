//! Persistence.
//!
//! For now this is the RDB snapshot format: [`rdb::Writer`] produces a file
//! Redis can load, and [`rdb::read`] loads files written by this server or
//! by Redis 7. The same value encoding carries `DUMP`/`RESTORE` payloads.

pub mod crc64;
mod lzf;
pub mod rdb;

pub use rdb::{Item, RdbError};
