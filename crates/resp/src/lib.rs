//! RESP2 / RESP3 wire protocol.
//!
//! Three entry points, none of which perform I/O:
//!
//! * [`parse_request`] — the server-side hot path. Parses one client command
//!   (multibulk or inline) into its arguments without copying payload bytes.
//! * [`decode`] — a general decoder for any RESP2/RESP3 value.
//! * [`encode`] — serialises a [`Frame`] for a given [`Protocol`] version,
//!   downgrading RESP3-only types when the client speaks RESP2.

mod decode;
mod encode;
mod frame;
mod request;
mod util;

pub use decode::decode;
pub use encode::{encode, format_double};
pub use frame::{Frame, Protocol};
pub use request::parse_request;
pub use util::parse_i64;

use std::fmt;

/// Largest bulk string accepted, matching Redis's `proto-max-bulk-len` default.
pub const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
/// Largest number of elements accepted in one aggregate or command.
pub const MAX_AGGREGATE_LEN: usize = 1024 * 1024;
/// Longest header or inline line accepted before the terminator is seen.
pub const MAX_LINE_LEN: usize = 64 * 1024;
/// Deepest nesting the general decoder will follow.
pub const MAX_DEPTH: usize = 32;

/// The peer sent bytes that are not valid RESP. The connection should be
/// closed after reporting `-ERR <message>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError {
    message: String,
}

impl ProtocolError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ProtocolError {}
