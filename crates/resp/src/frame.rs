use bytes::Bytes;

/// Protocol version negotiated with `HELLO`. Connections start in RESP2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protocol {
    #[default]
    Resp2,
    Resp3,
}

/// A RESP value.
///
/// Command handlers build replies using the richest type available (for
/// example [`Frame::Map`]); the encoder downgrades to the RESP2 equivalent
/// when the client has not switched to RESP3.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// `+OK` — must not contain CR or LF.
    Simple(String),
    /// `-ERR ...` — must not contain CR or LF.
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    /// RESP3 `_`; RESP2 `$-1`.
    Null,
    /// A null array: RESP2 `*-1`, RESP3 `_`. Returned where Redis
    /// distinguishes "no list" from "no string" (`LPOP key count` on a
    /// missing key, blocking pops that time out, aborted `EXEC`).
    NullArray,
    Array(Vec<Frame>),
    /// RESP3 `,`; RESP2 bulk string.
    Double(f64),
    /// RESP3 `#t` / `#f`; RESP2 `:1` / `:0`.
    Boolean(bool),
    /// RESP3 `(`; RESP2 bulk string.
    BigNumber(String),
    /// RESP3 `=`; RESP2 bulk string without the format prefix.
    Verbatim {
        format: [u8; 3],
        data: Bytes,
    },
    /// RESP3 `%`; RESP2 flat array of alternating keys and values.
    Map(Vec<(Frame, Frame)>),
    /// RESP3 `~`; RESP2 array.
    Set(Vec<Frame>),
    /// RESP3 `>`; RESP2 array.
    Push(Vec<Frame>),
    /// Several replies sent back to back, for a command that answers with
    /// more than one (`SUBSCRIBE a b` confirms each channel separately).
    /// Never decoded; only produced by the server.
    Sequence(Vec<Frame>),
}

impl Frame {
    pub fn ok() -> Frame {
        Frame::Simple("OK".to_owned())
    }

    pub fn simple(s: impl Into<String>) -> Frame {
        Frame::Simple(s.into())
    }

    pub fn error(s: impl Into<String>) -> Frame {
        Frame::Error(s.into())
    }

    pub fn bulk(b: impl Into<Bytes>) -> Frame {
        Frame::Bulk(b.into())
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Frame::Error(_))
    }
}
