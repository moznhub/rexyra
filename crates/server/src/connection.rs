//! One task per client connection: read bytes, parse requests, hand them to
//! the execution thread, and write to the socket whatever the thread sends
//! back, in the order it sends it: replies, and messages pushed to the
//! client (Pub/Sub) at any time.

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::{Bytes, BytesMut};
use rexyra_resp::parse_request;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use crate::executor::{ClientChannels, Executor, Out};

const READ_BUFFER: usize = 16 * 1024;

/// While a batch is executing, input from the client is buffered up to this
/// many bytes; beyond it the client has to wait for its replies first.
const MAX_BUFFERED_WHILE_WAITING: usize = 1024 * 1024;

/// Writes `data` and lowers the count of bytes queued for the client.
/// Returns `false`, abandoning the write, if the client is killed first: a
/// client that does not read would otherwise keep this task waiting here
/// for ever, which is exactly when it gets killed for its output limit.
async fn write(
    stream: &mut TcpStream,
    queued: &AtomicUsize,
    data: Bytes,
    killed: &mut oneshot::Receiver<()>,
) -> io::Result<bool> {
    tokio::select! {
        biased;
        _ = killed => Ok(false),
        written = stream.write_all(&data) => {
            written?;
            queued.fetch_sub(data.len(), Ordering::Relaxed);
            Ok(true)
        }
    }
}

/// Serves `stream` until the client disconnects, sends `QUIT`, violates the
/// protocol, or is killed (`killed` fires). The caller registers and
/// unregisters the client with the executor.
pub(crate) async fn serve(
    mut stream: TcpStream,
    id: u64,
    executor: &Executor,
    channels: ClientChannels,
) -> io::Result<()> {
    let ClientChannels {
        mut killed,
        mut out,
        reply_to,
        queued,
    } = channels;
    let mut buf = BytesMut::with_capacity(READ_BUFFER);
    // A batch has been handed over and its `Out::Done` has not arrived.
    let mut in_flight = false;
    // The peer closed its end while a batch was in flight: once the batch's
    // output is written, the connection is over.
    let mut peer_closed = false;
    let mut protocol_error = None;

    loop {
        if !in_flight {
            if peer_closed {
                return Ok(());
            }
            // Collect every complete request already buffered. This is what
            // makes pipelining cheap: the whole pipeline is executed in one
            // hand-off.
            let mut batch = Vec::new();
            if protocol_error.is_none() {
                loop {
                    match parse_request(&mut buf) {
                        Ok(Some(args)) => batch.push(args),
                        Ok(None) => break,
                        Err(e) => {
                            protocol_error = Some(e);
                            break;
                        }
                    }
                }
            }
            if !batch.is_empty() {
                if executor.execute(id, batch, &reply_to).is_err() {
                    return Ok(()); // the server is shutting down
                }
                in_flight = true;
            } else if let Some(e) = protocol_error.take() {
                // Commands before the bad bytes have been answered; now
                // report the error and drop the connection, as Redis does.
                let message = format!("-ERR {e}\r\n");
                stream.write_all(message.as_bytes()).await?;
                return stream.shutdown().await;
            }
        }

        // While a batch is in flight (a blocking command can keep it for a
        // long time), keep watching the socket: that is the only way to
        // notice that a blocked client has gone.
        let can_read = !peer_closed && (!in_flight || buf.len() < MAX_BUFFERED_WHILE_WAITING);
        tokio::select! {
            biased;
            _ = &mut killed => return Ok(()),
            message = out.recv() => match message {
                // Cannot happen while `reply_to` is held here.
                None => return Ok(()),
                Some(Out::Data(data)) => {
                    if !write(&mut stream, &queued, data, &mut killed).await? {
                        return Ok(());
                    }
                }
                Some(Out::Done { data, close }) => {
                    if !write(&mut stream, &queued, data, &mut killed).await? {
                        return Ok(());
                    }
                    in_flight = false;
                    if close {
                        return stream.shutdown().await;
                    }
                }
                // The client hung up while blocked.
                Some(Out::Abandon) => return Ok(()),
            },
            read = stream.read_buf(&mut buf), if can_read => {
                if read? == 0 {
                    if !in_flight {
                        return Ok(()); // peer closed
                    }
                    // Possibly only a half-close after a pipeline, so the
                    // replies are still owed. The executor drops the batch
                    // only if the client is blocked.
                    peer_closed = true;
                    executor.hangup(id);
                }
            }
        }
    }
}
