//! One task per client connection: read bytes, parse requests, hand them to
//! the execution thread, write the replies back.

use std::io;

use bytes::BytesMut;
use rexyra_resp::parse_request;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use crate::executor::Executor;

const READ_BUFFER: usize = 16 * 1024;

/// While a batch is executing, input from the client is buffered up to this
/// many bytes; beyond it the client has to wait for its replies first.
const MAX_BUFFERED_WHILE_WAITING: usize = 1024 * 1024;

/// Serves `stream` until the client disconnects, sends `QUIT`, violates the
/// protocol, or is killed by another client (`killed` fires). The caller
/// registers and unregisters the client with the executor.
pub(crate) async fn serve(
    mut stream: TcpStream,
    id: u64,
    executor: &Executor,
    mut killed: oneshot::Receiver<()>,
) -> io::Result<()> {
    let mut buf = BytesMut::with_capacity(READ_BUFFER);

    loop {
        // Collect every complete request already buffered. This is what makes
        // pipelining cheap: the whole pipeline is executed in one hand-off.
        let mut batch = Vec::new();
        let mut protocol_error = None;
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

        let executed = !batch.is_empty();
        if executed {
            // A blocking command can keep the reply away for a long time, so
            // keep watching the socket meanwhile: that is the only way to
            // notice that a blocked client has gone.
            let pending = executor.execute(id, batch);
            tokio::pin!(pending);
            let mut peer_closed = false;
            let reply = loop {
                tokio::select! {
                    biased;
                    reply = &mut pending => break reply,
                    _ = &mut killed => return Ok(()),
                    read = stream.read_buf(&mut buf),
                        if !peer_closed && buf.len() < MAX_BUFFERED_WHILE_WAITING =>
                    {
                        if read? == 0 {
                            // Possibly only a half-close after a pipeline, so
                            // the replies are still owed. The executor drops
                            // the batch only if the client is blocked.
                            peer_closed = true;
                            executor.hangup(id);
                        }
                    }
                }
            };
            let Ok(reply) = reply else {
                // The server is shutting down, or this client hung up while
                // blocked.
                return Ok(());
            };
            stream.write_all(&reply.data).await?;
            if reply.close {
                return stream.shutdown().await;
            }
        }

        if let Some(e) = protocol_error {
            // Commands before the bad bytes were answered above; now report
            // the error and drop the connection, as Redis does.
            let message = format!("-ERR {e}\r\n");
            stream.write_all(message.as_bytes()).await?;
            return stream.shutdown().await;
        }

        if executed {
            // More requests may have arrived, and been buffered, while that
            // batch was executing. Handle them before waiting for input.
            continue;
        }

        tokio::select! {
            _ = &mut killed => return Ok(()),
            read = stream.read_buf(&mut buf) => {
                if read? == 0 {
                    return Ok(()); // peer closed
                }
            }
        }
    }
}
