use tokio::sync::oneshot;
use tokio_stream::{Stream, StreamExt};

use protocol::extensions as pb_ext;
use protocol::generated as pb;

use crate::client::errors::ClientError;
use crate::client::ClientResult;

/// `Appender` adapts an `Append RPC` to the `Stream` trait. The first byte
/// written to the `Appender` initiates the RPC. Subsequent bytes are streamed to
/// brokers as they are written. Writes to the `Appender` may stall as the RPC
/// window fills, while waiting for brokers to sequence this Append into the
/// journal. Once they do, brokers will expect remaining content to append is
/// quickly written to this `Appender` (and may time-out the RPC if it's not).
///
/// Content written to this `Appender` does not commit until `close` is called,
/// including cases where the application dies without calling `close`. If a
/// call to `close` is started and the application dies before `close` returns,
/// the append may or may commit.
///
/// The application can cleanly roll-back a started `Appender` by `aborting` it.
#[derive(Debug)]
pub struct Appender {
    rjc: pb_ext::RoutedJournalClient,
    request: pb::AppendRequest,
}

impl Appender {
    /// Returns an Appender of the given AppendRequest with an async stream ready to process requests.
    pub async fn new(
        rjc: pb_ext::RoutedJournalClient,
        request: pb::AppendRequest,
    ) -> ClientResult<Self> {
        // TODO: implement validator
        // The initial request must have a non-empty `journal` to initiate the Append RPC.
        if request.journal.is_empty() {
            return Err(pb_ext::ProtocolError::Validation {
                msg: "expected Request.Journal".to_string(),
                source: None,
            }
            .into());
        }
        Ok(Self { rjc, request })
    }

    /// Append the contents of a `Stream` to a journal as a single Append transaction.
    pub async fn append<Reader>(&mut self, reader: Reader) -> ClientResult<pb::AppendResponse>
    where
        Reader: Stream<Item = std::io::Result<bytes::Bytes>> + Send + Sync + 'static + Unpin,
    {
        let (stream_err_tx, stream_err_rx) = oneshot::channel::<ClientError>();
        let stream = self.stream(reader, stream_err_tx);
        let rpc_result = self.rjc.client.append(stream).await;
        match stream_err_rx.await {
            Ok(stream_err) => Err(stream_err),
            _ => {
                // The channel was closed along with the stream. Proceed with processing the RPC result.
                match rpc_result {
                    Ok(res) => {
                        let res = res.into_inner();
                        if res.status == pb::Status::Ok as i32 {
                            tracing::debug!("closed");
                            Ok(res)
                        } else {
                            Err(ClientError::Response(res.status))
                        }
                    }
                    Err(status) => Err(ClientError::from(status)),
                }
            }
        }
    }

    /// Create an async `Stream` that is consumed by an Append RPC.
    fn stream<Reader>(
        &self,
        mut reader: Reader,
        stream_err_tx: oneshot::Sender<ClientError>,
    ) -> impl Stream<Item = pb::AppendRequest>
    where
        Reader: Stream<Item = std::io::Result<bytes::Bytes>> + Send + Sync + 'static + Unpin,
    {
        let request = self.request.clone();
        async_stream::stream! {
            // When yielding a request, it will block until the RPC method has fully processed it. That is, if the
            // message is too large, the Append RPC will split it until in chunks draining the message's contents.
            // After that, this method will continue to the next iteration without waiting for the broker's ACK.

            yield request; // Send append request metadata as first message.
            loop {
                match reader.next().await {
                    None => {
                        // Clean EOF of |reader|. Commit by sending empty AppendRequest, then close.
                        yield pb::AppendRequest::default();
                        return;
                    }
                    Some(Ok(content)) => {
                        if content.is_empty() {
                            continue;
                        }

                        // Send non-empty content to broker.
                        yield pb::AppendRequest { content: content.to_vec(), ..Default::default() };
                    }
                    Some(Err(err)) => {
                        // Internal error. Retain it, and return _without_ sending an empty AppendRequest.
                        // The broker will treat this as a roll-back.
                        let _ = stream_err_tx.send(ClientError::from(err));
                        return;
                    }
                }
            }
        }
    }
}
