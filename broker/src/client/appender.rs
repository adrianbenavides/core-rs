use std::cmp::min;

use tokio::sync::oneshot;
use tokio_stream::{Stream, StreamExt};

use protocol::extensions as pb_ext;
use protocol::generated as pb;

use crate::client::{ClientError, ClientResult};

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
#[derive(Debug, Clone)]
pub struct Appender {
    rjc: pb_ext::RoutedJournalClient,
    pub(crate) request: pb::AppendRequest,
}

impl Appender {
    /// Returns an Appender of the given AppendRequest with an async stream ready to process requests.
    pub fn new(rjc: pb_ext::RoutedJournalClient, request: pb::AppendRequest) -> ClientResult<Self> {
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
    pub async fn append<S>(&mut self, stream: StreamSection<S>) -> ClientResult<pb::AppendResponse>
    where
        S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + Sync + 'static + Unpin,
    {
        let (stream_err_tx, stream_err_rx) = oneshot::channel::<ClientError>();
        let stream = self.stream(stream, stream_err_tx);
        let rpc_result = self.rjc.client.append(stream).await;
        match (stream_err_rx.await, rpc_result) {
            (Ok(stream_err), _) => Err(stream_err),
            (_, Err(status)) => Err(ClientError::from(status)),
            (_, Ok(res)) => {
                let res = res.into_inner();
                if res.status == pb::Status::Ok as i32 {
                    tracing::debug!("closed");
                    Ok(res)
                } else {
                    Err(ClientError::Response(res.status))
                }
            }
        }
    }

    /// Create an async `Stream` that is consumed by an Append RPC.
    fn stream<S>(
        &self,
        mut stream: StreamSection<S>,
        stream_err_tx: oneshot::Sender<ClientError>,
    ) -> impl Stream<Item = pb::AppendRequest>
    where
        S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + Sync + 'static + Unpin,
    {
        let request = self.request.clone();
        async_stream::stream! {
            // When yielding a request, it will block until the RPC method has fully processed it. That is, if the
            // message is too large, the Append RPC will split it until in chunks draining the message's contents.
            // After that, this method will continue to the next iteration without waiting for the broker's ACK.

            yield request; // Send append request metadata as first message.
            loop {
                match stream.inner.next().await {
                    None => {
                        // Clean EOF of |stream|. Commit by sending empty AppendRequest, then close.
                        yield pb::AppendRequest::default();
                        return;
                    }
                    Some(Ok(content)) => {
                        if content.is_empty() {
                            continue;
                        }

                        match stream.read(content) {
                            None => {
                                // EOF of |stream| section. Commit by sending empty AppendRequest, then close.
                                yield pb::AppendRequest::default();
                                return;
                            },
                            Some(content) => {
                                // Send non-empty content to broker.
                                yield pb::AppendRequest { content, ..Default::default() };
                            }
                        }
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

#[derive(Debug)]
pub struct StreamSection<S>
where
    S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + Sync + 'static + Unpin,
{
    inner: S,
    bytes_read: usize,
    checkpoint: usize,
}

impl<S> StreamSection<S>
where
    S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + Sync + 'static + Unpin,
{
    pub fn new(inner: S, checkpoint: usize) -> Self {
        Self {
            inner,
            bytes_read: 0,
            checkpoint,
        }
    }

    fn read(&mut self, content: bytes::Bytes) -> Option<Vec<u8>> {
        let n_to_read = min(content.len(), self.checkpoint - self.bytes_read);
        if n_to_read == 0 {
            // End of section
            None
        } else {
            self.bytes_read += n_to_read;
            Some(
                content
                    .get(..n_to_read)
                    .expect("should have bytes in sub-range")
                    .to_vec(),
            )
        }
    }

    #[cfg(test)]
    pub async fn to_vec(mut self) -> Vec<u8> {
        let mut stream_contents = Vec::new();
        while let Some(chunk) = self.inner.next().await {
            if let Some(chunk) = &self.read(chunk.unwrap()) {
                stream_contents.extend_from_slice(chunk);
            }
        }
        stream_contents
    }
}
