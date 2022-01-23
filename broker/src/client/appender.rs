use futures_util::Stream;
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot};
use tonic::Code;

use protocol::extensions as pb_ext;
use protocol::generated as pb;

use crate::client::errors::ClientError;
use crate::client::retry::backoff;
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
    // Channel where new chunks are written to the Append RPC stream.
    buf_tx: mpsc::Sender<StreamOp>,
    // Channel to receive the Append RPC response after the broker processes the stream.
    res_rx: oneshot::Receiver<Result<tonic::Response<pb::AppendResponse>, tonic::Status>>,
}

impl Appender {
    /// Returns an Appender of the given AppendRequest with an async stream ready to process requests.
    fn new(mut rjc: pb_ext::RoutedJournalClient, request: pb::AppendRequest) -> ClientResult<Self> {
        // TODO: implement validator
        // The initial request must have a non-empty `journal` to initiate the Append RPC.
        if request.journal.is_empty() {
            return Err(pb_ext::ProtocolError::Validation {
                msg: "expected Request.Journal".to_string(),
                source: None,
            }
            .into());
        }

        let (res_tx, res_rx) = oneshot::channel();
        let (buf_tx, buf_rx) = mpsc::channel(1_000);
        tokio::spawn(async move {
            let stream = Self::stream(request, buf_rx);
            let res = rjc.client.append(stream).await;
            let _ = res_tx.send(res);
        });

        Ok(Self { buf_tx, res_rx })
    }

    /// Append zero or more `AsyncRead`s to a journal as a single Append transaction.
    /// Append retries on transport or routing errors, but fails on all other errors.
    /// Each `AsyncRead` is read from byte zero until EOF, and may be read multiple times.
    /// If no `AsyncRead`s are provided, an Append RPC with no content is issued.
    #[tracing::instrument(level="debug", skip(rjc, request, readers), fields(journal=%request.journal))]
    pub async fn append<Reader>(
        rjc: pb_ext::RoutedJournalClient,
        request: pb::AppendRequest,
        readers: &mut [Reader],
    ) -> ClientResult<pb::AppendResponse>
    where
        Reader: tokio::io::AsyncRead + Send + Sync + 'static + Unpin,
    {
        tracing::info!("initiating append");
        let mut write_buf = Vec::new();
        let mut attempt = 0;
        loop {
            // TODO: how to handle "stream closed by broker" error?
            let mut appender = Self::new(rjc.clone(), request.clone())?;
            appender.init().await?;
            for reader in &mut *readers {
                reader.read_to_end(&mut write_buf).await?;
                appender.write(write_buf.drain(..).collect()).await?;
            }
            tracing::debug!("writes buffered");
            match appender.close().await {
                Ok(res) => {
                    return Ok(res);
                }
                Err(err) => {
                    tracing::debug!("close failed [err={err}]");
                    let mut should_retry = false;
                    match &err {
                        ClientError::Grpc(status) => {
                            should_retry = status.code() == Code::Unavailable;
                        }
                        ClientError::Response(code) => {
                            should_retry = *code == pb::Status::NoJournalPrimaryBroker as i32;
                        }
                        _ => {}
                    }
                    if should_retry {
                        tokio::time::sleep(backoff(&mut attempt)).await;
                        tracing::info!("retrying [attempt={attempt}")
                    } else {
                        return Err(err);
                    }
                }
            }
        }
    }

    /// Initialize an Append RPC by sending a first request with only the `journal` field defined.
    async fn init(&mut self) -> ClientResult<()> {
        self.buf_tx.send(StreamOp::Init).await?;
        Ok(())
    }

    /// Write a chunk of bytes to the `Appender` async stream. The stream must be initialized
    /// calling the `init` method before doing any write.
    async fn write(&mut self, buf: Vec<u8>) -> ClientResult<()> {
        // Skip empty chunks as the broker interprets them as "commit".
        if buf.is_empty() {
            return Ok(());
        }
        self.buf_tx.send(StreamOp::Write(buf)).await?;
        Ok(())
    }

    /// Close the `Appender` async stream by sending a final request with no contents to signal
    /// the broker that the previous writes can be committed. Returns the broker response with
    /// the result of the Append RPC.  
    async fn close(self) -> ClientResult<pb::AppendResponse> {
        self.buf_tx.send(StreamOp::Close).await?;
        let res = match self.res_rx.await? {
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
        };
        res
    }

    async fn abort(self) -> ClientResult<()> {
        self.buf_tx.send(StreamOp::Abort).await?;
        Ok(())
    }

    /// Create an async `Stream` that is consumed by an Append RPC.
    #[tracing::instrument(level="trace", skip(request, buf_rx), fields(journal=%request.journal))]
    fn stream(
        mut request: pb::AppendRequest,
        mut buf_rx: mpsc::Receiver<StreamOp>,
    ) -> impl Stream<Item = pb::AppendRequest> {
        async_stream::stream! {
            let journal = request.journal.clone();
            request.journal.clear();
            loop {
                let mut request = request.clone();
                // The Append RPC won't try to read the next stream message until it doesn't fully processed. If the message is
                // too large, the Append RPC will split it into multiple Frames and Windows
                match buf_rx.recv().await {
                    // The channel has been closed.
                    None => {
                        tracing::warn!("buf channel was closed");
                        break;
                    }
                    Some(op) => {
                        // When yielding a request, it will block until the RPC method has fully processed it. That is, if the
                        // message is too large, the Append RPC will split it into multiple Frames and Windows until draining
                        // the message's contents. After that, this method will continue to the next iteration without waiting
                        // for the broker's ACK.
                        match op {
                            StreamOp::Init => {
                                // Send initial request with the `journal` field filled and the `content` field empty.
                                // Following write requests will have the `journal` field empty and the `content` field filled.
                                request.journal = journal.clone();
                                yield request;
                                tracing::debug!("init request processed");
                            }
                            StreamOp::Write(buf) => {
                                // Send write request.
                                request.content = buf;
                                yield request;
                                tracing::trace!("write request processed");
                            }
                            StreamOp::Close => {
                                // Close channel, stopping new messages from being buffered and blocking until its buffer is drained.
                                buf_rx.close();

                                // Send last request with no `content to indicate the broker to commit previous writes.
                                request.content.clear();
                                yield request;
                                tracing::debug!("close request processed");

                                // Close stream.
                                break;
                            }
                            StreamOp::Abort => {
                                tracing::debug!("aborting stream");
                                // Close channel, stopping new messages from being buffered and blocking until its buffer is drained.
                                buf_rx.close();

                                // Close stream.
                                break;
                            }
                        }
                    }
                }
            }
            tracing::debug!("stream closed");
        }
    }
}

#[derive(Debug)]
enum StreamOp {
    Init,
    Write(Vec<u8>),
    Close,
    Abort,
}

#[cfg(test)]
mod private_tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn error_if_create_instance_with_no_journal() {
        let client = pb::JournalClient::connect("http://localhost:8080")
            .await
            .expect("Connect to server");
        let rjc = pb_ext::RoutedJournalClient::new(client);
        let req = pb::AppendRequest {
            journal: "".to_string(),
            ..Default::default()
        };
        let appender = Appender::new(rjc, req);
        assert!(matches!(
            appender.unwrap_err(),
            ClientError::Protocol(pb_ext::ProtocolError::Validation { .. })
        ));
    }

    #[tokio::test]
    #[ignore]
    async fn commits_successfully() {
        let client = pb::JournalClient::connect("http://localhost:8080")
            .await
            .expect("Connect to server");
        let rjc = pb_ext::RoutedJournalClient::new(client);
        let journal = "test/journal".to_string();
        let req = pb::AppendRequest {
            journal: journal.clone(),
            ..Default::default()
        };
        let mut appender = Appender::new(rjc, req).expect("create Appender instance");
        appender.init().await.unwrap();
        appender.write(b"foo".to_vec()).await.unwrap();
        appender.write(b"bar".to_vec()).await.unwrap();
        let res = appender.close().await.unwrap();
        assert_eq!(res.commit.expect("committed fragment").journal, journal);
    }
}
