use std::collections::HashMap;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::sync::Arc;

use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot, Mutex, OwnedMutexGuard};

use append_buffer::*;
use async_append::*;
use protocol::extensions as pb_ext;
use protocol::generated as pb;
pub use service::*;

use crate::client::appender::Appender;
use crate::client::errors::{ClientError, ClientResult};

const APPEND_BUFFER_SIZE: usize = 8 * 1024; // 8KB
const APPEND_BUFFER_CUTOFF: usize = 64 * 1024 * 1024; // 64MB

pub(crate) type SectionStream = tokio_util::io::ReaderStream<tokio::io::Take<tokio::fs::File>>;
type WorkingAppendNode = (Arc<Mutex<AsyncAppendNode>>, OwnedMutexGuard<()>);

/// AsyncJournalClient defines an API for performing asynchronous Append operations.
#[async_trait::async_trait]
trait AsyncJournalClient {
    /// start_append begins a new asynchronous Append RPC. The caller holds exclusive access
    /// to the returned AsyncAppend, and must then:
    ///  * Write content to its Writer.
    ///  * Optionally Require that one or more errors are nil.
    ///  * Release the AsyncAppend, allowing queued writes to commit or,
    ///    if an error occurred, to roll back.
    ///
    /// For performance reasons, an Append will often be batched with other Appends
    /// having identical AppendRequests which are dispatched to this AppendService,
    /// and note the Response.Fragment will reflect the entire batch written to the
    /// broker. In all cases, relative order of Appends is preserved. One or more
    /// dependencies may optionally be supplied. The Append RPC will not begin
    /// until all dependencies have completed without error. A failure of a
    /// dependency will also permanently fail the returned AsyncAppend and prevent
    /// any further appends to this journal from starting. For this reason, a
    /// Future should only fail if it also invalidates the AsyncJournalClient
    /// (eg, because the client is scoped to a context which is invalidated by the
    /// OpFuture failure).
    async fn start_append(
        &mut self,
        request: pb::AppendRequest,
        // TODO
        // dependencies: Future,
    ) -> ClientResult<WorkingAppendNode>;

    // TODO
    // (except: Journal) -> Vec<Future>;
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum JournalClientReq {
    StartAppend {
        res_tx: oneshot::Sender<ClientResult<WorkingAppendNode>>,
        request: pb::AppendRequest,
    },
    RemoveJournal {
        journal: pb_ext::Journal,
    },
    UpdateJournal {
        journal: pb_ext::Journal,
        shared_mutex: Arc<Mutex<()>>,
        append: Arc<Mutex<AsyncAppendNode>>,
    },
}

pub mod service {
    use super::*;

    type AppendsMapValue = (Arc<Mutex<()>>, Arc<Mutex<AsyncAppendNode>>);

    /// AppendService batches, dispatches, and (if needed) retries asynchronous
    /// Append RPCs. Use of an AppendService is appropriate for clients who make
    /// large numbers of small writes to a Journal, and where those writes may be
    /// pipelined and batched to amortize the cost of broker Append RPCs. It may
    /// also simplify implementations for clients who would prefer to simply have
    /// writes block until successfully committed, as opposed to handling errors
    /// and retries themselves.
    ///
    /// For each journal, AppendService manages an ordered list of AsyncAppends,
    /// each having buffered content to be appended. The list is dispatched in
    /// FIFO order by a journal-specific goroutine.
    ///
    /// AsyncAppends are backed by temporary files on the local disk rather
    /// than memory buffers. This minimizes the impact of buffering on the heap
    /// and garbage collector, and also makes AppendService tolerant to sustained
    /// service disruptions (up to the capacity of the disk).
    ///
    /// AppendService implements the AsyncJournalClient trait.
    #[derive(Debug)]
    pub struct AppendService {
        client: pb_ext::RoutedJournalClient,
        appends: HashMap<pb_ext::Journal, AppendsMapValue>,
        // TODO
        // errs: HashMap<pb_ext::Journal, ClientError>,
        req_rx: mpsc::Receiver<JournalClientReq>,
        req_tx: mpsc::Sender<JournalClientReq>,
    }

    impl AppendService {
        pub fn init(client: pb_ext::RoutedJournalClient) -> AppendServiceHandle {
            // TODO: the buffer should match the expected number of concurrent clients?
            let (req_tx, req_rx) = mpsc::channel(100);
            let this = Self {
                client,
                appends: Default::default(),
                req_rx,
                req_tx: req_tx.clone(),
            };
            tokio::spawn(Self::handle(this));
            AppendServiceHandle { req_tx }
        }

        async fn handle(mut this: Self) -> ClientResult<()> {
            while let Some(req) = this.req_rx.recv().await {
                match req {
                    JournalClientReq::StartAppend { res_tx, request } => {
                        let _ = res_tx.send(this.start_append(request).await);
                    }
                    JournalClientReq::RemoveJournal { journal } => {
                        this.appends.remove(&journal);
                    }
                    JournalClientReq::UpdateJournal {
                        journal,
                        shared_mutex,
                        append,
                    } => {
                        this.appends.insert(journal, (shared_mutex, append));
                    }
                }
            }
            Ok(())
        }

        async fn serve_appends(
            req_tex: mpsc::Sender<JournalClientReq>,
            shared_mutex: Arc<Mutex<()>>,
            mut append_node: Arc<Mutex<AsyncAppendNode>>,
        ) -> ClientResult<()> {
            {
                let _shared_lock = shared_mutex.lock().await;
            }
            let mut prev_append_node = append_node.clone();
            loop {
                {
                    let _shared_lock = shared_mutex.lock().await;
                    let mut append_node_lock = append_node.lock().await;
                    // Termination condition: this |append_node| can be immediately resolved without
                    // blocking, and no further appends are queued.
                    if append_node_lock.next.is_none() && append_node_lock.inner.buf.is_none()
                    // && async_append.dependencies.is_empty() // TODO
                    {
                        req_tex
                            .send(JournalClientReq::RemoveJournal {
                                journal: append_node_lock.inner.journal(),
                            })
                            .await?;
                        // Mark |append_node| as completed.
                        append_node_lock.next = Some(append_node.clone());
                        return Ok(());
                    }

                    if append_node_lock.next.is_none() {
                        let next = Arc::new(Mutex::new(append_node_lock.chain()?));
                        append_node_lock.next = Some(next.clone());
                        req_tex
                            .send(JournalClientReq::UpdateJournal {
                                journal: append_node_lock.inner.journal(),
                                shared_mutex: shared_mutex.clone(),
                                append: next,
                            })
                            .await?;
                    }
                }

                // Further appends may queue while we dispatch this RPC.
                {
                    let mut append_node_lock = append_node.lock().await;

                    // TODO: loop dependencies
                    // ...

                    if append_node_lock.inner.buf.is_some() {
                        let stream = append_node_lock.inner.read()?;
                        append_node_lock.inner.appender.append(stream).await?;
                    }
                }

                // Step |append_node| to |append_node.next|. While doing so, clear fields of prior |append_node|
                // not required to represent its final response. This keeps retained
                // AsyncAppends from pinning other resources from the garbage collector.
                //
                // Also set |append_node.next| to itself to denote this AsyncAppend as completed.
                {
                    let _shared_lock = shared_mutex.lock().await;
                    let mut append_node_lock = append_node.lock().await;
                    let next_append_node = append_node_lock
                        .next
                        .clone()
                        .expect("next should be defined");
                    append_node_lock.next = Some(prev_append_node.clone());
                    // async_append_guard.dependencies = None; // TODO
                    append_node_lock.inner.buf = None;
                    drop(append_node_lock);
                    prev_append_node = std::mem::replace(&mut append_node, next_append_node);
                }
            }
        }
    }

    #[async_trait::async_trait]
    impl AsyncJournalClient for AppendService {
        async fn start_append(
            &mut self,
            request: pb::AppendRequest,
        ) -> ClientResult<(Arc<Mutex<AsyncAppendNode>>, OwnedMutexGuard<()>)> {
            // Fetch the current AsyncAppend for |journal|, or start one if none exists.
            let (shared_mutex, mut append_node, is_new_journal) = {
                match self.appends.get(&request.journal) {
                    None => {
                        let shared_mutex = Arc::new(Mutex::new(()));
                        let append_node = Arc::new(Mutex::new(AsyncAppendNode {
                            inner: AsyncAppend::new(self.client.clone(), request.clone())?,
                            next: None,
                        }));
                        self.appends.insert(
                            request.journal.clone(),
                            (shared_mutex.clone(), append_node.clone()),
                        );
                        (shared_mutex, append_node, true)
                    }
                    Some((shared_mutex, append_node)) => {
                        (shared_mutex.clone(), append_node.clone(), false)
                    }
                }
            };

            // Acquire exclusive write access for |journal|. This may race with
            // other writes in progress, and on mutex acquisition a different AsyncAppend
            // may now be current for the journal.
            let shared_lock = {
                let shared_mutex = shared_mutex.clone();
                shared_mutex.lock_owned().await
            };

            // Start the service loop (if needed) *after* we acquire |shared_mutex|, and
            // *before* we skip forward to the current AsyncAppend. This ensures
            // serveAppends starts from the first AsyncAppend of the chain, and
            // that it blocks until the client completes the first write.
            if is_new_journal {
                let shared_mutex = shared_mutex.clone();
                let append_node = append_node.clone();
                let req_tx = self.req_tx.clone();
                tokio::spawn(async move {
                    Self::serve_appends(req_tx, shared_mutex, append_node).await
                });
            }

            // Follow the chain to the current AsyncAppend.
            loop {
                let append_node_lock = append_node.lock().await;
                match append_node_lock.next.clone() {
                    None => {
                        break;
                    }
                    Some(next) => {
                        drop(append_node_lock);
                        append_node = next;

                        // It's possible that (while we were waiting for |shared_mutex|)
                        // serveAppends completed this AsyncAppend, which it marked by setting
                        // |append_node.next| to itself. Recurse to try again.
                        let append_node_lock = append_node.lock().await;
                        if let Some(next) = &append_node_lock.next {
                            let next_append_node_lock = next.lock().await;
                            if append_node_lock.inner == next_append_node_lock.inner {
                                return self.start_append(request.clone()).await;
                            }
                        }
                    }
                }
            }
            {
                // Chain a new Append RPC if this one is over a threshold size.
                let mut append_node_lock = append_node.lock().await;
                if append_node_lock.inner.checkpoint > APPEND_BUFFER_CUTOFF
                    // Or has dependencies which are not a subset of |append_node|'s.
                    // || !dependencies.IsSubsetOf(append_node.dependencies) // TODO
                    // Or if the requests themselves differ.
                    || request != append_node_lock.inner.appender.request
                {
                    let new = Arc::new(Mutex::new(append_node_lock.chain()?));
                    append_node_lock.next = Some(new.clone());
                    self.appends
                        .insert(request.journal.clone(), (shared_mutex.clone(), new.clone()));
                    drop(append_node_lock);
                    append_node = new;
                }
            }
            {
                // Initialize append buffer if needed
                let mut append_node_lock = append_node.lock().await;
                if append_node_lock.inner.buf.is_none() {
                    // This is the first time this AsyncAppend is being returned by
                    // start_append. Initialize its buffer, which also signals that this
                    // |append_node| has been returned by start_append, and that a client may be
                    // waiting on its RPC response.
                    append_node_lock.inner.buf =
                        Some(AppendBuffer::new(append_node_lock.inner.journal())?);
                }
            }
            Ok((append_node, shared_lock))
        }
    }

    #[derive(Debug, Clone)]
    pub struct AppendServiceHandle {
        req_tx: mpsc::Sender<JournalClientReq>,
    }

    impl AppendServiceHandle {
        pub async fn start_append(
            &self,
            request: pb::AppendRequest,
        ) -> ClientResult<(Arc<Mutex<AsyncAppendNode>>, OwnedMutexGuard<()>)> {
            let (res_tx, res_rx) = oneshot::channel();
            self.req_tx
                .send(JournalClientReq::StartAppend { res_tx, request })
                .await?;
            match res_rx.await {
                Err(err) => Err(ClientError::from(err)),
                Ok(res) => res,
            }
        }
    }
}

mod async_append {
    use super::*;

    #[derive(Debug)]
    pub struct AsyncAppend {
        pub appender: Appender,
        pub checkpoint: usize,
        pub buf: Option<AppendBuffer>,
    }

    impl AsyncAppend {
        pub fn new(
            client: pb_ext::RoutedJournalClient,
            req: pb::AppendRequest,
        ) -> ClientResult<Self> {
            Ok(Self {
                appender: Appender::new(client, req)?,
                checkpoint: 0,
                buf: None,
            })
        }

        pub fn journal(&self) -> pb_ext::Journal {
            self.appender.request.journal.clone()
        }

        pub fn write<B: Into<bytes::Bytes>>(
            &mut self,
            contents: B,
        ) -> ClientResult<AppendBufferStatus> {
            match &mut self.buf {
                None => Err(ClientError::Runtime("buffer not initialized".to_string())),
                Some(buf) => buf.write(contents.into()),
            }
        }

        pub fn release(
            &mut self,
            status: AppendBufferStatus,
            shared_lock: OwnedMutexGuard<()>,
        ) -> ClientResult<()> {
            self.checkpoint = status.offset + status.buffer_len;
            drop(shared_lock);
            Ok(())
        }

        pub fn read(&mut self) -> ClientResult<SectionStream> {
            match &mut self.buf {
                None => Err(ClientError::Runtime("buffer not initialized".to_string())),
                Some(buf) => buf.read(self.checkpoint),
            }
        }
    }

    impl PartialEq for AsyncAppend {
        fn eq(&self, other: &Self) -> bool {
            std::ptr::eq(self, other)
        }
    }

    #[derive(Debug)]
    pub struct AsyncAppendNode {
        pub inner: AsyncAppend,
        pub next: Option<Arc<Mutex<Self>>>,
    }

    impl AsyncAppendNode {
        pub fn chain(&self) -> ClientResult<Self> {
            if self.next.is_some() {
                panic!("next is Some")
            }
            Ok(Self {
                inner: AsyncAppend {
                    appender: self.inner.appender.clone(),
                    checkpoint: 0,
                    buf: None,
                },
                next: None,
            })
        }
    }
}

mod append_buffer {
    use crate::client::retry::retry_until_blocking;

    use super::*;

    /// AppendBuffer composes a backing File with a BufWriter, and additionally
    /// tracks the offset through which the file is written.
    #[derive(Debug)]
    pub struct AppendBuffer {
        writer: std::io::BufWriter<tempfile::NamedTempFile>,
        offset: usize,
        journal: Arc<pb_ext::Journal>,
    }

    impl AppendBuffer {
        pub fn new(journal: pb_ext::Journal) -> ClientResult<Self> {
            let file = tempfile::NamedTempFile::new()?;
            Ok(Self {
                writer: std::io::BufWriter::with_capacity(APPEND_BUFFER_SIZE, file),
                offset: 0,
                journal: Arc::new(journal),
            })
        }

        pub fn write(&mut self, buf: bytes::Bytes) -> ClientResult<AppendBufferStatus> {
            self.writer.write_all(&buf)?;
            self.offset += buf.len();
            Ok(AppendBufferStatus {
                offset: self.offset,
                buffer_len: self.writer.buffer().len(),
            })
        }

        pub fn rollback(&mut self, checkpoint: usize) -> ClientResult<AppendBufferStatus> {
            self.offset = checkpoint;
            let journal = self.journal.clone();
            retry_until_blocking(|| self.flush(), &journal, "failed to flush append buffer");
            retry_until_blocking(
                || {
                    self.writer
                        .get_mut()
                        .seek(SeekFrom::Start(self.offset as u64))
                        .map_err(ClientError::from)
                },
                &journal,
                "failed to seek append buffer",
            );
            Ok(AppendBufferStatus {
                offset: self.offset,
                buffer_len: self.writer.buffer().len(),
            })
        }

        pub fn read(&mut self, checkpoint: usize) -> ClientResult<SectionStream> {
            let journal = self.journal.clone();
            retry_until_blocking(|| self.flush(), &journal, "failed to flush append buffer");
            let reader = retry_until_blocking(
                || self.writer.get_ref().reopen().map_err(ClientError::from),
                &journal,
                "failed to reopen backing file",
            );
            let reader = tokio::fs::File::from(reader).take(checkpoint as u64);
            let stream = tokio_util::io::ReaderStream::with_capacity(reader, APPEND_BUFFER_SIZE);
            Ok(stream)
        }

        fn flush(&mut self) -> ClientResult<()> {
            // TODO: handle flush error as in https://github.com/gazette/core/blob/master/broker/client/append_service.go#L453
            self.writer.flush()?;
            Ok(())
        }
    }

    #[derive(Debug)]
    pub struct AppendBufferStatus {
        pub offset: usize,
        pub buffer_len: usize,
    }

    #[cfg(test)]
    mod tests {
        use tokio_stream::StreamExt;

        use super::*;

        #[tokio::test]
        async fn write_and_read() {
            let mut append_buffer = AppendBuffer::new("journal".to_string()).unwrap();

            // write
            let write = append_buffer.write(bytes::Bytes::from("hello")).unwrap();
            assert_eq!(write.offset, 5);
            assert_eq!(write.buffer_len, 5);
            let write = append_buffer.write(bytes::Bytes::from("world")).unwrap();
            assert_eq!(write.offset, 10);
            assert_eq!(write.buffer_len, 10);

            // read
            let stream = append_buffer.read(write.offset).unwrap();
            let stream_contents = read_stream_contents(stream).await;
            assert_eq!(&stream_contents, b"helloworld");
        }

        #[tokio::test]
        async fn write_and_rollback() {
            let mut append_buffer = AppendBuffer::new("journal".to_string()).unwrap();

            // write
            let first_write = append_buffer.write(bytes::Bytes::from("hello")).unwrap();
            assert_eq!(first_write.offset, 5);
            assert_eq!(first_write.buffer_len, 5);
            let second_write = append_buffer.write(bytes::Bytes::from("world")).unwrap();
            assert_eq!(second_write.offset, 10);
            assert_eq!(second_write.buffer_len, 10);

            // rollback
            let after_rollback = append_buffer.rollback(first_write.offset).unwrap();
            assert_eq!(after_rollback.offset, 5);
            assert_eq!(after_rollback.buffer_len, 0);

            // read
            let stream = append_buffer.read(after_rollback.offset).unwrap();
            let stream_contents = read_stream_contents(stream).await;
            assert_eq!(&stream_contents, b"hello");
        }

        async fn read_stream_contents(mut stream: SectionStream) -> Vec<u8> {
            let mut stream_contents = Vec::new();
            while let Some(chunk) = stream.next().await {
                stream_contents.extend_from_slice(&chunk.unwrap());
            }
            stream_contents
        }
    }
}
