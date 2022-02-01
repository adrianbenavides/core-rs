use std::io::prelude::*;
use std::io::{BufWriter, SeekFrom};

use tokio::sync::{mpsc, oneshot};

use crate::client::appender::StreamSection;
use crate::client::errors::{ClientError, ClientResult};

const APPEND_BUFFER_SIZE: usize = 8 * 1024; // 8KB

#[derive(Debug)]
struct AppendBuffer;

impl AppendBuffer {
    pub async fn init() -> ClientResult<AppendBufferHandle> {
        // TODO: named temp files have some security concerns (see docs), should they be written outside of temp directories?
        let file = tempfile::NamedTempFile::new()?;
        let (req_tx, req_rx) = mpsc::channel::<AppendBufferReq>(1);
        tokio::spawn(Self::handle_requests(file, req_rx));
        Ok(AppendBufferHandle { req_tx })
    }

    async fn handle_requests(
        file: tempfile::NamedTempFile,
        mut req_rx: mpsc::Receiver<AppendBufferReq>,
    ) {
        let mut offset = 0;
        let mut writer = std::io::BufWriter::with_capacity(APPEND_BUFFER_SIZE, file);
        while let Some(op) = req_rx.recv().await {
            match op {
                AppendBufferReq::Write { res_tx, buf } => {
                    let _ = res_tx.send(Self::write(&mut writer, &mut offset, buf));
                }
                AppendBufferReq::Rollback { res_tx, checkpoint } => {
                    let _ = res_tx.send(Self::rollback(&mut writer, &mut offset, checkpoint));
                }
                AppendBufferReq::Read { res_tx, checkpoint } => {
                    let _ = res_tx.send(Self::read(writer, checkpoint));
                    break;
                }
            };
        }
    }

    fn write(
        writer: &mut BufWriter<tempfile::NamedTempFile>,
        offset: &mut usize,
        buf: bytes::Bytes,
    ) -> ClientResult<AppendBufferStatus> {
        writer.write_all(&buf)?;
        *offset += buf.len();
        Ok(AppendBufferStatus {
            offset: *offset,
            buffer_len: writer.buffer().len(),
        })
    }

    fn rollback(
        writer: &mut BufWriter<tempfile::NamedTempFile>,
        offset: &mut usize,
        checkpoint: usize,
    ) -> ClientResult<AppendBufferStatus> {
        *offset = checkpoint;
        writer.seek(SeekFrom::Start(*offset as u64))?; // Flush and seek
        Ok(AppendBufferStatus {
            offset: *offset,
            buffer_len: writer.buffer().len(),
        })
    }

    fn read(
        writer: BufWriter<tempfile::NamedTempFile>,
        checkpoint: usize,
    ) -> ClientResult<StreamSection<tokio_util::io::ReaderStream<tokio::fs::File>>> {
        // TODO: handle flush error as in https://github.com/gazette/core/blob/master/broker/client/append_service.go#L453
        let reader = writer.into_inner()?.reopen()?; // Flush buffer into File and reopen to read contents.
        let reader = tokio::fs::File::from(reader);
        let stream = StreamSection::new(
            tokio_util::io::ReaderStream::with_capacity(reader, APPEND_BUFFER_SIZE),
            checkpoint,
        );
        Ok(stream)
    }
}

#[derive(Debug, Clone)]
struct AppendBufferHandle {
    req_tx: mpsc::Sender<AppendBufferReq>,
}

impl AppendBufferHandle {
    async fn write<B: Into<bytes::Bytes>>(&self, buf: B) -> ClientResult<AppendBufferStatus> {
        let (res_tx, res_rx) = oneshot::channel::<ClientResult<AppendBufferStatus>>();
        self.req_tx
            .send(AppendBufferReq::Write {
                res_tx,
                buf: buf.into(),
            })
            .await?;
        match res_rx.await {
            Err(err) => Err(ClientError::from(err)),
            Ok(res) => res,
        }
    }

    async fn rollback(&self, checkpoint: usize) -> ClientResult<AppendBufferStatus> {
        let (res_tx, res_rx) = oneshot::channel::<ClientResult<AppendBufferStatus>>();
        self.req_tx
            .send(AppendBufferReq::Rollback { res_tx, checkpoint })
            .await?;
        match res_rx.await {
            Err(err) => Err(ClientError::from(err)),
            Ok(res) => res,
        }
    }

    async fn read(
        &mut self,
        checkpoint: usize,
    ) -> ClientResult<StreamSection<tokio_util::io::ReaderStream<tokio::fs::File>>> {
        let (res_tx, res_rx) = oneshot::channel::<
            ClientResult<StreamSection<tokio_util::io::ReaderStream<tokio::fs::File>>>,
        >();
        self.req_tx
            .send(AppendBufferReq::Read { res_tx, checkpoint })
            .await?;
        match res_rx.await {
            Err(err) => Err(ClientError::from(err)),
            Ok(res) => res,
        }
    }
}

#[derive(Debug)]
enum AppendBufferReq {
    Write {
        res_tx: oneshot::Sender<ClientResult<AppendBufferStatus>>,
        buf: bytes::Bytes,
    },
    Rollback {
        res_tx: oneshot::Sender<ClientResult<AppendBufferStatus>>,
        checkpoint: usize,
    },
    Read {
        res_tx: oneshot::Sender<
            ClientResult<StreamSection<tokio_util::io::ReaderStream<tokio::fs::File>>>,
        >,
        checkpoint: usize,
    },
}

#[derive(Debug)]
struct AppendBufferStatus {
    offset: usize,
    buffer_len: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn write_and_read() {
        let mut append_buffer = AppendBuffer::init().await.unwrap();

        // write
        let write = append_buffer.write("hello").await.unwrap();
        assert_eq!(write.offset, 5);
        assert_eq!(write.buffer_len, 5);
        let write = append_buffer.write("world").await.unwrap();
        assert_eq!(write.offset, 10);
        assert_eq!(write.buffer_len, 10);

        // read
        let stream = append_buffer.read(write.offset).await.unwrap();
        let stream_contents = stream.to_vec().await;
        assert_eq!(&stream_contents, b"helloworld");
    }

    #[tokio::test]
    async fn write_and_rollback() {
        let mut append_buffer = AppendBuffer::init().await.unwrap();

        // write
        let first_write = append_buffer.write("hello").await.unwrap();
        assert_eq!(first_write.offset, 5);
        assert_eq!(first_write.buffer_len, 5);
        let second_write = append_buffer.write("world").await.unwrap();
        assert_eq!(second_write.offset, 10);
        assert_eq!(second_write.buffer_len, 10);

        // rollback
        let after_rollback = append_buffer.rollback(first_write.offset).await.unwrap();
        assert_eq!(after_rollback.offset, 5);
        assert_eq!(after_rollback.buffer_len, 0);

        // read
        let stream = append_buffer.read(after_rollback.offset).await.unwrap();
        let stream_contents = stream.to_vec().await;
        assert_eq!(&stream_contents, b"hello");
    }

    #[tokio::test]
    async fn write_and_read_from_different_tasks() {
        let mut append_buffer = AppendBuffer::init().await.unwrap();

        // write
        let append_buffer_moved = append_buffer.clone();
        tokio::spawn(async move {
            append_buffer_moved.write("hello").await.unwrap();
            append_buffer_moved.write("world").await.unwrap();
        })
        .await
        .unwrap();

        // write
        let append_buffer_moved = append_buffer.clone();
        tokio::spawn(async move {
            append_buffer_moved.write("foo").await.unwrap();
            append_buffer_moved.write("bar").await.unwrap();
        })
        .await
        .unwrap();

        // read
        let expected_contents = b"helloworldfoobar";
        let stream = append_buffer.read(expected_contents.len()).await.unwrap();
        let stream_contents = stream.to_vec().await;
        assert_eq!(&stream_contents, expected_contents);
    }
}
