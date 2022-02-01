use std::io::prelude::*;
use std::io::{BufWriter, SeekFrom};

use std::sync::mpsc;

use crate::client::appender::StreamSection;
use crate::client::errors::{ClientError, ClientResult};

const APPEND_BUFFER_SIZE: usize = 8 * 1024; // 8KB

#[derive(Debug)]
struct AppendBuffer;

impl AppendBuffer {
    pub fn init() -> ClientResult<AppendBufferHandle> {
        // TODO: named temp files have some security concerns (see docs), should they be written outside of temp directories?
        let file = tempfile::NamedTempFile::new()?;
        let (req_tx, req_rx) = mpsc::channel::<AppendBufferReq>();
        tokio::task::spawn_blocking(move || Self::handle_requests(file, req_rx));
        Ok(AppendBufferHandle { req_tx })
    }

    fn handle_requests(file: tempfile::NamedTempFile, req_rx: mpsc::Receiver<AppendBufferReq>) {
        let mut offset = 0;
        let mut writer = std::io::BufWriter::with_capacity(APPEND_BUFFER_SIZE, file);
        while let Ok(op) = req_rx.recv() {
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
    fn write<B: Into<bytes::Bytes>>(&self, buf: B) -> ClientResult<AppendBufferStatus> {
        let (res_tx, res_rx) = mpsc::channel::<ClientResult<AppendBufferStatus>>();
        self.req_tx.send(AppendBufferReq::Write {
            res_tx,
            buf: buf.into(),
        })?;
        match res_rx.recv() {
            Err(err) => Err(ClientError::from(err)),
            Ok(res) => res,
        }
    }

    fn rollback(&self, checkpoint: usize) -> ClientResult<AppendBufferStatus> {
        let (res_tx, res_rx) = mpsc::channel::<ClientResult<AppendBufferStatus>>();
        self.req_tx
            .send(AppendBufferReq::Rollback { res_tx, checkpoint })?;
        match res_rx.recv() {
            Err(err) => Err(ClientError::from(err)),
            Ok(res) => res,
        }
    }

    fn read(
        &mut self,
        checkpoint: usize,
    ) -> ClientResult<StreamSection<tokio_util::io::ReaderStream<tokio::fs::File>>> {
        let (res_tx, res_rx) = mpsc::channel::<
            ClientResult<StreamSection<tokio_util::io::ReaderStream<tokio::fs::File>>>,
        >();
        self.req_tx
            .send(AppendBufferReq::Read { res_tx, checkpoint })?;
        match res_rx.recv() {
            Err(err) => Err(ClientError::from(err)),
            Ok(res) => res,
        }
    }
}

#[derive(Debug)]
enum AppendBufferReq {
    Write {
        res_tx: mpsc::Sender<ClientResult<AppendBufferStatus>>,
        buf: bytes::Bytes,
    },
    Rollback {
        res_tx: mpsc::Sender<ClientResult<AppendBufferStatus>>,
        checkpoint: usize,
    },
    Read {
        res_tx: mpsc::Sender<
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
        let mut append_buffer = AppendBuffer::init().unwrap();

        // write
        let write = append_buffer.write("hello").unwrap();
        assert_eq!(write.offset, 5);
        assert_eq!(write.buffer_len, 5);
        let write = append_buffer.write("world").unwrap();
        assert_eq!(write.offset, 10);
        assert_eq!(write.buffer_len, 10);

        // read
        let stream = append_buffer.read(write.offset).unwrap();
        let stream_contents = stream.to_vec().await;
        assert_eq!(&stream_contents, b"helloworld");
    }

    #[tokio::test]
    async fn write_and_rollback() {
        let mut append_buffer = AppendBuffer::init().unwrap();

        // write
        let first_write = append_buffer.write("hello").unwrap();
        assert_eq!(first_write.offset, 5);
        assert_eq!(first_write.buffer_len, 5);
        let second_write = append_buffer.write("world").unwrap();
        assert_eq!(second_write.offset, 10);
        assert_eq!(second_write.buffer_len, 10);

        // rollback
        let after_rollback = append_buffer.rollback(first_write.offset).unwrap();
        assert_eq!(after_rollback.offset, 5);
        assert_eq!(after_rollback.buffer_len, 0);

        // read
        let stream = append_buffer.read(after_rollback.offset).unwrap();
        let stream_contents = stream.to_vec().await;
        assert_eq!(&stream_contents, b"hello");
    }

    #[tokio::test]
    async fn write_and_read_from_different_tasks() {
        let mut append_buffer = AppendBuffer::init().unwrap();

        // write
        let append_buffer_moved = append_buffer.clone();
        tokio::spawn(async move {
            append_buffer_moved.write("hello").unwrap();
            append_buffer_moved.write("world").unwrap();
        })
        .await
        .unwrap();

        // write
        let append_buffer_moved = append_buffer.clone();
        tokio::spawn(async move {
            append_buffer_moved.write("foo").unwrap();
            append_buffer_moved.write("bar").unwrap();
        })
        .await
        .unwrap();

        // read
        let expected_contents = b"helloworldfoobar";
        let stream = append_buffer.read(expected_contents.len()).unwrap();
        let stream_contents = stream.to_vec().await;
        assert_eq!(&stream_contents, expected_contents);
    }
}
