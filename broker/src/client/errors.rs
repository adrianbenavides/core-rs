use std::fmt::Debug;

use thiserror::Error;

use protocol::extensions as pb_ext;

pub type ClientResult<T> = Result<T, ClientError>;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("gRPC error")]
    Grpc(tonic::Status),

    #[error("response error")]
    Response(i32),

    #[error("channel error")]
    Channel(String),

    #[error(transparent)]
    Protocol(#[from] pb_ext::ProtocolError),

    #[error(transparent)]
    IO(#[from] std::io::Error),
}

mod third_party {
    use super::*;

    impl From<tonic::Status> for ClientError {
        fn from(status: tonic::Status) -> Self {
            assert_ne!(status.code(), tonic::Code::Ok);
            Self::Grpc(status)
        }
    }

    impl<T> From<tokio::sync::mpsc::error::SendError<T>> for ClientError
    where
        T: Debug,
    {
        fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
            println!("{:?}", err.0);
            Self::Channel(err.to_string())
        }
    }

    impl From<tokio::sync::oneshot::error::RecvError> for ClientError {
        fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
            Self::Channel(err.to_string())
        }
    }
}
