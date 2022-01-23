use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("validation error")]
    Validation {
        msg: String,
        #[source]
        source: Option<tonic::codegen::StdError>,
    },
}
