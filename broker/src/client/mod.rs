use errors::ClientResult;

pub mod appender;
#[cfg(test)]
mod appender_test;
mod errors;
mod retry;
#[cfg(test)]
mod retry_test;
