use broker::client::appender::{Appender, StreamSection};
use broker::client::errors::ClientError;
use protocol::extensions as pb_ext;
use protocol::generated as pb;
use std::io::Write;

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
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(b"foo".as_slice()).unwrap();
    let reader = tokio::fs::File::from(file.reopen().unwrap());
    let stream = StreamSection::new(tokio_util::io::ReaderStream::new(reader), 3);
    let mut appender = Appender::new(rjc, req).unwrap();
    let res = appender
        .append(stream)
        .await
        .expect("Append contents to journal");
    assert_eq!(res.commit.expect("fragment").journal, journal);
}
