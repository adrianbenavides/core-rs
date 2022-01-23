use protocol::extensions as pb_ext;
use protocol::generated as pb;

use crate::client::appender::*;

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

    let mut readers = [b"foo".as_slice(), b"bar".as_slice()];
    let res = Appender::append(rjc, req, &mut readers)
        .await
        .expect("Append contents to journal");
    assert_eq!(res.commit.expect("Fragment").journal, journal);
}
