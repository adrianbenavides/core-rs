use protocol::extensions as pb_ext;
use protocol::generated as pb;

use broker::client::append_service::{AppendService, AppendServiceHandle};

#[tokio::test]
#[ignore]
async fn append_to_same_journal_from_different_tasks() {
    let client = pb::JournalClient::connect("http://localhost:8080")
        .await
        .expect("Connect to server");
    let rjc = pb_ext::RoutedJournalClient::new(client);
    let append_service = AppendService::init(rjc);
    let req = pb::AppendRequest {
        journal: "test/journal".to_string(),
        ..Default::default()
    };
    tokio::join!(
        do_append("hello".to_string(), append_service.clone(), req.clone()),
        do_append("world".to_string(), append_service, req)
    );
    // Give some time for the append_service to finish.
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
}

async fn do_append(msg: String, append_service: AppendServiceHandle, req: pb::AppendRequest) {
    let (append, shared_lock) = append_service.start_append(req).await.unwrap();
    let mut append = append.lock().await;
    let status = append.inner.write(msg).unwrap();
    append.inner.release(status, shared_lock).unwrap();
}
