use crate::generated as pb;

#[derive(Debug, Clone)]
pub struct RoutedJournalClient {
    pub client: pb::JournalClient<tonic::transport::Channel>,
}

impl RoutedJournalClient {
    pub fn new(client: pb::JournalClient<tonic::transport::Channel>) -> Self {
        Self { client }
    }
}
