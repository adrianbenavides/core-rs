use std::collections::HashMap;

use tokio::sync::RwLock;

use protocol::extensions as pb_ext;
use protocol::generated as pb;

struct AppendService<'a> {
    client: pb_ext::RoutedJournalClient,
    // do we need any of the following maps sorted?
    appends: HashMap<pb_ext::Journal, &'a AsyncAppend>,
    errs: RwLock<HashMap<pb_ext::Journal, dyn std::error::Error>>, // ClientErrors only? or does this receive errors from other domains?
}

struct AsyncAppend {}
