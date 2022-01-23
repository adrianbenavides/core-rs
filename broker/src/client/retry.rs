pub fn backoff(attempt: &mut i32) -> tokio::time::Duration {
    let duration = match attempt {
        0 | 1 => tokio::time::Duration::from_millis(50),
        2 | 3 => tokio::time::Duration::from_millis(100),
        4 | 5 => tokio::time::Duration::from_secs(1),
        _ => tokio::time::Duration::from_secs(5),
    };
    *attempt += 1;
    duration
}
