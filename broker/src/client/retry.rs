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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_durations() {
        let mut test_cases = [
            (0, tokio::time::Duration::from_millis(50)),
            (1, tokio::time::Duration::from_millis(50)),
            (2, tokio::time::Duration::from_millis(100)),
            (3, tokio::time::Duration::from_millis(100)),
            (4, tokio::time::Duration::from_secs(1)),
            (5, tokio::time::Duration::from_secs(1)),
            (6, tokio::time::Duration::from_secs(5)),
            (20, tokio::time::Duration::from_secs(5)),
        ];
        for (attempt, duration) in test_cases.iter_mut() {
            let prev_attempt = *attempt;
            assert_eq!(&backoff(attempt), duration);
            assert_eq!(*attempt, prev_attempt + 1);
        }
    }
}
