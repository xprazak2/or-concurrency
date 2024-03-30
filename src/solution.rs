use crate::statement::*;
use async_trait::async_trait;
use tokio::{
    sync::{
        broadcast::{self, Receiver},
        mpsc::{self, Sender},
    },
    time,
};

#[async_trait]
pub trait Solution {
    async fn solve(repositories: Vec<ServerName>) -> Option<Binary>;
}

pub struct Solution0;

#[derive(Clone)]
pub struct CancelMsg;

#[async_trait]
impl Solution for Solution0 {
    async fn solve(repositories: Vec<ServerName>) -> Option<Binary> {
        let fetcher = Fetcher {};
        solve(repositories, fetcher, 0).await
    }
}

#[async_trait]
trait BinaryFetcher {
    async fn download(&self, server_name: ServerName) -> Result<Binary, ServerError>;
}

#[derive(Clone)]
struct Fetcher;

#[async_trait]
impl BinaryFetcher for Fetcher {
    async fn download(&self, server_name: ServerName) -> Result<Binary, ServerError> {
        download(server_name).await
    }
}

async fn solve<T: BinaryFetcher + Send + Sync + Clone + 'static>(
    repositories: Vec<ServerName>,
    fetcher: T,
    retries_count: u8,
) -> Option<Binary> {
    for retry in 0..=retries_count {
        if let Some(res) = fetch_binary(&repositories, &fetcher).await {
            return Some(res);
        }
        time::sleep(time::Duration::from_secs(2u8.pow(retry.into()).into())).await;
    }
    None
}

async fn fetch_binary<T: BinaryFetcher + Send + Sync + Clone + 'static>(
    repositories: &Vec<ServerName>,
    fetcher: &T,
) -> Option<Binary> {
    let repos_len = repositories.len();
    let (tx, mut rx) = mpsc::channel::<Result<Binary, ServerError>>(repos_len);
    let (cancel_tx, _cancel_rx) = broadcast::channel::<CancelMsg>(1);
    let (cancel_reply_tx, mut cancel_reply_rx) = mpsc::channel::<()>(repos_len);
    let mut handles = Vec::with_capacity(repositories.len());

    for server_name in repositories.iter() {
        let tx_clone = tx.clone();
        let cancel_rx_clone = cancel_tx.subscribe();
        let fetcher_clone = fetcher.clone();
        let server_name_clone = server_name.clone();
        let cancel_reply_tx_clone = cancel_reply_tx.clone();
        let handle = tokio::spawn(async move {
            download_pkg(
                server_name_clone,
                tx_clone,
                cancel_rx_clone,
                fetcher_clone,
                cancel_reply_tx_clone,
            )
            .await
        });

        handles.push(handle);
    }

    let mut error_count = 0;
    let mut confirmed_shutdowns = 0;
    // use newer version of tokio for `sync::mpsc::Receiver::is_closed`
    let mut receiver_closed = false;
    let mut result = None;

    while let Some(msg) = rx.recv().await {
        match msg {
            Ok(binary) => {
                confirmed_shutdowns += 1;
                if !receiver_closed {
                    receiver_closed = true;
                    rx.close();
                    match cancel_tx.send(CancelMsg {}) {
                        Ok(_) => result = Some(binary),
                        Err(_) => {
                            // there are no active receivers, exit
                            return Some(binary);
                        }
                    }
                }
            }
            Err(_) => {
                error_count += 1;
                if error_count == repos_len {
                    return None;
                }
            }
        }
    }

    if confirmed_shutdowns + error_count == repos_len {
        return result;
    }

    // wait for remaining tasks to confirm shutdown
    // this might be less verbose with `tokio_util::sync::CancellationToken` and `tokio_util::task::TaskTracker`
    loop {
        tokio::select! {
            _msg = cancel_reply_rx.recv() => {
                confirmed_shutdowns += 1;
                if confirmed_shutdowns + error_count == repos_len {
                    return result;
                }
            },
            // the timeout interval should be configurable
            _ = time::sleep(time::Duration::from_secs(5)) => {
                // failed to get shutdown confirmation from some of the tasks
                // maybe they completely crashed?
                return result;
            }
        }
    }
}

async fn download_pkg(
    server_name: ServerName,
    tx: Sender<Result<Binary, ServerError>>,
    mut cancel_rx: Receiver<CancelMsg>,
    fetcher: impl BinaryFetcher,
    cancel_reply_tx: Sender<()>,
) {
    tokio::select! {
        cancel_msg = cancel_rx.recv() => {
            match cancel_msg {
                Ok(_) => {
                    // if we get error on send, the receiver is already closed and we want to quit as well
                    let _ = cancel_reply_tx.send(()).await;
                },
                Err(_) => {
                    // close message is sent at most once so we will never get RecvError::Lagged
                    // RecvError::Closed means sender is no longer active so we want to quit as well
                },
            }
        },
        download_res = fetcher.download(server_name.clone()) => {
            let res = tx.send(download_res).await;
            // if we get error on send, the receiver got a binary from other source so we will signal that we are terminating
            match res {
                Ok(_) => (),
                Err(_) => {
                    let _ = cancel_reply_tx.send(()).await;
                },
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::{solve, Binary, ServerError, ServerName};
    use crate::solution::BinaryFetcher;

    #[derive(Clone)]
    struct TestFetcher0 {}

    #[async_trait]
    impl BinaryFetcher for TestFetcher0 {
        async fn download(&self, server_name: ServerName) -> Result<Binary, ServerError> {
            if server_name.0 == "second" {
                Ok(Binary::new(server_name))
            } else {
                Err(ServerError::Disconnected(server_name))
            }
        }
    }

    #[tokio::test]
    async fn test_fetching_from_second_server() {
        let repos = vec![
            ServerName("first".into()),
            ServerName("second".into()),
            ServerName("third".into()),
        ];
        let fetcher = TestFetcher0 {};
        if let Some(res) = solve(repos, fetcher, 0).await {
            assert_eq!(Binary::new(ServerName("second".into())), res)
        } else {
            panic!("should have fetched binary from second server")
        }
    }

    #[derive(Clone)]
    struct TestFetcher1 {}

    #[async_trait]
    impl BinaryFetcher for TestFetcher1 {
        async fn download(&self, server_name: ServerName) -> Result<Binary, ServerError> {
            Err(ServerError::Disconnected(server_name))
        }
    }

    #[tokio::test]
    async fn test_fetching_failure_with_retry() {
        let repos = vec![ServerName("first".into())];
        let fetcher = TestFetcher1 {};
        assert_eq!(None, solve(repos, fetcher, 1).await)
    }
}
