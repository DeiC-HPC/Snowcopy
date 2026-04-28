use tokio::time::sleep;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use std::future::Future;
use std::time::SystemTime;
use crate::cc_util::CommandCenterEvent;


pub async fn retry_async<F, Fut, T, E, ShouldRetry>(
    mut op: F,
    cc_tx: UnboundedSender<CommandCenterEvent>,
    should_retry: ShouldRetry,
    message: String,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    ShouldRetry: Fn(&E) -> bool,
    E: std::fmt::Display,
{
    let mut attempt = 0;

    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if !should_retry(&err) {
                    return Err(err);
                }

                attempt += 1;

                let (tx, rx) = oneshot::channel();
                cc_tx.send(CommandCenterEvent::RetryRequested(tx, attempt, SystemTime::now(), message.clone())).expect("Failed to send message to command center. Should never happen.");

                if let Ok(delay) = rx.await {
                    sleep(delay).await;
                } else {
                    return Err(err);
                }
                
                cc_tx.send(CommandCenterEvent::Log(format!("{}: Error: {}. Retrying (attempt {})...", message, err, attempt))).expect("Failed to send message to command center.");
            }
        }
    }
}
