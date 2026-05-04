use tokio::time::sleep;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use std::future::Future;
use std::time::SystemTime;
use crate::cc_util::CommandCenterEvent;


/// ```text
/// Asynchronously retries a given operation upon failure based on the specified retry condition.
/// The idea is that one can warp fucntions in this and provide information about which errors should be retired.
/// The reason that the command_center is involved is to provide a centralized logging and monitoring system for retry operations.
///
/// # Parameters
///
/// - `op`: A closure that produces a future representing the asynchronous operation to be executed.
///         The closure is invoked each time the operation is retried.
/// - `cc_tx`: An `UnboundedSender` used to communicate retry events and logging information with the command center.
/// - `should_retry`: A function that takes a reference to an error and returns `true` if the operation should be retried,
///                   or `false` otherwise.
/// - `message`: A `String` that describes the operation being retried, used for logging and monitoring purposes.
///
/// # Returns
///
/// - Returns `Ok(T)` if the operation succeeds.
/// - Returns `Err(E)` if the operation fails and the retry condition (`should_retry`) is not met or other issues occur.
///
/// ```
pub async fn retry_on_failure_async<F, Fut, T, E, ShouldRetry>(
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
