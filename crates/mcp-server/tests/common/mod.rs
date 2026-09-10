//! What more than one end-to-end binary needs and none of them owns.

use std::future::Future;
use std::time::Duration;

use serde_json::Value;

/// How long an answer that depends on a write reaching the resident is waited for. Long
/// enough that a starved watcher thread is not mistaken for a broken one; short enough
/// that a genuinely broken mechanism still ends the test rather than the suite's clock.
const DELIVERY_DEADLINE: Duration = Duration::from_secs(30);

/// How often the question is asked again. Cheap next to the walk an answer may cost, and
/// far above the storm floor a forced re-scan is bounded by.
const ASK_AGAIN_EVERY: Duration = Duration::from_millis(200);

/// Ask until the answer is the one the test asserts, or fail saying it never came.
///
/// The shape every gate over a post-startup disk write needs. Such a write reaches the
/// resident by two mechanisms, and BOTH can be late: the change hub's drain runs on a
/// watcher thread the scheduler may starve, and a forced re-scan waits out a storm floor
/// before it walks. A single call is therefore a race — and a race in a smoke test is a
/// test that fails for something it does not measure.
///
/// This does not soften anything. `accept` is the test's own claim, so an answer that
/// never satisfies it still fails, with the last answer quoted; a claim about what must
/// STAY absent is not asked this way at all — one call already settles it, and asking
/// again could only be answered by the thing the control says is not there.
pub async fn settled<Ask, Fut>(
    expected: &str,
    accept: impl Fn(&Value) -> bool,
    mut ask: Ask,
) -> Value
where
    Ask: FnMut() -> Fut,
    Fut: Future<Output = Value>,
{
    let deadline = tokio::time::Instant::now() + DELIVERY_DEADLINE;
    loop {
        let answer = ask().await;
        if accept(&answer) {
            return answer;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{expected}, and {DELIVERY_DEADLINE:?} of asking never said so: {answer}",
        );
        tokio::time::sleep(ASK_AGAIN_EVERY).await;
    }
}
