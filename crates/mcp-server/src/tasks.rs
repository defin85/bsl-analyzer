//! The second branch of execution, `io.modelcontextprotocol/tasks`.
//!
//! A resident-reading tool has two ways to answer a caller that arrives while the analysis
//! database is still being built. The synchronous branch publishes a `loading` envelope and
//! asks the caller to come back. The branch here hands back a durable handle instead: the
//! work is queued on the daemon, the caller polls `tasks/get`, and what it eventually reads
//! is the real answer — never a repackaged `loading` body.
//!
//! Which branch a caller gets is the caller's own choice. Only a client that declared the
//! extension can be handed a task, and the decision is taken here rather than left to the
//! dispatcher: the dispatcher's own guard replaces a task result for a non-declaring client
//! with `-32021`, so leaning on it would turn today's `loading` answer into an error for
//! every client that never asked for tasks.
//!
//! The registry lives on [`crate::SharedState`], one per daemon backend, so a handle
//! outlives the connection that created it. It does not outlive the process: a daemon that
//! exits on its idle timer takes its handles with it, and a later `tasks/get` is answered
//! `-32602`, the same as any unknown id.

use std::time::Duration;

use rmcp::handler::server::common::{AsRequestContext, FromContextPart};
use rmcp::model::{CallToolResponse, CallToolResult, CreateTaskResult};
use rmcp::task_manager::{TaskContext, TaskExit, TaskOptions};
use rmcp::ErrorData as McpError;

use crate::diagnostics_state::{
    resident_call, CallOutcome, DiagnosticsState, DiagnosticsStatus, ResidentSession,
};
use crate::tools::location::ReasonCode;
use crate::McpServer;

/// How long a handle stays addressable. The SDK's own default; restated here because the
/// deadline below is derived from it and a silent change of one must move the other.
const DEFAULT_TTL_MS: u64 = 300_000;

/// The interval a caller is told to poll at. Readiness of a configuration-sized index moves
/// on the scale of seconds, so a tighter loop would only bill the caller for nothing.
const POLL_INTERVAL_MS: u64 = 1_000;

/// How long a stopped read is given to finish on its own before it is cancelled.
///
/// A read past its last cancellation checkpoint cannot be stopped any more — it will finish
/// in moments whatever anyone does. Cancelling on the spot throws that answer away: the token
/// that reaches the work also tells the join to stop looking, deliberately, because a
/// cancelled REQUEST must never race into a normal response. A task is the other case: its
/// caller still holds a handle to read from, so an answer that lands in this window is worth
/// publishing. Short, because a read that is not nearly done must not delay the release of
/// the resident by more than this.
const FINISHING_GRACE: Duration = Duration::from_millis(50);

/// How often the waiting task itself looks at the resident's lifecycle. Independent of what
/// the caller is told: this one decides how fast an answer becomes available, not how often
/// it is asked for.
const READINESS_POLL: Duration = Duration::from_millis(200);

/// Whether the caller declared the task extension, read off the request it arrived on.
///
/// A tool asks for this rather than for the whole request context because that is the only
/// thing about the caller the branch turns on, and because a bare fact is something a test
/// can state directly instead of assembling a live peer to imply it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TaskCapable(pub(crate) bool);

impl<C: AsRequestContext> FromContextPart<C> for TaskCapable {
    fn from_context_part(context: &mut C) -> Result<Self, McpError> {
        Ok(Self(
            context
                .as_request_context()
                .client_capabilities()
                .is_some_and(|caps| caps.supports_tasks()),
        ))
    }
}

/// Whether the task branch is offered at all.
///
/// Off by default, and read per call rather than latched: it gates a branch no shipping
/// client can reach yet, and a latched knob would make the gates that exercise both branches
/// depend on which of them ran first.
pub(crate) fn enabled() -> bool {
    match std::env::var("BSL_MCP_TASKS") {
        Ok(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "" | "0" | "false" | "no" | "off" => false,
            _ => {
                tracing::warn!(
                    value = %value,
                    "unrecognized BSL_MCP_TASKS (want 1/true/yes/on); task branch disabled"
                );
                false
            }
        },
        Err(_) => false,
    }
}

/// The advertised lifetime of a handle, in milliseconds.
fn ttl_ms() -> u64 {
    match std::env::var("BSL_MCP_TASK_TTL_MS") {
        Ok(value) => match value.parse::<u64>() {
            Ok(ms) if ms > 0 => ms,
            _ => {
                tracing::warn!(
                    value = %value,
                    default_ms = DEFAULT_TTL_MS,
                    "invalid BSL_MCP_TASK_TTL_MS (want a positive integer); using the default"
                );
                DEFAULT_TTL_MS
            }
        },
        Err(_) => DEFAULT_TTL_MS,
    }
}

/// When the task stops itself, derived from the advertised lifetime so the two cannot drift
/// apart.
///
/// It has to fire first, and the reason is not tidiness. Expiry on the SDK side aborts the
/// join handle, which drops the future — and dropping it never runs the `select!` arm that
/// cancels this request's salsa tokens, so a read already inside `spawn_blocking` keeps
/// running and keeps the resident held. Stopping the work ourselves, through the same route
/// a client cancellation takes, is the only route that actually releases it. Reaching the
/// SDK's expiry is therefore a defect in this deadline, not a normal path.
fn deadline(ttl_ms: u64) -> Duration {
    Duration::from_millis(ttl_ms.saturating_mul(4) / 5)
}

/// Answer a resident-reading call, as a task when the caller asked for that branch and the
/// resident is not ready to answer synchronously.
///
/// `body` is the tool's own work, run once — on the calling request in the synchronous
/// branch, on the daemon after readiness in the task branch. `retry` builds that tool's
/// retry envelope for the one outcome that is neither: a writer superseding the read.
pub(crate) async fn resident_response<F, R>(
    server: &McpServer,
    caller: TaskCapable,
    tool: &'static str,
    ct: tokio_util::sync::CancellationToken,
    body: F,
    retry: R,
) -> Result<CallToolResponse, McpError>
where
    F: FnOnce(&ResidentSession) -> Result<CallToolResult, McpError> + Send + 'static,
    R: FnOnce() -> CallToolResult + Send + 'static,
{
    let diag = server.state.diagnostics().clone();
    let started = std::time::Instant::now();

    // Decided before the work is moved anywhere, and from ONE reading of the lifecycle: the
    // branch has to be chosen while both halves are still available to take it. A build in
    // flight is the only state worth a handle — a ready resident answers now, and `Disabled`
    // or `Failed` will not become ready by being waited on.
    let building = matches!(diag.status(), DiagnosticsStatus::Idle | DiagnosticsStatus::Loading);
    // A handle nobody can be told about is worse than no handle: the transport drops the
    // response to a request it has already seen cancelled, so the caller would never learn
    // the id, while the work it names would run on this daemon until its own deadline. The
    // synchronous branch answers such a request with its cancellation, and so does this one.
    if enabled() && caller.0 && building && !ct.is_cancelled() {
        let ttl = ttl_ms();
        let options = TaskOptions::new()
            .with_ttl_ms(ttl)
            .with_poll_interval_ms(POLL_INTERVAL_MS)
            .with_status_message(waiting_message(tool));
        let task = server.state.tasks().spawn(options, move |task_ctx| {
            Box::pin(run(diag, task_ctx, tool, deadline(ttl), body, retry))
        });
        return Ok(CallToolResponse::Task(CreateTaskResult::new(task)));
    }

    let outcome = resident_call(diag, ct, body).await;
    crate::cancellable_answer(outcome, tool, started, retry).map(CallToolResponse::from)
}

/// The whole life of one task: wait for the index, then do the work the call asked for.
async fn run<F, R>(
    diag: DiagnosticsState,
    task_ctx: TaskContext,
    tool: &'static str,
    deadline: Duration,
    body: F,
    retry: R,
) -> Result<CallToolResult, TaskExit>
where
    F: FnOnce(&ResidentSession) -> Result<CallToolResult, McpError> + Send + 'static,
    R: FnOnce() -> CallToolResult + Send + 'static,
{
    let expires = tokio::time::Instant::now() + deadline;
    await_resident(&diag, &task_ctx, tool, expires).await?;
    // The two phases are told apart on the wire, not just in a log: a caller polling a task
    // that has been `working` for a minute has no other way to tell an index that is still
    // building from an answer that is being computed.
    task_ctx.set_status_message(reading_message(tool));

    // The work runs under a token of this task's own: the request that opened the handle is
    // long answered, and its token with it. Cancelling THIS one is what reaches the blocking
    // read, through the same registry a client cancellation reaches it by.
    let ct = tokio_util::sync::CancellationToken::new();
    let call = resident_call(diag, ct.clone(), body);
    tokio::pin!(call);
    let stop = tokio::time::sleep_until(expires);
    tokio::pin!(stop);

    // Both reasons to stop share ONE arm, and that is deliberate. The arm cancels and then
    // AWAITS the call instead of dropping it: a dropped future leaves the blocking read
    // running and the resident held, which is the failure this whole route exists to avoid.
    // Written as two arms, each would need its own gate to prove it still joins — and the
    // one that expires only after minutes is the one a gate is least likely to reach.
    // Awaiting costs nothing: a cancelled call returns without waiting for the blocking
    // thread.
    let mut stopped = None;
    let outcome = tokio::select! {
        biased;
        outcome = &mut call => outcome,
        reason = stop_reason(&task_ctx, stop) => {
            stopped = Some(reason);
            // The grace first: an answer already assembled is worth more than the moment
            // saved by discarding it, and a caller holding a handle can still read it.
            match tokio::time::timeout(FINISHING_GRACE, &mut call).await {
                Ok(outcome) => outcome,
                Err(_) => {
                    ct.cancel();
                    call.await
                }
            }
        }
    };

    match outcome {
        CallOutcome::Ready(answer) => answer.map_err(TaskExit::Error),
        CallOutcome::Cancelled if stopped == Some(Stop::Expired) => {
            Err(TaskExit::Error(unfinished_error(tool)))
        }
        CallOutcome::Cancelled => Err(TaskExit::Cancelled),
        // A writer moved the database out from under the read. Nobody cancelled anything and
        // nothing failed: the caller is owed the same retry envelope the synchronous branch
        // hands it, so the task completes carrying it.
        CallOutcome::Superseded => Ok(retry()),
        CallOutcome::Panicked => {
            Err(TaskExit::Error(McpError::internal_error("internal handler panic", None)))
        }
    }
}

/// Why a running task was told to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    /// The client sent `tasks/cancel`.
    Cancelled,
    /// This task's own deadline elapsed.
    Expired,
}

/// The first reason to stop the work, whichever arrives.
async fn stop_reason(
    task_ctx: &TaskContext,
    expiry: impl std::future::Future<Output = ()>,
) -> Stop {
    tokio::select! {
        biased;
        () = task_ctx.cancelled() => Stop::Cancelled,
        () = expiry => Stop::Expired,
    }
}

/// Wait until there is nothing left to wait for.
///
/// Returning is not the same as "the resident can answer": a build that failed and a profile
/// with no resident at all are answers too, and both are worded by the tool's own body rather
/// than here. Repeating those sentences in this module would make one state speak two ways,
/// and the pair would drift the first time either side was edited. Only a build in flight is
/// worth sleeping on.
async fn await_resident(
    diag: &DiagnosticsState,
    task_ctx: &TaskContext,
    tool: &'static str,
    expires: tokio::time::Instant,
) -> Result<(), TaskExit> {
    let mut expired = false;
    loop {
        // A waiting task is a caller using the resident, and the sweeper measures use by
        // reads alone. Unmarked, a resident published to this task is evicted from under it
        // the moment it appears, and the wait then watches a build that nobody restarts.
        diag.mark_in_use();
        if !matches!(diag.status(), DiagnosticsStatus::Idle | DiagnosticsStatus::Loading) {
            return Ok(());
        }
        // The deadline is answered HERE, after that reading, and not in the arm that
        // observes it. A lifecycle that left the building set while this loop slept is an
        // answer already, and announcing the deadline over it would blame an index that is
        // no longer the reason — a failed build most of all, since it is sticky and the
        // caller would read "still building" about a state that will never move.
        if expired {
            return Err(TaskExit::Error(never_built_error(tool)));
        }

        // Nothing of this request is running yet, so cancellation here is the task settling
        // itself rather than a signal that has to reach a worker.
        tokio::select! {
            biased;
            () = task_ctx.cancelled() => return Err(TaskExit::Cancelled),
            () = tokio::time::sleep_until(expires) => expired = true,
            () = tokio::time::sleep(READINESS_POLL) => {}
        }
    }
}

/// What a caller polling this task reads while the index it needs is still being built.
pub(crate) fn waiting_message(tool: &str) -> String {
    format!("{tool} is waiting for the analysis index")
}

/// What it reads once the index answered and the work itself is running.
pub(crate) fn reading_message(tool: &str) -> String {
    format!("{tool} is reading the analysis index")
}

/// The deadline elapsed while the index was still being built.
///
/// This one has a machine-readable cause, and it is the code the synchronous branch already
/// uses for the same state — taken from that vocabulary rather than retyped beside it, so the
/// two branches cannot come to name one cause differently.
fn never_built_error(tool: &'static str) -> McpError {
    McpError::internal_error(
        format!("{tool} gave up waiting for the analysis index"),
        Some(serde_json::json!({ "reason": ReasonCode::IndexBuilding.as_str() })),
    )
}

/// The deadline elapsed while the answer itself was being computed.
///
/// Deliberately carries no cause code. That vocabulary says why an answer a caller HAS is
/// less than the whole answer; a task stopped mid-work produced no answer at all, the same
/// way a cancelled call does — and lending it `index_building` would tell the caller the
/// index was not ready when it was.
fn unfinished_error(tool: &'static str) -> McpError {
    McpError::internal_error(format!("{tool} ran out of time reading the analysis index"), None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::TaskStatus;
    use rmcp::task_manager::TaskManager;

    /// A deadline no test would sit through, so a wait that should have ended does not pass
    /// these gates late — it hangs, and the timeout reports it.
    const UNREACHABLE_DEADLINE: Duration = Duration::from_secs(600);

    async fn wait_for_terminal(manager: &TaskManager, task_id: &str) {
        loop {
            let detailed = manager.get_task(task_id).expect("the task is addressable");
            if detailed.task.status != TaskStatus::Working {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// A build that failed is a terminal answer, not a slow one: the wait ends on the spot
    /// and hands the state to the tool's body, which words it.
    ///
    /// Fails on a wait that treats every not-ready state as "keep waiting" — there the task
    /// sits until its deadline over a build that will never publish anything.
    #[tokio::test]
    async fn a_failed_index_stops_the_wait_instead_of_burning_the_deadline() {
        let diag = DiagnosticsState::for_workspace(std::env::temp_dir());
        diag.fail_for_test("boom");

        let manager = TaskManager::new();
        let expires = tokio::time::Instant::now() + UNREACHABLE_DEADLINE;
        let task = manager.spawn(TaskOptions::new(), move |task_ctx| {
            Box::pin(async move {
                await_resident(&diag, &task_ctx, "probe", expires).await?;
                Ok(CallToolResult::success(vec![]))
            })
        });

        tokio::time::timeout(Duration::from_secs(5), wait_for_terminal(&manager, &task.task_id))
            .await
            .expect("a failed index must end the wait, not leave it running to its deadline");
    }

    /// A deadline that elapses before the index is built says so, in the code the
    /// synchronous branch already uses for that state.
    ///
    /// Stated here rather than over the wire because over the wire the phase has to be won
    /// from the build, and it cannot be: a stand slow enough to still be building was grown
    /// to six thousand modules and the build still finished first often enough to make the
    /// gate quietly measure the OTHER phase. Here the lifecycle is simply never kicked, so
    /// there is nothing to race.
    #[tokio::test]
    async fn a_deadline_before_the_index_names_the_index() {
        // Never kicked: the state stays `Idle`, which is what a wait sleeps on.
        let diag = DiagnosticsState::for_workspace(std::env::temp_dir());

        let manager = TaskManager::new();
        let expires = tokio::time::Instant::now() + Duration::from_millis(100);
        let task = manager.spawn(TaskOptions::new(), move |task_ctx| {
            Box::pin(async move {
                await_resident(&diag, &task_ctx, "probe", expires).await?;
                Ok(CallToolResult::success(vec![]))
            })
        });

        tokio::time::timeout(Duration::from_secs(5), wait_for_terminal(&manager, &task.task_id))
            .await
            .expect("the deadline must stop the wait");
        let detailed = manager.get_task(&task.task_id).expect("the task is addressable");
        assert_eq!(detailed.task.status, TaskStatus::Failed);
        let error = match detailed.payload {
            rmcp::model::TaskPayload::Failed { error } => error,
            other => panic!("a failed task carries the error: {other:?}"),
        };
        let message = error["message"].as_str().unwrap_or_default();
        assert!(
            message.contains("gave up waiting for the analysis index"),
            "a wait that ended before the index did must say that, not that the answer ran \
             out of time: {message}"
        );
        let reason = error["data"]["reason"].as_str().unwrap_or_default();
        assert_eq!(reason, ReasonCode::IndexBuilding.as_str(), "the machine-readable cause");
        // The same code, read back from what the server publishes to consumers of the
        // synchronous branch: a task branch naming a cause that vocabulary does not carry
        // would leave the two speaking different words about one state.
        let published =
            serde_json::to_string(&crate::tools::diagnostics::schema()).expect("schema");
        assert!(published.contains(reason), "published for the synchronous branch: {reason}");
    }

    /// An answer that lands while the task is being stopped is published, not thrown away.
    ///
    /// The read that reaches its result past the last cancellation checkpoint cannot be
    /// stopped any more; the only question is whether anyone looks at what it produced. For a
    /// request nobody does, and that is deliberate. For a task the caller still holds a
    /// handle, and this gate fails on a build that cancels the join the moment a stop is
    /// decided: there the same work settles as `cancelled` with no result at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_answer_that_arrives_while_stopping_is_still_published() {
        // `Failed` is not a state the wait sleeps on, so the body runs at once and this gate
        // is about the stop alone.
        let diag = DiagnosticsState::for_workspace(std::env::temp_dir());
        diag.fail_for_test("not the subject");

        let manager = TaskManager::new();
        let task = manager.spawn(TaskOptions::new(), move |task_ctx| {
            Box::pin(run(
                diag,
                task_ctx,
                "probe",
                UNREACHABLE_DEADLINE,
                // Shorter than the grace and longer than the stop takes to arrive: work
                // whose tail outlives the decision to stop it.
                |_session| {
                    std::thread::sleep(Duration::from_millis(20));
                    Ok(CallToolResult::success(vec![]))
                },
                || CallToolResult::success(vec![]),
            ))
        });

        manager.cancel_task(&task.task_id).expect("the running task is addressable");
        tokio::time::timeout(Duration::from_secs(5), wait_for_terminal(&manager, &task.task_id))
            .await
            .expect("the task must settle");

        let detailed = manager.get_task(&task.task_id).expect("the task is addressable");
        assert_eq!(
            detailed.task.status,
            TaskStatus::Completed,
            "the work finished inside the grace, so its answer belongs to the caller rather \
             than to the bin"
        );
    }

    /// A call already cancelled leaves no handle behind.
    ///
    /// The transport drops the response to a request it has seen cancelled, so a handle
    /// opened here reaches nobody while the work it names runs on. Both halves are needed:
    /// without the control below the gate is green for a build that never opens a handle.
    /// Written around its own runtime rather than as an `#[tokio::test]`: the turn over the
    /// environment is taken with the same lock every other env-mutating test in this binary
    /// takes, and holding that lock across an await is exactly what must not happen.
    #[test]
    fn a_cancelled_call_leaves_no_handle_behind() {
        // The only gate here that goes through the branch decision, so it is the only one
        // that needs the branch switched on at all — and it puts the process back the way
        // it found it: the flag is read per call, and a stray "1" left behind would arm the
        // branch for every later test in this binary regardless of how cargo was run.
        let _lock = crate::state::test_support::env_lock();
        let _flag = crate::state::test_support::EnvVarGuard::set("BSL_MCP_TASKS", "1");
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(a_cancelled_call_leaves_no_handle_behind_body());
    }

    async fn a_cancelled_call_leaves_no_handle_behind_body() {
        let stand = crate::diagnostics_state::test_support::staged_designer_fixture();
        let state = crate::SharedState::workspace(stand.path().to_path_buf())
            .expect("valid workspace project");
        let server = crate::McpServer::new(crate::McpProfile::Workspace, state);
        server.state.diagnostics().ensure_loading();

        let cancelled = tokio_util::sync::CancellationToken::new();
        cancelled.cancel();
        let answer = resident_response(
            &server,
            TaskCapable(true),
            "probe",
            cancelled,
            |_session| Ok(CallToolResult::success(vec![])),
            || CallToolResult::success(vec![]),
        )
        .await;
        assert!(answer.is_err(), "a cancelled call is answered by its cancellation");
        assert_eq!(
            server.state.tasks().running_task_count(),
            0,
            "a handle opened for a call nobody can be told about outlives the call itself"
        );

        let live = tokio_util::sync::CancellationToken::new();
        let offered = resident_response(
            &server,
            TaskCapable(true),
            "probe",
            live,
            |_session| Ok(CallToolResult::success(vec![])),
            || CallToolResult::success(vec![]),
        )
        .await
        .expect("a live call is answered");
        assert!(
            matches!(offered, CallToolResponse::Task(_)),
            "the same call on a live token still opens a handle"
        );
    }

    /// A deadline does not speak over a lifecycle that already answered.
    ///
    /// The wait sleeps between readings, and the reading interval is longer than the last
    /// stretch before a deadline can be. A build that fails inside that stretch is a terminal
    /// answer — sticky, so nothing will move it — and a task that reports "still building"
    /// over it hands the caller a diagnosis that will never come true. Fails on a build whose
    /// deadline arm answers on the spot instead of reading once more.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_deadline_does_not_speak_over_an_answered_lifecycle() {
        let diag = DiagnosticsState::for_workspace(std::env::temp_dir());
        let manager = TaskManager::new();
        let watched = diag.clone();
        let task = manager.spawn(TaskOptions::new(), move |task_ctx| {
            Box::pin(async move {
                // Shorter than one reading interval, so the wait is woken by the deadline
                // rather than by its own poll — which is the whole window under test.
                let expires = tokio::time::Instant::now() + READINESS_POLL / 4;
                let failing = watched.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(READINESS_POLL / 8).await;
                    failing.fail_for_test("boom");
                });
                await_resident(&watched, &task_ctx, "probe", expires).await?;
                Ok(CallToolResult::success(vec![]))
            })
        });

        tokio::time::timeout(Duration::from_secs(5), wait_for_terminal(&manager, &task.task_id))
            .await
            .expect("the task must settle");
        let detailed = manager.get_task(&task.task_id).expect("the task is addressable");
        assert_eq!(
            detailed.task.status,
            TaskStatus::Completed,
            "the lifecycle answered before the deadline, so the wait had nothing left to \
             wait for and the body — not the deadline — owns the outcome"
        );
    }

    /// A waiting task counts as use of the resident.
    ///
    /// The sweeper measures use by reads, and a task that is waiting has nothing to read yet.
    /// Unmarked, the resident it is waiting for is evicted the moment it is published and the
    /// wait watches a build nobody restarts. Measured rather than asserted structurally: the
    /// gate compares the idle age of an unattended state with the same state under a waiting
    /// task, and only the marking can make the second smaller.
    #[tokio::test]
    async fn a_waiting_task_counts_as_use_of_the_resident() {
        let diag = DiagnosticsState::for_workspace(std::env::temp_dir());
        let dwell = READINESS_POLL * 3;

        tokio::time::sleep(dwell).await;
        let unattended = diag.access_age();
        assert!(unattended >= dwell, "nothing has used it yet: {unattended:?}");

        let manager = TaskManager::new();
        let waited_on = diag.clone();
        let task = manager.spawn(TaskOptions::new(), move |task_ctx| {
            Box::pin(async move {
                let expires = tokio::time::Instant::now() + UNREACHABLE_DEADLINE;
                await_resident(&waited_on, &task_ctx, "probe", expires).await?;
                Ok(CallToolResult::success(vec![]))
            })
        });
        tokio::time::sleep(dwell).await;
        let attended = diag.access_age();
        manager.cancel_task(&task.task_id).expect("the waiting task is addressable");

        assert!(
            attended < unattended,
            "a task has been waiting for {dwell:?} and the resident still counts as unused \
             for {attended:?}, so the sweeper is free to evict it from under the wait"
        );
    }
}
