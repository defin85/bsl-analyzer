use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use base_db::{RootQueryDb, SourceDatabase};
use crossbeam_channel::RecvTimeoutError;
use serde::{Deserialize, Serialize};
use vfs::loader::{self, LoadingProgress};

use crate::global_state::GlobalState;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    Boot,
    FirstPaint,
    Hover,
    Deps,
    Session,
}

impl Scenario {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "boot" => Ok(Scenario::Boot),
            "first_paint" | "first-paint" | "firstpaint" => Ok(Scenario::FirstPaint),
            "hover" => Ok(Scenario::Hover),
            "deps" => Ok(Scenario::Deps),
            "session" => Ok(Scenario::Session),
            other => Err(format!(
                "unknown scenario `{other}` (valid: boot, first_paint, hover, deps, session)"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Scenario::Boot => "boot",
            Scenario::FirstPaint => "first_paint",
            Scenario::Hover => "hover",
            Scenario::Deps => "deps",
            Scenario::Session => "session",
        }
    }

    pub fn all() -> &'static [Scenario] {
        &[Scenario::Boot, Scenario::FirstPaint, Scenario::Hover, Scenario::Deps, Scenario::Session]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Budgets {
    pub boot_vfs_done_ms: u64,
    pub boot_rss_bytes: u64,
    pub boot_degraded_files_max: usize,
    pub first_paint_total_ms: u64,
    pub hover_cold_ms: u64,
    pub hover_warm_ms: u64,
    pub deps_cold_p95_ms: u64,
}

impl Default for Budgets {
    fn default() -> Self {
        Self {
            boot_vfs_done_ms: 30_000,
            boot_rss_bytes: 3 * 1024 * 1024 * 1024,
            boot_degraded_files_max: 0,
            first_paint_total_ms: 1_500,
            hover_cold_ms: 1_300,
            hover_warm_ms: 200,
            deps_cold_p95_ms: 1_000,
        }
    }
}

impl Budgets {
    pub fn load_or_default(path: Option<&Path>) -> Self {
        let Some(path) = path else { return Self::default() };
        match std::fs::read_to_string(path).and_then(|s| {
            serde_json::from_str::<Budgets>(&s)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        }) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("smoke: budgets file {} unreadable: {e}; using defaults", path.display());
                Self::default()
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetViolation {
    pub scenario: String,
    pub metric: String,
    pub observed: u64,
    pub budget: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SmokeReport {
    pub scenarios_run: Vec<String>,
    pub boot: Option<BootResult>,
    pub first_paint: Option<FirstPaintResult>,
    pub hover: Option<HoverResult>,
    pub deps: Option<DepsResult>,
    pub session: Option<SessionResult>,
    pub violations: Vec<BudgetViolation>,
}

impl SmokeReport {
    pub fn passed(&self) -> bool {
        self.violations.is_empty()
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BootResult {
    pub vfs_done_ms: u64,
    pub rss_bytes_post_boot: Option<u64>,
    pub degraded_files_count: usize,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FirstPaintResult {
    pub phases: Vec<FirstPaintPhase>,
    pub total_ms: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FirstPaintPhase {
    pub name: String,
    pub cold_ms: u64,
    pub warm_ms: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct HoverResult {
    pub targets: Vec<HoverTargetResult>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct HoverTargetResult {
    pub file: String,
    pub line: u32,
    pub character: u32,
    pub cold_ms: u64,
    pub warm_ms: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DepsResult {
    pub roots_sampled: usize,
    pub cold_ms_p50: u64,
    pub cold_ms_p95: u64,
}

/// Salsa working-set memory measurement under a simulated editing session.
/// `rss_*_bytes` bracket the live cache: `boot` is the structural floor (VFS text
/// and metadata, before any file opens), `warm` is the peak after the open files'
/// features are computed, and `post_trim` is after `enforce_lru` plus clearing the
/// parser green-node cache. The drop from `warm` to `post_trim` is the
/// salsa-reclaimable footprint; the gap from `boot` to `post_trim` is what survives
/// the trim. `ingredient_counts` is the top-N salsa ingredients by live entry count
/// at the peak, so a heavy intermediate query whose LRU did not trim (resident count
/// far above the open-file method count) is visible directly. The detailed
/// per-ingredient tables go to stderr via [`crate::mem_report`].
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SessionResult {
    pub open_files: usize,
    pub rss_boot_bytes: Option<u64>,
    pub rss_warm_bytes: Option<u64>,
    #[serde(default)]
    pub rss_post_edits_bytes: Option<u64>,
    pub rss_post_trim_bytes: Option<u64>,
    pub ingredient_counts: Vec<(String, usize)>,
    /// didChange-style edit rounds actually executed (0 = stage skipped).
    #[serde(default)]
    pub edit_rounds: usize,
    /// Interned-ingredient count/event trend across the edit stage.
    #[serde(default)]
    pub interned_trend: Vec<InternedTrendRow>,
}

/// One interned ingredient's trajectory across the session's edit stage
/// (populated only with `BSL_SALSA_EVENTS=1` — without the event stream,
/// interned ingredients cannot be told apart from memoized queries). Live-slot
/// counts bracket the stage; the event deltas attribute the movement:
/// `intern_new` = fresh slots, `intern_reuse` = garbage-collected slots
/// recycled for new values, `intern_validate` = existing values re-read in a
/// new revision. A flat count with near-zero `intern_new` means the key set is
/// stable and interned GC has nothing to collect for this type.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct InternedTrendRow {
    pub name: String,
    pub count_post_warm: usize,
    pub count_post_edits: usize,
    pub count_max: usize,
    pub intern_new: u64,
    pub intern_reuse: u64,
    pub intern_validate: u64,
    pub did_discard: u64,
}

pub fn read_rss_bytes() -> Option<u64> {
    crate::mem_report::process_rss_bytes()
}

#[derive(Debug, Clone)]
pub struct SmokeArgs {
    pub source_dir: PathBuf,
    pub scenarios: Vec<Scenario>,
    pub budgets: Budgets,
    pub json: bool,
}

pub fn run(args: SmokeArgs) -> SmokeReport {
    let scenarios_run: Vec<String> =
        args.scenarios.iter().map(|s| s.as_str().to_string()).collect();
    let mut report = SmokeReport { scenarios_run, ..SmokeReport::default() };

    for scenario in &args.scenarios {
        match scenario {
            Scenario::Boot => match run_boot(&args) {
                Ok(boot) => {
                    check_boot_budgets(&boot, &args.budgets, &mut report.violations);
                    report.boot = Some(boot);
                }
                Err(e) => {
                    tracing::error!(error = %e, "smoke[boot]: failed");
                    report.violations.push(BudgetViolation {
                        scenario: "boot".to_string(),
                        metric: "run_error".to_string(),
                        observed: 0,
                        budget: 0,
                    });
                }
            },
            Scenario::FirstPaint => match run_first_paint(&args) {
                Ok(fp) => {
                    check_first_paint_budgets(&fp, &args.budgets, &mut report.violations);
                    report.first_paint = Some(fp);
                }
                Err(e) => {
                    tracing::error!(error = %e, "smoke[first_paint]: failed");
                    report.violations.push(BudgetViolation {
                        scenario: "first_paint".to_string(),
                        metric: "run_error".to_string(),
                        observed: 0,
                        budget: 0,
                    });
                }
            },
            Scenario::Hover => match run_hover(&args) {
                Ok(hv) => {
                    check_hover_budgets(&hv, &args.budgets, &mut report.violations);
                    report.hover = Some(hv);
                }
                Err(e) => {
                    tracing::error!(error = %e, "smoke[hover]: failed");
                    report.violations.push(BudgetViolation {
                        scenario: "hover".to_string(),
                        metric: "run_error".to_string(),
                        observed: 0,
                        budget: 0,
                    });
                }
            },
            Scenario::Deps => match run_deps(&args) {
                Ok(dp) => {
                    check_deps_budgets(&dp, &args.budgets, &mut report.violations);
                    report.deps = Some(dp);
                }
                Err(e) => {
                    tracing::error!(error = %e, "smoke[deps]: failed");
                    report.violations.push(BudgetViolation {
                        scenario: "deps".to_string(),
                        metric: "run_error".to_string(),
                        observed: 0,
                        budget: 0,
                    });
                }
            },
            Scenario::Session => match run_session(&args) {
                Ok(sess) => report.session = Some(sess),
                Err(e) => {
                    tracing::error!(error = %e, "smoke[session]: failed");
                    report.violations.push(BudgetViolation {
                        scenario: "session".to_string(),
                        metric: "run_error".to_string(),
                        observed: 0,
                        budget: 0,
                    });
                }
            },
        }
    }

    if args.json {
        match serde_json::to_string_pretty(&report) {
            Ok(s) => println!("{s}"),
            Err(e) => eprintln!("smoke: JSON serialisation failed: {e}"),
        }
    } else {
        emit_text_report(&report);
    }

    report
}

fn emit_text_report(report: &SmokeReport) {
    if let Some(boot) = &report.boot {
        eprintln!(
            "smoke[boot]: vfs_done_ms={} rss={} degraded_files={}",
            boot.vfs_done_ms,
            boot.rss_bytes_post_boot
                .map(|b| format!("{:.1}MB", b as f64 / (1024.0 * 1024.0)))
                .unwrap_or_else(|| "n/a".to_string()),
            boot.degraded_files_count,
        );
    }
    if let Some(fp) = &report.first_paint {
        eprintln!("smoke[first_paint]: total_ms={}", fp.total_ms);
        for phase in &fp.phases {
            eprintln!("  - {:>16}: cold={}ms warm={}ms", phase.name, phase.cold_ms, phase.warm_ms,);
        }
    }
    if let Some(hv) = &report.hover {
        eprintln!("smoke[hover]: targets={}", hv.targets.len());
        for t in &hv.targets {
            eprintln!(
                "  - {}:{}:{}: cold={}ms warm={}ms",
                t.file, t.line, t.character, t.cold_ms, t.warm_ms,
            );
        }
    }
    if let Some(dp) = &report.deps {
        eprintln!(
            "smoke[deps]: roots={} p50={}ms p95={}ms",
            dp.roots_sampled, dp.cold_ms_p50, dp.cold_ms_p95,
        );
    }
    if let Some(sess) = &report.session {
        let mb = |b: Option<u64>| {
            b.map(|v| format!("{:.1}MB", v as f64 / (1024.0 * 1024.0)))
                .unwrap_or_else(|| "n/a".to_string())
        };
        eprintln!(
            "smoke[session]: open_files={} edit_rounds={} rss_boot={} rss_peak={} rss_post_edits={} rss_post_trim={}",
            sess.open_files,
            sess.edit_rounds,
            mb(sess.rss_boot_bytes),
            mb(sess.rss_warm_bytes),
            mb(sess.rss_post_edits_bytes),
            mb(sess.rss_post_trim_bytes),
        );
        for (name, count) in &sess.ingredient_counts {
            eprintln!("  - {name:<46} count={count}");
        }
    }
    if !report.violations.is_empty() {
        eprintln!("smoke: {} budget violation(s):", report.violations.len());
        for v in &report.violations {
            eprintln!(
                "  - {} {}: observed={} budget={}",
                v.scenario, v.metric, v.observed, v.budget
            );
        }
    } else if !report.scenarios_run.is_empty() {
        eprintln!("smoke: all budgets within thresholds");
    }
}

pub(crate) struct SmokeBootstrap {
    pub(crate) state: GlobalState,
    pub(crate) _drain_handle: std::thread::JoinHandle<()>,
    pub(crate) boot: BootResult,
}

pub(crate) fn bootstrap_smoke(args: &SmokeArgs) -> Result<SmokeBootstrap, String> {
    let (sender, receiver) = crossbeam_channel::unbounded::<lsp_server::Message>();
    let drain_handle = std::thread::spawn(move || while receiver.recv().is_ok() {});

    let mut state = GlobalState::new(sender);
    state.init_empty_source_root();

    let start = Instant::now();
    if let Err(e) = state.set_workspace_root(args.source_dir.clone()) {
        return Err(format!("invalid project at {}: {e}", args.source_dir.display()));
    }

    let safety = args.budgets.boot_vfs_done_ms.saturating_mul(4).saturating_add(60_000);
    let deadline = start + Duration::from_millis(safety);

    let mut finished = false;
    while !finished {
        let now = Instant::now();
        if now >= deadline {
            drop(state);
            let _ = drain_handle.join();
            return Err(format!(
                "boot exceeded hard deadline ({} ms = 4× budget + 60 s buffer)",
                safety,
            ));
        }
        let remaining = deadline - now;
        let msg = match state.loader_receiver.recv_timeout(remaining) {
            Ok(m) => m,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => {
                drop(state);
                let _ = drain_handle.join();
                return Err("loader channel disconnected before Finished arrived".to_string());
            }
        };

        match msg {
            loader::Message::Progress { n_done: LoadingProgress::Finished, .. } => {
                state.process_changes(true);
                state.init_source_root();
                // Mirrors the server finalize: reopen the whole-config loader
                // gate that `set_workspace_root` closed for the initial load.
                state.analysis_host.raw_database_mut().set_workspace_load_complete(true);
                state.bootstrap_metadata_substrate();
                state.warm_metadata_cache();
                state.degraded_files_count = state.skipped_bsl.len();
                let extra = state.assert_total_vfs_invariant();
                if extra > 0 {
                    tracing::error!(
                        extra,
                        "smoke[boot]: total-VFS invariant violated — B1/B2-A missed a path",
                    );
                }
                state.vfs_done = true;
                finished = true;
            }
            loader::Message::Progress { .. } => {}
            loader::Message::Loaded { files } | loader::Message::Changed { files } => {
                stream_loader_batch(&mut state, files);
            }
            loader::Message::WatchOnly { files } => {
                let mut vfs = state.vfs.write();
                for path in files {
                    vfs.register_watch_only(vfs::VfsPath::new(path.as_path()));
                }
            }
            // Batch smoke loads a snapshot once; live removals don't apply.
            loader::Message::RemovedRecursive { .. } => {}
        }
    }

    let vfs_done_ms = start.elapsed().as_millis() as u64;
    let rss_bytes_post_boot = read_rss_bytes();
    let degraded_files_count = state.degraded_files_count;

    Ok(SmokeBootstrap {
        state,
        _drain_handle: drain_handle,
        boot: BootResult { vfs_done_ms, rss_bytes_post_boot, degraded_files_count },
    })
}

fn run_boot(args: &SmokeArgs) -> Result<BootResult, String> {
    Ok(bootstrap_smoke(args)?.boot)
}

fn stream_loader_batch(state: &mut GlobalState, files: Vec<(paths::AbsPathBuf, Option<Vec<u8>>)>) {
    const VFS_WRITE_MINI_BATCH: usize = 16;

    let mut converted: Vec<(vfs::VfsPath, Option<Arc<str>>)> = Vec::with_capacity(files.len());
    for (path, contents) in files {
        let std_path: &std::path::Path = path.as_ref();
        let vfs_path = vfs::VfsPath::new(std_path);

        let contents_str =
            contents.and_then(|bytes| base_db::decode_disk_bytes(&bytes).map(Arc::from));

        if project_model::is_bsl_source_path(std_path) {
            let mutated = if contents_str.is_some() {
                state.skipped_bsl.remove(&path)
            } else if state.skipped_bsl.insert(path.clone()) {
                tracing::warn!(
                    path = %path,
                    "BSL file unreadable by VFS; recorded as skipped",
                );
                true
            } else {
                false
            };
            if mutated {
                state.degraded_files_count = state.skipped_bsl.len();
            }
        }

        converted.push((vfs_path, contents_str));
    }

    for chunk in converted.chunks(VFS_WRITE_MINI_BATCH) {
        let mut vfs = state.vfs.write();
        for (vfs_path, contents_str) in chunk {
            vfs.set_file_contents(vfs_path.clone(), contents_str.clone());
        }
    }
}

fn run_first_paint(args: &SmokeArgs) -> Result<FirstPaintResult, String> {
    let mut ctx = bootstrap_smoke(args)?;
    let file_id = pick_sample_bsl_file(&ctx.state)
        .ok_or_else(|| "first_paint: no resident BSL files in workspace".to_string())?;
    let url = url_for_file_id(&ctx.state, file_id)
        .ok_or_else(|| "first_paint: failed to compute file URL".to_string())?;
    let text = ctx.state.analysis_host.analysis().file_text(file_id);

    tracing::info!(file_id = file_id.0, %url, "smoke[first_paint]: sample file selected");

    let mut phases = Vec::with_capacity(5);

    {
        let cold_start = Instant::now();
        ctx.state.mem_docs.insert(url.clone(), text.clone(), 1);
        let cold_ms = cold_start.elapsed().as_millis() as u64;
        let warm_start = Instant::now();
        let _ = ctx.state.mem_docs.get(&url);
        let warm_ms = warm_start.elapsed().as_millis() as u64;
        phases.push(FirstPaintPhase { name: "didOpen".to_string(), cold_ms, warm_ms });
    }

    let stages: &[(&str, AnalysisStageFn)] = &[
        ("semantic_tokens", semantic_tokens_stage),
        ("document_symbols", document_symbols_stage),
        ("folding_ranges", folding_ranges_stage),
        ("diagnostics", diagnostics_stage),
    ];
    let cfg = ide::DiagnosticsConfig::default();
    for (name, runner) in stages {
        let (cold_ms, warm_ms) = time_analysis_call(&ctx.state, *runner, file_id, &cfg);
        phases.push(FirstPaintPhase { name: (*name).to_string(), cold_ms, warm_ms });
    }

    let total_ms = phases.iter().map(|p| p.cold_ms).sum();
    Ok(FirstPaintResult { phases, total_ms })
}

type AnalysisStageFn = fn(&ide::Analysis, vfs::FileId, &ide::DiagnosticsConfig);

fn semantic_tokens_stage(a: &ide::Analysis, fid: vfs::FileId, _cfg: &ide::DiagnosticsConfig) {
    let _ = a.highlight(fid);
}

fn document_symbols_stage(a: &ide::Analysis, fid: vfs::FileId, _cfg: &ide::DiagnosticsConfig) {
    let _ = a.document_symbols(fid);
}

fn folding_ranges_stage(a: &ide::Analysis, fid: vfs::FileId, _cfg: &ide::DiagnosticsConfig) {
    let _ = a.folding_ranges(fid);
}

fn diagnostics_stage(a: &ide::Analysis, fid: vfs::FileId, cfg: &ide::DiagnosticsConfig) {
    let _ = a.diagnostics(fid, cfg);
}

fn time_analysis_call(
    state: &GlobalState,
    run: AnalysisStageFn,
    fid: vfs::FileId,
    cfg: &ide::DiagnosticsConfig,
) -> (u64, u64) {
    let cold_start = Instant::now();
    {
        let a = state.analysis_host.analysis();
        run(&a, fid, cfg);
    }
    let cold_ms = cold_start.elapsed().as_millis() as u64;
    let warm_start = Instant::now();
    {
        let a = state.analysis_host.analysis();
        run(&a, fid, cfg);
    }
    let warm_ms = warm_start.elapsed().as_millis() as u64;
    (cold_ms, warm_ms)
}

fn pick_sample_bsl_file(state: &GlobalState) -> Option<vfs::FileId> {
    let db = state.analysis_host.raw_database();
    let source_root_input = db.source_root_input(base_db::SourceRootId(0));
    let source_root = source_root_input.root(db);
    let file_set = source_root.file_set();

    let mut bsl_paths: Vec<(String, vfs::FileId)> = file_set
        .iter()
        .filter_map(|fid| {
            let vfs_path = file_set.path_for_file(&fid)?;
            let std_path = vfs_path.as_path();
            if !project_model::is_bsl_source_path(std_path) {
                return None;
            }
            db.try_file_revision_input(fid)?;
            Some((std_path.to_string_lossy().into_owned(), fid))
        })
        .collect();
    bsl_paths.sort();
    bsl_paths.into_iter().next().map(|(_, fid)| fid)
}

fn url_for_file_id(state: &GlobalState, file_id: vfs::FileId) -> Option<lsp_types::Url> {
    let vfs = state.vfs.read();
    let vfs_path = vfs.file_path(file_id);
    lsp_types::Url::from_file_path(vfs_path.as_path()).ok()
}

const HOVER_MAX_TARGETS: usize = 5;

fn run_hover(args: &SmokeArgs) -> Result<HoverResult, String> {
    let ctx = bootstrap_smoke(args)?;
    let file_id = pick_sample_bsl_file(&ctx.state)
        .ok_or_else(|| "hover: no resident BSL files in workspace".to_string())?;
    let url = url_for_file_id(&ctx.state, file_id)
        .ok_or_else(|| "hover: failed to compute file URL".to_string())?;

    let offsets = pick_hover_offsets(&ctx.state, file_id, HOVER_MAX_TARGETS);
    if offsets.is_empty() {
        return Err("hover: no identifier tokens found in sample file".to_string());
    }

    let text = ctx.state.analysis_host.analysis().file_text(file_id);
    let mut targets = Vec::with_capacity(offsets.len());
    for offset in offsets {
        let (line, character) = offset_to_line_col(&text, offset);
        let (cold_ms, warm_ms) = time_hover_call(&ctx.state, file_id, offset);
        targets.push(HoverTargetResult {
            file: url.to_string(),
            line,
            character,
            cold_ms,
            warm_ms,
        });
    }
    Ok(HoverResult { targets })
}

fn pick_hover_offsets(state: &GlobalState, file_id: vfs::FileId, max: usize) -> Vec<u32> {
    let db = state.analysis_host.raw_database();
    let parse = db.parse(file_id);
    let root = parse.syntax_node();
    let mut offsets = Vec::with_capacity(max);
    for elem in root.descendants_with_tokens() {
        if let Some(token) = elem.as_token() {
            if token.kind() == syntax::SyntaxKind::IDENT {
                offsets.push(u32::from(token.text_range().start()));
                if offsets.len() >= max {
                    break;
                }
            }
        }
    }
    offsets
}

fn time_hover_call(state: &GlobalState, fid: vfs::FileId, offset: u32) -> (u64, u64) {
    let locale = ide::Locale::default();
    let cold_start = Instant::now();
    {
        let a = state.analysis_host.analysis();
        let _ = a.hover(fid, offset, locale);
    }
    let cold_ms = cold_start.elapsed().as_millis() as u64;
    let warm_start = Instant::now();
    {
        let a = state.analysis_host.analysis();
        let _ = a.hover(fid, offset, locale);
    }
    let warm_ms = warm_start.elapsed().as_millis() as u64;
    (cold_ms, warm_ms)
}

fn offset_to_line_col(text: &str, offset: u32) -> (u32, u32) {
    let off = (offset as usize).min(text.len());
    let prefix = &text[..off];
    let line = prefix.matches('\n').count() as u32;
    let line_start = prefix.rfind('\n').map_or(0, |p| p + 1);
    let character = (off - line_start) as u32;
    (line, character)
}

const DEPS_MAX_ROOTS: usize = 50;

fn run_deps(args: &SmokeArgs) -> Result<DepsResult, String> {
    let ctx = bootstrap_smoke(args)?;
    let roots = enumerate_bsl_roots(&ctx.state, DEPS_MAX_ROOTS);
    if roots.is_empty() {
        return Err("deps: no resident BSL files in workspace".to_string());
    }

    let mut cold_ms: Vec<u64> = Vec::with_capacity(roots.len());
    for fid in &roots {
        let start = Instant::now();
        {
            let a = ctx.state.analysis_host.analysis();
            let _ = a.file_dependencies(*fid);
        }
        cold_ms.push(start.elapsed().as_millis() as u64);
    }
    cold_ms.sort_unstable();
    Ok(DepsResult {
        roots_sampled: roots.len(),
        cold_ms_p50: percentile(&cold_ms, 50),
        cold_ms_p95: percentile(&cold_ms, 95),
    })
}

fn enumerate_bsl_roots(state: &GlobalState, cap: usize) -> Vec<vfs::FileId> {
    let db = state.analysis_host.raw_database();
    let source_root_input = db.source_root_input(base_db::SourceRootId(0));
    let source_root = source_root_input.root(db);
    let file_set = source_root.file_set();

    let mut paths: Vec<(String, vfs::FileId)> = file_set
        .iter()
        .filter_map(|fid| {
            let vfs_path = file_set.path_for_file(&fid)?;
            let std_path = vfs_path.as_path();
            if !project_model::is_bsl_source_path(std_path) {
                return None;
            }
            db.try_file_revision_input(fid)?;
            Some((std_path.to_string_lossy().into_owned(), fid))
        })
        .collect();
    paths.sort();
    paths.into_iter().take(cap).map(|(_, f)| f).collect()
}

fn percentile(sorted: &[u64], p: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (sorted.len() * p / 100).min(sorted.len() - 1);
    sorted[idx]
}

/// How many files the simulated session "opens" and analyzes — a realistic
/// editor working set, far below the corpus size, so a heavy intermediate
/// query that retains the *whole* corpus stands out against this baseline.
const SESSION_OPEN_FILES: usize = 40;
const SESSION_HOVER_PER_FILE: usize = 3;
const SESSION_TOP_INGREDIENTS: usize = 16;

/// didChange-style edit rounds after the warm-up (`BSL_SMOKE_SESSION_EDITS`
/// overrides; `0` skips the stage). Interned garbage collection considers a
/// value stale only after `revisions` (default 3) distinct *read* revisions and
/// reclaims a stale slot only when a new value is interned afterwards, so the
/// stage must span dozens of revisions for either effect to be observable.
const SESSION_EDIT_ROUNDS: usize = 30;
/// Rotation-edited files per round: enough churn to keep the interned key
/// ingredients read in every revision without recomputing the whole set.
const SESSION_EDIT_FILES_PER_ROUND: usize = 8;
/// Count-snapshot cadence during the edit stage. `memory_usage` walks every
/// live slot of every ingredient, which is expensive on a large corpus.
const SESSION_EDIT_SNAPSHOT_EVERY: usize = 5;
/// Oscillation period of the method-churn probe: the probe file's extra-method
/// count cycles `0..PERIOD-1`, so its top `MethodIdInput` values sit unread for
/// several revisions (becoming stale GC candidates) before being re-read.
const SESSION_CHURN_PERIOD: usize = 6;

/// A working set of up to `n` BSL files spread evenly across the corpus (stride
/// sampling over the sorted path list), so the session touches diverse modules
/// and exercises cross-module resolution rather than one clustered subsystem.
fn pick_session_working_set(state: &GlobalState, n: usize) -> Vec<vfs::FileId> {
    let db = state.analysis_host.raw_database();
    let source_root_input = db.source_root_input(base_db::SourceRootId(0));
    let source_root = source_root_input.root(db);
    let file_set = source_root.file_set();

    let mut paths: Vec<(String, vfs::FileId)> = file_set
        .iter()
        .filter_map(|fid| {
            let vfs_path = file_set.path_for_file(&fid)?;
            let std_path = vfs_path.as_path();
            if !project_model::is_bsl_source_path(std_path) {
                return None;
            }
            db.try_file_revision_input(fid)?;
            Some((std_path.to_string_lossy().into_owned(), fid))
        })
        .collect();
    paths.sort();

    if paths.is_empty() {
        return Vec::new();
    }
    if paths.len() <= n {
        return paths.into_iter().map(|(_, f)| f).collect();
    }
    let stride = paths.len() / n;
    (0..n).map(|i| paths[i * stride].1).collect()
}

/// Cumulative interned-lifecycle event counters per ingredient name as
/// `[intern_new, intern_reuse, intern_validate, did_discard]`, or `None` when
/// the database was built without `BSL_SALSA_EVENTS=1`.
fn interned_event_totals(
    db: &ide::RootDatabaseImpl,
) -> Option<std::collections::HashMap<String, [u64; 4]>> {
    let rows = db.salsa_event_report()?;
    Some(
        rows.into_iter()
            .map(|r| (r.name, [r.intern_new, r.intern_reuse, r.intern_validate, r.did_discard]))
            .collect(),
    )
}

/// A simulated series of didChange edits over the working set: does a long
/// multi-revision session make interned key sets grow, and does interned GC
/// ever engage? Most files get a text-only trailing edit each round — the
/// revision moves, the key set stays stable. Two probe files vary their method
/// count, because `MethodIdInput` is keyed by (file, method index): the
/// oscillating probe leaves its top indices unread for several revisions
/// (stale GC candidates), the growing probe interns one fresh value per round
/// (interned GC is lazy — without new interning nothing is ever reclaimed).
/// Returns the executed round count and the per-interned-ingredient trend
/// (empty without `BSL_SALSA_EVENTS=1`).
fn run_session_edit_loop(
    state: &mut GlobalState,
    files: &[vfs::FileId],
    cfg: &ide::DiagnosticsConfig,
) -> (usize, Vec<InternedTrendRow>) {
    let rounds = std::env::var("BSL_SMOKE_SESSION_EDITS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(SESSION_EDIT_ROUNDS);
    if rounds == 0 || files.len() < 3 {
        return (0, Vec::new());
    }

    let probe_grow = files[files.len() - 2];
    let probe_osc = files[files.len() - 1];
    let rotation = &files[..files.len() - 2];

    let originals: std::collections::HashMap<vfs::FileId, Arc<str>> = {
        let a = state.analysis_host.analysis();
        files.iter().map(|f| (*f, a.file_text_arc(*f))).collect()
    };

    let events_before = interned_event_totals(state.analysis_host.raw_database());

    // Per-ingredient live-slot counts as (post_warm, max, last).
    let mut counts: std::collections::HashMap<String, (usize, usize, usize)> =
        crate::mem_report::salsa_memory_rows(state.analysis_host.raw_database())
            .into_iter()
            .map(|(name, c, ..)| (name.to_string(), (c, c, c)))
            .collect();

    let per_round = SESSION_EDIT_FILES_PER_ROUND.min(rotation.len());
    for r in 0..rounds {
        let mut edited: Vec<vfs::FileId> = Vec::with_capacity(per_round + 2);
        {
            let db = state.analysis_host.raw_database_mut();
            let start = (r * per_round) % rotation.len();
            for i in 0..per_round {
                let fid = rotation[(start + i) % rotation.len()];
                db.set_file_text(fid, &format!("{}\n// смоук-правка {r}", originals[&fid]));
                edited.push(fid);
            }
            let mut osc_text = originals[&probe_osc].to_string();
            for k in 0..r % SESSION_CHURN_PERIOD {
                osc_text.push_str(&format!("\nПроцедура СмоукПробаОсц{k}()\nКонецПроцедуры"));
            }
            db.set_file_text(probe_osc, &osc_text);
            edited.push(probe_osc);
            let mut grow_text = originals[&probe_grow].to_string();
            for k in 0..=r {
                grow_text.push_str(&format!("\nПроцедура СмоукПробаРост{k}()\nКонецПроцедуры"));
            }
            db.set_file_text(probe_grow, &grow_text);
            edited.push(probe_grow);
        }

        {
            let a = state.analysis_host.analysis();
            for fid in &edited {
                let _ = a.document_symbols(*fid);
                let _ = a.diagnostics(*fid, cfg);
            }
        }

        if (r + 1) % SESSION_EDIT_SNAPSHOT_EVERY == 0 || r + 1 == rounds {
            for (name, c, ..) in
                crate::mem_report::salsa_memory_rows(state.analysis_host.raw_database())
            {
                let entry = counts.entry(name.to_string()).or_insert((0, 0, 0));
                entry.1 = entry.1.max(c);
                entry.2 = c;
            }
            eprintln!("smoke[session]: edit round {}/{rounds} done", r + 1);
        }
    }

    let events_after = interned_event_totals(state.analysis_host.raw_database());
    let (Some(before), Some(after)) = (events_before, events_after) else {
        return (rounds, Vec::new());
    };

    let mut trend: Vec<InternedTrendRow> = counts
        .into_iter()
        .filter_map(|(name, (post_warm, max, last))| {
            let a = after.get(&name)?;
            // Interned ingredients are the ones with intern lifecycle activity;
            // `did_discard` alone also fires for memoized queries.
            if a[0] + a[1] + a[2] == 0 {
                return None;
            }
            let b = before.get(&name).copied().unwrap_or_default();
            Some(InternedTrendRow {
                name,
                count_post_warm: post_warm,
                count_post_edits: last,
                count_max: max,
                intern_new: a[0].saturating_sub(b[0]),
                intern_reuse: a[1].saturating_sub(b[1]),
                intern_validate: a[2].saturating_sub(b[2]),
                did_discard: a[3].saturating_sub(b[3]),
            })
        })
        .collect();
    trend.sort_by_key(|row| {
        std::cmp::Reverse((
            row.count_post_edits as i64 - row.count_post_warm as i64,
            row.intern_new,
        ))
    });
    (rounds, trend)
}

/// Print the interned trend table collected by [`run_session_edit_loop`], or
/// the hint that the event stream was off. Emitted to stderr.
fn print_interned_trend(trend: &[InternedTrendRow], rounds: usize) {
    if trend.is_empty() {
        eprintln!(
            "smoke[session]: interned trend unavailable — rerun with BSL_SALSA_EVENTS=1 \
             to attribute interned counts"
        );
        return;
    }
    eprintln!("\n##### interned ingredients — trend over {rounds} edit rounds #####");
    eprintln!(
        "{:<44}{:>10}{:>10}{:>10}{:>9}{:>9}{:>9}{:>9}  verdict",
        "ingredient", "warm", "end", "max", "int_new", "int_reu", "int_val", "discard",
    );
    for row in trend {
        let verdict = match row.count_post_edits.cmp(&row.count_post_warm) {
            std::cmp::Ordering::Greater => "grows",
            std::cmp::Ordering::Equal => "plateau",
            std::cmp::Ordering::Less => "shrinks",
        };
        eprintln!(
            "{:<44}{:>10}{:>10}{:>10}{:>9}{:>9}{:>9}{:>9}  {verdict}",
            row.name,
            row.count_post_warm,
            row.count_post_edits,
            row.count_max,
            row.intern_new,
            row.intern_reuse,
            row.intern_validate,
            row.did_discard,
        );
    }
}

/// Simulate an LSP working set and attribute its memory. Boots the real config
/// (`rss_boot` = structural floor: VFS text + metadata), "opens" a spread of
/// files and computes their LSP features (semantic tokens, symbols, diagnostics,
/// hover) to fill the derived caches (`infer_method`, `method_body`, symbol/item
/// trees, parse green trees) including cross-module pulls (`rss_peak`). Then it
/// trims every salsa memo to its LRU cap and clears the parser's green-node arena
/// (`rss_post_trim`). `peak − post_trim` is the salsa-reclaimable footprint —
/// including ingredients with no `heap_size` hook, which the byte report misses;
/// `post_trim − boot` is what survives the trim. Prints both per-ingredient
/// tables to stderr; [`SessionResult`] carries the RSS points and peak counts.
fn run_session(args: &SmokeArgs) -> Result<SessionResult, String> {
    let mut ctx = bootstrap_smoke(args)?;

    // RSS right after boot, before any file is opened: the structural floor (all
    // file texts in the VFS + the loaded metadata configuration). Anything above
    // this at steady state was produced by the session's analysis.
    let rss_boot_bytes = read_rss_bytes();

    let open_target = std::env::var("BSL_SMOKE_SESSION_FILES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(SESSION_OPEN_FILES);
    let files = pick_session_working_set(&ctx.state, open_target);
    if files.is_empty() {
        return Err("session: no resident BSL files in workspace".to_string());
    }

    let cfg = ide::DiagnosticsConfig::default();
    let locale = ide::Locale::default();

    // Open + first-paint each working-set file: this is the demand that fills the
    // intermediate caches we are evaluating.
    for fid in &files {
        if let Some(url) = url_for_file_id(&ctx.state, *fid) {
            let text = ctx.state.analysis_host.analysis().file_text(*fid);
            ctx.state.mem_docs.insert(url, text, 1);
        }
        let offsets = pick_hover_offsets(&ctx.state, *fid, SESSION_HOVER_PER_FILE);
        let a = ctx.state.analysis_host.analysis();
        let _ = a.highlight(*fid);
        let _ = a.document_symbols(*fid);
        let _ = a.diagnostics(*fid, &cfg);
        for offset in offsets {
            let _ = a.hover(*fid, offset, locale);
        }
    }

    // Peak working set: every open file's features computed, nothing evicted yet.
    let rss_warm_bytes = read_rss_bytes();
    let ingredient_counts = {
        let db = ctx.state.analysis_host.raw_database();
        crate::mem_report::print_salsa_memory_report(
            db,
            "session post-warm / peak (working set populated)",
        );
        crate::mem_report::print_salsa_event_report(
            db,
            "session post-warm / peak (working set populated)",
        );
        // Only at the peak, before the edit stage below bumps the revision: key
        // decode reads interned inputs and is sound only within the revision that
        // produced the captured keys.
        crate::mem_report::print_salsa_key_event_report(
            db,
            "session post-warm / peak (working set populated)",
        );
        crate::mem_report::salsa_memory_rows(db)
            .into_iter()
            .take(SESSION_TOP_INGREDIENTS)
            .map(|(name, count, ..)| (name.to_string(), count))
            .collect()
    };

    // Edit stage: a long multi-revision window between the peak snapshot and the
    // trim, where interned staleness and slot reuse have revisions to happen in.
    let (edit_rounds, interned_trend) = run_session_edit_loop(&mut ctx.state, &files, &cfg);
    let rss_post_edits_bytes = (edit_rounds > 0).then(read_rss_bytes).flatten();
    if edit_rounds > 0 {
        let db = ctx.state.analysis_host.raw_database();
        let label = format!("session post-edits ({edit_rounds} didChange rounds)");
        crate::mem_report::print_salsa_memory_report(db, &label);
        crate::mem_report::print_salsa_event_report(db, &label);
        print_interned_trend(&interned_trend, edit_rounds);
    }

    // Attribution test: force salsa to trim every memo to its LRU cap and release
    // the parser's shared green-node arena (which salsa does not own), then close
    // the open documents. Whatever RSS drops here was salsa-side derived state
    // (green trees, item/symbol trees, bodies, inference) — including the part the
    // heap report cannot see, because those ingredients carry no `heap_size` hook
    // so their Arc payloads never show up in `salsa-tracked` bytes. If RSS instead
    // stays high, the cost is structural (boot text + metadata), not the cache.
    for uri in ctx.state.mem_docs.uris() {
        ctx.state.mem_docs.remove(&uri);
    }
    ctx.state.analysis_host.raw_database_mut().enforce_lru();
    syntax::clear_shared_node_cache();
    rayon::broadcast(|_| syntax::clear_shared_node_cache());
    profile::purge_allocator();
    // With a jemalloc background thread the purge is serviced asynchronously, so
    // an immediate RSS read races the page return. Let it settle (also past the
    // bounded dirty-decay interval) so `post_trim` reflects the steady-state
    // reclaimable footprint rather than an in-flight snapshot.
    std::thread::sleep(std::time::Duration::from_secs(2));
    let rss_post_trim_bytes = read_rss_bytes();
    crate::mem_report::print_salsa_memory_report(
        ctx.state.analysis_host.raw_database(),
        "session post-trim (enforce_lru + green-node cache cleared)",
    );
    crate::mem_report::print_salsa_event_report(
        ctx.state.analysis_host.raw_database(),
        "session post-trim (enforce_lru + green-node cache cleared)",
    );

    Ok(SessionResult {
        open_files: files.len(),
        rss_boot_bytes,
        rss_warm_bytes,
        rss_post_edits_bytes,
        rss_post_trim_bytes,
        ingredient_counts,
        edit_rounds,
        interned_trend,
    })
}

fn check_boot_budgets(boot: &BootResult, budgets: &Budgets, out: &mut Vec<BudgetViolation>) {
    if boot.vfs_done_ms > budgets.boot_vfs_done_ms {
        out.push(BudgetViolation {
            scenario: "boot".to_string(),
            metric: "vfs_done_ms".to_string(),
            observed: boot.vfs_done_ms,
            budget: budgets.boot_vfs_done_ms,
        });
    }
    if let Some(rss) = boot.rss_bytes_post_boot {
        if rss > budgets.boot_rss_bytes {
            out.push(BudgetViolation {
                scenario: "boot".to_string(),
                metric: "rss_bytes_post_boot".to_string(),
                observed: rss,
                budget: budgets.boot_rss_bytes,
            });
        }
    }
    if boot.degraded_files_count > budgets.boot_degraded_files_max {
        out.push(BudgetViolation {
            scenario: "boot".to_string(),
            metric: "degraded_files_count".to_string(),
            observed: boot.degraded_files_count as u64,
            budget: budgets.boot_degraded_files_max as u64,
        });
    }
}

fn check_first_paint_budgets(
    fp: &FirstPaintResult,
    budgets: &Budgets,
    out: &mut Vec<BudgetViolation>,
) {
    if fp.total_ms > budgets.first_paint_total_ms {
        out.push(BudgetViolation {
            scenario: "first_paint".to_string(),
            metric: "total_ms".to_string(),
            observed: fp.total_ms,
            budget: budgets.first_paint_total_ms,
        });
    }
}

fn check_hover_budgets(hv: &HoverResult, budgets: &Budgets, out: &mut Vec<BudgetViolation>) {
    let max_cold = hv.targets.iter().map(|t| t.cold_ms).max().unwrap_or(0);
    let max_warm = hv.targets.iter().map(|t| t.warm_ms).max().unwrap_or(0);
    if max_cold > budgets.hover_cold_ms {
        out.push(BudgetViolation {
            scenario: "hover".to_string(),
            metric: "max_cold_ms".to_string(),
            observed: max_cold,
            budget: budgets.hover_cold_ms,
        });
    }
    if max_warm > budgets.hover_warm_ms {
        out.push(BudgetViolation {
            scenario: "hover".to_string(),
            metric: "max_warm_ms".to_string(),
            observed: max_warm,
            budget: budgets.hover_warm_ms,
        });
    }
}

fn check_deps_budgets(dp: &DepsResult, budgets: &Budgets, out: &mut Vec<BudgetViolation>) {
    if dp.cold_ms_p95 > budgets.deps_cold_p95_ms {
        out.push(BudgetViolation {
            scenario: "deps".to_string(),
            metric: "cold_ms_p95".to_string(),
            observed: dp.cold_ms_p95,
            budget: budgets.deps_cold_p95_ms,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_parse_round_trip() {
        for &s in Scenario::all() {
            let name = s.as_str();
            assert_eq!(Scenario::parse(name).unwrap(), s);
        }
    }

    #[test]
    fn scenario_parse_case_insensitive() {
        assert_eq!(Scenario::parse("BOOT").unwrap(), Scenario::Boot);
        assert_eq!(Scenario::parse("First-Paint").unwrap(), Scenario::FirstPaint);
        assert_eq!(Scenario::parse(" hover ").unwrap(), Scenario::Hover);
    }

    #[test]
    fn scenario_parse_rejects_unknown() {
        assert!(Scenario::parse("refs").is_err());
    }

    #[test]
    fn budgets_default_is_sane() {
        let b = Budgets::default();
        assert!(b.boot_vfs_done_ms > 0);
        assert!(b.boot_rss_bytes > 0);
        assert!(b.hover_warm_ms < b.hover_cold_ms);
    }

    #[test]
    fn report_passes_when_no_violations() {
        let r = SmokeReport::default();
        assert!(r.passed());
    }

    #[test]
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn rss_reader_returns_a_value_where_the_kernel_exposes_one() {
        let rss = read_rss_bytes();
        assert!(rss.is_some(), "the kernel must expose a resident set size here");
        assert!(rss.unwrap() > 0);
    }

    #[test]
    fn check_boot_budgets_flags_vfs_done() {
        let boot = BootResult {
            vfs_done_ms: 1000,
            rss_bytes_post_boot: Some(100),
            degraded_files_count: 0,
        };
        let budgets = Budgets { boot_vfs_done_ms: 500, ..Budgets::default() };
        let mut out = Vec::new();
        check_boot_budgets(&boot, &budgets, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric, "vfs_done_ms");
        assert_eq!(out[0].observed, 1000);
        assert_eq!(out[0].budget, 500);
    }

    #[test]
    fn check_boot_budgets_flags_rss_and_degraded() {
        let boot = BootResult {
            vfs_done_ms: 10,
            rss_bytes_post_boot: Some(2_000),
            degraded_files_count: 3,
        };
        let budgets = Budgets {
            boot_vfs_done_ms: 10_000,
            boot_rss_bytes: 1_000,
            boot_degraded_files_max: 0,
            ..Budgets::default()
        };
        let mut out = Vec::new();
        check_boot_budgets(&boot, &budgets, &mut out);
        let metrics: Vec<_> = out.iter().map(|v| v.metric.as_str()).collect();
        assert!(metrics.contains(&"rss_bytes_post_boot"));
        assert!(metrics.contains(&"degraded_files_count"));
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn check_boot_budgets_passes_within_thresholds() {
        let boot = BootResult {
            vfs_done_ms: 100,
            rss_bytes_post_boot: Some(100),
            degraded_files_count: 0,
        };
        let mut out = Vec::new();
        check_boot_budgets(&boot, &Budgets::default(), &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn check_boot_budgets_tolerates_missing_rss() {
        let boot =
            BootResult { vfs_done_ms: 10, rss_bytes_post_boot: None, degraded_files_count: 0 };
        let mut out = Vec::new();
        check_boot_budgets(&boot, &Budgets::default(), &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn run_boot_on_empty_tmpdir_succeeds() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let args = SmokeArgs {
            source_dir: tmp.path().to_path_buf(),
            scenarios: vec![Scenario::Boot],
            budgets: Budgets::default(),
            json: false,
        };
        let boot = run_boot(&args).expect("boot on empty tmpdir should succeed");
        assert_eq!(boot.degraded_files_count, 0);
        assert!(boot.vfs_done_ms < Budgets::default().boot_vfs_done_ms);
    }

    #[test]
    fn check_first_paint_budgets_flags_total() {
        let fp = FirstPaintResult {
            phases: vec![
                FirstPaintPhase { name: "didOpen".into(), cold_ms: 800, warm_ms: 1 },
                FirstPaintPhase { name: "semantic_tokens".into(), cold_ms: 1200, warm_ms: 5 },
            ],
            total_ms: 2_000,
        };
        let budgets = Budgets { first_paint_total_ms: 1_000, ..Budgets::default() };
        let mut out = Vec::new();
        check_first_paint_budgets(&fp, &budgets, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric, "total_ms");
        assert_eq!(out[0].observed, 2_000);
    }

    #[test]
    fn check_first_paint_budgets_passes_within_threshold() {
        let fp = FirstPaintResult {
            phases: vec![FirstPaintPhase { name: "didOpen".into(), cold_ms: 1, warm_ms: 0 }],
            total_ms: 1,
        };
        let mut out = Vec::new();
        check_first_paint_budgets(&fp, &Budgets::default(), &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn run_first_paint_on_synthetic_workspace_succeeds() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let bsl = tmp.path().join("Module.bsl");
        std::fs::write(&bsl, "Процедура Test()\nКонецПроцедуры\n").expect("write bsl");

        let args = SmokeArgs {
            source_dir: tmp.path().to_path_buf(),
            scenarios: vec![Scenario::FirstPaint],
            budgets: Budgets::default(),
            json: false,
        };
        let fp = run_first_paint(&args).expect("first_paint on synthetic workspace should succeed");
        assert_eq!(fp.phases.len(), 5);
        let names: Vec<&str> = fp.phases.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["didOpen", "semantic_tokens", "document_symbols", "folding_ranges", "diagnostics"],
        );
        let cold_sum: u64 = fp.phases.iter().map(|p| p.cold_ms).sum();
        assert_eq!(fp.total_ms, cold_sum, "total_ms must be the sum of cold timings");
    }

    #[test]
    fn run_first_paint_errors_when_workspace_has_no_bsl() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let args = SmokeArgs {
            source_dir: tmp.path().to_path_buf(),
            scenarios: vec![Scenario::FirstPaint],
            budgets: Budgets::default(),
            json: false,
        };
        let err = run_first_paint(&args).expect_err("empty workspace must produce an error");
        assert!(err.contains("no resident BSL files"), "got: {err}");
    }

    #[test]
    fn offset_to_line_col_handles_multiple_lines() {
        let text = "abc\nde\nfgh";
        assert_eq!(offset_to_line_col(text, 0), (0, 0));
        assert_eq!(offset_to_line_col(text, 2), (0, 2));
        assert_eq!(offset_to_line_col(text, 4), (1, 0));
        assert_eq!(offset_to_line_col(text, 5), (1, 1));
        assert_eq!(offset_to_line_col(text, 7), (2, 0));
        assert_eq!(offset_to_line_col(text, 999), (2, 3));
    }

    #[test]
    fn percentile_handles_typical_distributions() {
        assert_eq!(percentile(&[], 50), 0);
        assert_eq!(percentile(&[42], 50), 42);
        assert_eq!(percentile(&[42], 95), 42);
        let sorted = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        assert_eq!(percentile(&sorted, 50), 6);
        assert_eq!(percentile(&sorted, 95), 10);
        assert_eq!(percentile(&sorted, 0), 1);
    }

    #[test]
    fn check_hover_budgets_flags_worst_case() {
        let hv = HoverResult {
            targets: vec![
                HoverTargetResult {
                    file: "f".into(),
                    line: 0,
                    character: 0,
                    cold_ms: 100,
                    warm_ms: 5,
                },
                HoverTargetResult {
                    file: "f".into(),
                    line: 1,
                    character: 0,
                    cold_ms: 1500,
                    warm_ms: 250,
                },
            ],
        };
        let budgets = Budgets { hover_cold_ms: 1_000, hover_warm_ms: 200, ..Budgets::default() };
        let mut out = Vec::new();
        check_hover_budgets(&hv, &budgets, &mut out);
        let metrics: Vec<&str> = out.iter().map(|v| v.metric.as_str()).collect();
        assert!(metrics.contains(&"max_cold_ms"));
        assert!(metrics.contains(&"max_warm_ms"));
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn check_deps_budgets_flags_p95() {
        let dp = DepsResult { roots_sampled: 10, cold_ms_p50: 5, cold_ms_p95: 1500 };
        let budgets = Budgets { deps_cold_p95_ms: 1_000, ..Budgets::default() };
        let mut out = Vec::new();
        check_deps_budgets(&dp, &budgets, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].metric, "cold_ms_p95");
    }

    #[test]
    fn run_hover_on_synthetic_workspace_succeeds() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let bsl = tmp.path().join("Module.bsl");
        std::fs::write(&bsl, "Процедура Test()\n    Сообщить(\"hi\");\nКонецПроцедуры\n")
            .expect("write bsl");

        let args = SmokeArgs {
            source_dir: tmp.path().to_path_buf(),
            scenarios: vec![Scenario::Hover],
            budgets: Budgets::default(),
            json: false,
        };
        let hv = run_hover(&args).expect("hover on synthetic workspace should succeed");
        assert!(!hv.targets.is_empty(), "expected at least one hover target");
        assert!(hv.targets.len() <= HOVER_MAX_TARGETS);
    }

    #[test]
    fn run_deps_on_synthetic_workspace_succeeds() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let bsl = tmp.path().join("Module.bsl");
        std::fs::write(&bsl, "Процедура Test()\nКонецПроцедуры\n").expect("write bsl");

        let args = SmokeArgs {
            source_dir: tmp.path().to_path_buf(),
            scenarios: vec![Scenario::Deps],
            budgets: Budgets::default(),
            json: false,
        };
        let dp = run_deps(&args).expect("deps on synthetic workspace should succeed");
        assert!(dp.roots_sampled >= 1);
        assert!(dp.cold_ms_p50 <= dp.cold_ms_p95);
    }
}
