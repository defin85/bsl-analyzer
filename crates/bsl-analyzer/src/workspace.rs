use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{anyhow, Result};
use lsp_types::Url;
use vfs::{loader, FileId, VfsPath};

use crate::global_state::GlobalState;

/// Adapts the LSP server's `parking_lot`-locked VFS to the lock-neutral
/// [`ide_host_core::VfsWrite`] the shared metadata policy expects, keeping the lock
/// flavour out of `ide-host-core` (the MCP server locks its VFS differently).
struct LspVfs<'a>(&'a std::sync::Arc<parking_lot::RwLock<vfs::Vfs>>);

impl ide_host_core::VfsWrite for LspVfs<'_> {
    fn with_write<R>(&self, f: impl FnOnce(&mut vfs::Vfs) -> R) -> R {
        let mut guard = self.0.write();
        f(&mut guard)
    }
}

/// What a [`GlobalState::process_changes`] batch did, so the caller can decide
/// whether already-open documents need re-analysis and re-publishing.
#[derive(Debug, Default, Clone, Copy)]
pub struct ChangeOutcome {
    /// A project config file (`bsl-analyzer.toml` / `.json`) changed, triggering
    /// a full project reload.
    pub config_file_changed: bool,
    pub diagnostics_baseline_changed: bool,
    /// A change was applied that can affect the analysis of *other* documents — a
    /// metadata XML edit or any `.bsl` source content (add / modify / delete). Open
    /// documents must be re-analyzed even though their own buffers did not change
    /// (e.g. files pulled in by `git pull` while editing).
    pub affects_open_documents: bool,
}

impl GlobalState {
    pub fn init_empty_source_root(&mut self) {
        use base_db::{SourceDatabase, SourceRoot, SourceRootId};

        let db = self.analysis_host.raw_database_mut();
        let source_root_id = SourceRootId(0);

        let file_set = vfs::file_set::FileSet::new();
        let source_root = SourceRoot::new_local(file_set);

        db.set_source_root(source_root_id, source_root);

        tracing::debug!("initialized empty SourceRoot(0) before event loop");
    }

    /// On an invalid project (unparseable config, invalid extension topology)
    /// the state is left untouched: the initial load then has no workspace at
    /// all, and a live reload keeps serving the last valid project. The caller
    /// surfaces the error to the client.
    pub fn set_workspace_root(&mut self, root: PathBuf) -> Result<(), project_model::ProjectError> {
        let start = Instant::now();
        tracing::info!(?root, "setting workspace root");

        let project = match project_model::Project::new(&root) {
            Ok(project) => project,
            Err(e) => {
                tracing::error!(error = %e, "invalid project; workspace root not set");
                return Err(e);
            }
        };

        for notice in [project.standalone_extension_notice(), project.standalone_external_notice()]
            .into_iter()
            .flatten()
        {
            self.show_warning_message(notice);
        }

        self.supersede_call_hierarchy_index(base_db::SourceRootId(0));

        let source_path = project.source_path().to_path_buf();
        let extensions: Vec<(String, PathBuf)> = project.extension_paths().to_vec();

        tracing::info!(
            ?source_path,
            extensions = extensions.len(),
            configuration_found = project.configuration_path().is_some(),
            "loaded project, scanning source path"
        );

        let configs_snapshot = ide_db::metadata::WorkspaceConfigsSnapshot::from_project(&project);
        let diagnostics_baseline =
            ide_host_core::diagnostics_baseline::DiagnosticsBaselineSnapshot::load(&project);
        self.workspace_root = Some(root.clone());
        self.project = Some(project);
        self.install_diagnostics_baseline(diagnostics_baseline);

        {
            self.analysis_host.request_cancellation();
            let db = self.analysis_host.raw_database_mut();
            db.set_workspace_configs_snapshot(configs_snapshot);
            // Close the whole-config loader gate for the INITIAL load only: the
            // vfs_done finalize reopens it before the metadata bootstrap and
            // warm-up. A live reload (config file edit) must not degrade
            // metadata resolution for already-running analysis.
            if !self.vfs_done {
                db.set_workspace_load_complete(false);
            }
        }

        self.update_diagnostics_config();
        self.update_features_config();
        // `[analysis].diff_base` may have (dis)appeared with the (re)loaded
        // config: rebuild the vendor-diff scope in the background.
        self.request_scope_rebuild();
        self.maybe_spawn_scope_build();
        self.warn_author_filter_unsupported();

        self.configure_loader();

        tracing::info!(
            elapsed_ms = start.elapsed().as_millis() as u64,
            "set_workspace_root complete (loader running async)",
        );
        Ok(())
    }

    fn configure_loader(&mut self) {
        let (Some(root), Some(project)) = (&self.workspace_root, &self.project) else { return };
        self.vfs_progress_config_version += 1;
        let mut config_files: Vec<_> = project_model::PROJECT_INPUT_FILE_NAMES
            .iter()
            .map(|name| paths::AbsPathBuf::assert_utf8(root.join(name)))
            .collect();
        config_files.sort();
        config_files.dedup();
        // NOTE: object names are content-addressed, so this list changes with every
        // baseline write and each one reconfigures the loader — a full workspace rescan.
        // Narrowing the watch to stable paths was tried and reverted: watching the
        // manifest alone hides a corrupted enabled object, and watching the object
        // directories makes edits to dormant partitions visible to the editor. Both
        // properties are asserted by tests, so the rescan is what a reconfiguration
        // still costs — the watch itself survives it, `vfs-notify` keeping its watcher
        // and adding only targets it has not already registered.
        let mut baseline_files: Vec<_> = self
            .diagnostics_baseline
            .observation_paths()
            .into_iter()
            .map(paths::AbsPathBuf::assert_utf8)
            .collect();
        baseline_files.sort();
        baseline_files.dedup();

        let include =
            project.source_roots().into_iter().map(paths::AbsPathBuf::assert_utf8).collect();
        for (name, path) in project.extension_paths() {
            tracing::info!(name, path = %path.display(), "adding extension to VFS scan");
        }
        let mut load = vec![loader::Entry::Directories(loader::Directories {
            extensions: project_model::SOURCE_EXTENSIONS.iter().map(|s| (*s).to_string()).collect(),
            include,
            exclude: vec![
                paths::AbsPathBuf::assert_utf8(root.join(".git")),
                paths::AbsPathBuf::assert_utf8(root.join("build")),
                paths::AbsPathBuf::assert_utf8(root.join(".vscode")),
            ],
            rules: vec![loader::FileRule {
                extensions: project_model::METADATA_WATCHED_EXTENSIONS
                    .iter()
                    .map(|s| (*s).to_string())
                    .collect(),
                load_mode: loader::LoadMode::WatchOnly,
            }],
        })];
        let mut watch = vec![0];
        if !config_files.is_empty() {
            load.push(loader::Entry::Files(config_files));
            watch.push(load.len() - 1);
        }
        if !baseline_files.is_empty() {
            load.push(loader::Entry::WatchOnlyFiles(baseline_files));
            watch.push(load.len() - 1);
        }
        self.loader.set_config(loader::Config {
            load,
            watch,
            version: self.vfs_progress_config_version,
        });
    }

    pub fn process_changes(&mut self, suppress_metadata_bump: bool) -> ChangeOutcome {
        use base_db::SourceDatabase;

        let start = Instant::now();
        let take_start = Instant::now();
        let changed_files = self.vfs.write().take_changes();
        let vfs_take_elapsed_ms = take_start.elapsed().as_millis() as u64;
        if changed_files.is_empty() {
            tracing::debug!(vfs_take_elapsed_ms, "process_changes: no VFS changes");
            return ChangeOutcome::default();
        }

        let file_count = changed_files.len();
        tracing::info!(file_count, vfs_take_elapsed_ms, "processing VFS changes");

        self.analysis_host.request_cancellation();

        let diagnostics_baseline_paths = self.diagnostics_baseline.observation_paths();

        let db = self.analysis_host.raw_database_mut();
        let source_root_id = base_db::SourceRootId(0);

        let source_root_input = db.source_root_input(source_root_id);
        let source_root = source_root_input.root(db);
        let mut file_set = source_root.file_set().clone();
        let mut file_set_modified = false;
        let mut config_file_changed = false;
        let mut diagnostics_baseline_changed = false;
        let mut bsl_source_changed = false;
        let mut call_hierarchy_body_edits = Vec::new();
        let mut call_hierarchy_structural_change = false;
        let mut changed_metadata_paths: Vec<std::path::PathBuf> = Vec::new();
        // Substrate-listed module bodies (common modules and HTTP/Web/Integration
        // services) that were added or removed (not merely edited): their per-MDO
        // listing `module_file` entry must be rebuilt.
        let mut changed_listed_bodies: Vec<std::path::PathBuf> = Vec::new();

        for file in changed_files {
            // Capture the change kind before `file.change` is consumed below; only an
            // add or remove (not a content edit) can change a common module's
            // `module_file` listing entry.
            let change_is_structural =
                matches!(file.change, vfs::Change::Create(..) | vfs::Change::Delete);
            let text = match file.change {
                vfs::Change::Create(content, _) | vfs::Change::Modify(content, _) => Some(content),
                vfs::Change::Delete => None,
            };

            let is_diagnostics_baseline = {
                let vfs = self.vfs.read();
                diagnostics_baseline_paths
                    .iter()
                    .any(|path| path == vfs.file_path(file.file_id).as_path())
            };
            if is_diagnostics_baseline {
                diagnostics_baseline_changed = true;
                continue;
            }

            db.set_file_source_root(file.file_id, source_root_id);

            let is_bsl_path = {
                let vfs = self.vfs.read();
                let path = vfs.file_path(file.file_id);
                let path_path = path.as_path();
                let file_name = path_path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if project_model::is_project_input_file_name(file_name) {
                    tracing::info!(path = %path_path.display(), "config file changed");
                    // Whether this is structural is decided after the reload
                    // attempt below: only a successful reload changes the
                    // effective project.
                    config_file_changed = true;
                }
                if project_model::is_metadata_path(path_path) {
                    tracing::info!(path = %path_path.display(), "metadata XML file changed");
                    changed_metadata_paths.push(path_path.to_path_buf());
                    call_hierarchy_structural_change = true;
                }
                if change_is_structural && project_model::is_substrate_listed_body_path(path_path) {
                    changed_listed_bodies.push(path_path.to_path_buf());
                }
                project_model::is_bsl_source_path(path_path)
            };

            if is_bsl_path {
                if change_is_structural {
                    call_hierarchy_structural_change = true;
                } else if text.is_some() {
                    call_hierarchy_body_edits.push(file.file_id);
                }
            }

            if is_bsl_path && text.is_none() {
                if file_set.path_for_file(&file.file_id).is_some() {
                    file_set.remove(file.file_id);
                    // `Deleted` rather than `Unreadable`: the VFS reports both as
                    // absent text, and the eviction below puts the file out of every
                    // consumer's reach anyway, so marking it unread would assert a
                    // cause nobody established and nobody could read back.
                    ide_host_core::set_file_text_source(
                        db,
                        file.file_id,
                        ide_host_core::FileTextSource::Deleted,
                    );
                    file_set_modified = true;
                    bsl_source_changed = true;
                    tracing::warn!(
                        file_id = file.file_id.0,
                        "BSL file evicted from FileSet (deleted or unreadable); text input emptied",
                    );
                }
                continue;
            }

            if file_set.path_for_file(&file.file_id).is_none() {
                let vfs = self.vfs.read();
                let path = vfs.file_path(file.file_id);
                file_set.insert(file.file_id, path.clone());
                drop(vfs);
                file_set_modified = true;

                tracing::debug!(
                    file_id = file.file_id.0,
                    "added file to FileSet during process_changes"
                );
            }

            if let Some(text) = text {
                let store_in_salsa = ide_db::is_bsl_source(&file_set, file.file_id);
                if store_in_salsa {
                    // Open editor buffers are the source of truth for unsaved
                    // content, so they stay a resident overlay. Closed files are
                    // recorded by content revision only and re-read from disk on
                    // demand (LRU-evictable), so a whole workspace's closed-file
                    // text is not pinned in memory. Open-ness is keyed by `FileId`
                    // (resolved at didOpen) to avoid URL-encoding misclassification.
                    let is_open = self.open_files.contains(&file.file_id);
                    tracing::debug!(
                        file_id = file.file_id.0,
                        text_len = text.len(),
                        is_open,
                        "process_changes: file text"
                    );
                    let source = if is_open {
                        ide_host_core::FileTextSource::Overlay(&text)
                    } else {
                        ide_host_core::FileTextSource::Disk(&text)
                    };
                    ide_host_core::set_file_text_source(db, file.file_id, source);
                    bsl_source_changed = true;
                }
            }
        }

        if file_set_modified {
            let updated_source_root = base_db::SourceRoot::new_local(file_set);
            db.set_source_root(source_root_id, updated_source_root);
        }

        if config_file_changed {
            if suppress_metadata_bump {
                tracing::debug!("suppressing project reload during initial sync");
            } else if self.reload_project_config() {
                call_hierarchy_structural_change = true;
            } else {
                // The edit produced an invalid config: the last-good project
                // stays in effect, so nothing downstream (call-hierarchy
                // index, batch diagnostics) may be torn down over it.
                config_file_changed = false;
            }
        }

        if diagnostics_baseline_changed && !suppress_metadata_bump {
            diagnostics_baseline_changed = self.reload_diagnostics_baseline();
        }

        if !changed_metadata_paths.is_empty() {
            if suppress_metadata_bump {
                tracing::debug!("suppressing metadata revision bump during initial sync");
            } else {
                let db = self.analysis_host.raw_database_mut();
                for path in &changed_metadata_paths {
                    tracing::info!(path = %path.display(), "bumping config revision after XML change");
                    db.bump_config_for_path(path);
                }
            }
        }

        // A listed module body `.bsl` add/remove (common module or service) changes its
        // per-MDO listing's `module_file` reverse-index entry, but the body is ordinary
        // source — it never flows through the metadata-XML refresh path. Re-discover
        // the owning roots so the reverse index reflects the new/removed body. (The
        // initial sync is suppressed; the post-sync bootstrap rebuilds the listings.)
        if !suppress_metadata_bump && !changed_listed_bodies.is_empty() {
            self.refresh_metadata_substrate(&changed_listed_bodies);
        }

        if !suppress_metadata_bump {
            if call_hierarchy_structural_change {
                self.supersede_call_hierarchy_index(source_root_id);
            } else if !call_hierarchy_body_edits.is_empty() {
                let generation = self.call_hierarchy_index.generation(source_root_id);
                let mut body_edit_applied = false;
                if let Some(generation) = generation {
                    for file_id in call_hierarchy_body_edits {
                        body_edit_applied |=
                            self.call_hierarchy_index.record_body_edit_or_supersede_ready(
                                source_root_id,
                                generation,
                                file_id,
                            );
                    }
                }
                if body_edit_applied && !self.call_hierarchy_index.has_active_build(source_root_id)
                {
                    self.schedule_call_hierarchy_index_build(source_root_id);
                }
            }
        }

        tracing::info!(
            file_count,
            vfs_take_elapsed_ms,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "process_changes complete",
        );

        // A suppressed batch (initial sync) neither reloaded the project nor bumped
        // metadata, so it must not claim those as observable changes.
        let metadata_changed = !suppress_metadata_bump && !changed_metadata_paths.is_empty();
        ChangeOutcome {
            config_file_changed: !suppress_metadata_bump && config_file_changed,
            diagnostics_baseline_changed: !suppress_metadata_bump && diagnostics_baseline_changed,
            affects_open_documents: bsl_source_changed
                || metadata_changed
                || (!suppress_metadata_bump && diagnostics_baseline_changed),
        }
    }

    fn install_diagnostics_baseline(
        &mut self,
        snapshot: ide_host_core::diagnostics_baseline::DiagnosticsBaselineSnapshot,
    ) {
        if snapshot.errors().is_empty() {
            self.diagnostics_baseline_notification_ledger.clear();
        }
        for error in snapshot.errors() {
            let key = format!("{}:{}", error.partition_id.as_deref().unwrap_or("set"), error.epoch);
            if self.diagnostics_baseline_notification_ledger.insert(key) {
                self.show_error_message(format!(
                    "bsl-analyzer: diagnostics baseline {}: {}",
                    error.code, error.detail
                ));
            }
        }
        self.diagnostics_baseline = std::sync::Arc::new(snapshot);
    }

    /// Reload the baseline when the ground moved under the snapshot in hand.
    ///
    /// The same question the resident asks before it answers, asked here for the same
    /// reason and over the same value: the watcher is armed asynchronously, after the
    /// loader has already announced the load finished, so a change landing before that
    /// raises no event at all. Without asking, the snapshot the initial load produced
    /// would answer for the life of the process — silently suppressing diagnostics
    /// against a baseline that is no longer on disk.
    pub(crate) fn refresh_diagnostics_baseline(&mut self) -> bool {
        /// How long an answer stands for.
        ///
        /// The question costs a stat per watched name and sits on the loop's hottest
        /// path, where one keystroke is one message, so it is not asked twice inside this
        /// window. What that buys is bounded and worth naming: a message arriving inside
        /// the window is answered from the previous answer, so a baseline that moved in
        /// it can be answered against once — and if no message ever follows, never
        /// noticed at all. Nothing is answered in that case either. The floor trades the
        /// last fraction of a second against a stat on every keystroke; the window it
        /// exists to cover, between the load and the watcher's arming, is orders of
        /// magnitude longer.
        const FLOOR: std::time::Duration = std::time::Duration::from_millis(250);

        let now = std::time::Instant::now();
        if self.diagnostics_baseline_checked_at.is_some_and(|last| now - last < FLOOR) {
            return false;
        }
        self.diagnostics_baseline_checked_at = Some(now);
        let Some(project) = self.project.as_ref() else { return false };
        if !self.diagnostics_baseline.moved_since_load(project) {
            return false;
        }
        self.reload_diagnostics_baseline()
    }

    pub(crate) fn reload_diagnostics_baseline(&mut self) -> bool {
        let Some(project) = self.project.as_ref() else { return false };
        let old_epoch = self.diagnostics_baseline.epoch().to_owned();
        let old_paths = self.diagnostics_baseline.observation_paths();
        let snapshot =
            ide_host_core::diagnostics_baseline::DiagnosticsBaselineSnapshot::load_reusing(
                project,
                &self.diagnostics_baseline,
            );
        let changed = snapshot.epoch() != old_epoch;
        let reconfigure = snapshot.observation_paths() != old_paths;
        self.install_diagnostics_baseline(snapshot);
        if reconfigure {
            self.configure_loader();
        }
        changed
    }

    /// Handle a removed directory subtree (delivered when a watch backend reports
    /// only the directory, not each child). Tombstones every loaded, closed file
    /// under one of `removed`, and invalidates the metadata of the owning config
    /// roots (a removed directory may have held metadata XML that, when coalesced,
    /// never arrived as its own event). Open files are left untouched — their
    /// editor buffer is authoritative. Returns whether open documents should be
    /// re-analyzed.
    pub fn remove_directories(&mut self, removed: &[paths::AbsPathBuf]) -> bool {
        use base_db::{SourceDatabase, SourceRootId};

        if removed.is_empty() {
            return false;
        }

        let mut descendants: Vec<VfsPath> = Vec::new();
        {
            let db = self.analysis_host.raw_database();
            let source_root = db.source_root_input(SourceRootId(0)).root(db);
            let file_set = source_root.file_set();
            for file_id in file_set.iter() {
                if self.open_files.contains(&file_id) {
                    continue;
                }
                let Some(vfs_path) = file_set.path_for_file(&file_id) else { continue };
                if removed.iter().any(|dir| vfs_path.as_path().starts_with(dir)) {
                    descendants.push(vfs_path.clone());
                }
            }
        }

        if !descendants.is_empty() {
            let mut vfs = self.vfs.write();
            for vfs_path in &descendants {
                vfs.set_file_contents(vfs_path.clone(), None);
            }
        }

        self.analysis_host.request_cancellation();
        self.analysis_host
            .raw_database_mut()
            .bump_config_for_paths(removed.iter().map(|p| p.as_ref()));
        self.supersede_call_hierarchy_index(SourceRootId(0));

        // Apply the tombstones. We always invalidated a config root above, which
        // `ChangeOutcome` does not reflect, so a refresh is warranted regardless.
        self.process_changes(false);
        true
    }

    pub fn reload_project_config(&mut self) -> bool {
        let Some(root) = self.workspace_root.clone() else {
            return false;
        };
        tracing::info!("reloading project config");
        if let Err(e) = self.set_workspace_root(root) {
            // Keep serving the last valid project; the config edit that broke
            // the file must not tear down a working workspace.
            self.show_error_message(format!("bsl-analyzer: project config reload failed: {e}"));
            return false;
        }
        self.prune_stale_workspace_files();
        true
    }

    fn prune_stale_workspace_files(&mut self) {
        use base_db::{SourceDatabase, SourceRoot, SourceRootId};

        let allowed_roots = self.workspace_allowed_roots();
        if allowed_roots.is_empty() {
            return;
        }
        let open_paths = self.open_doc_paths();

        let source_root_id = SourceRootId(0);
        let mut new_file_set = vfs::file_set::FileSet::new();
        let mut dropped = 0usize;
        {
            let db = self.analysis_host.raw_database();
            let source_root_input = db.source_root_input(source_root_id);
            let source_root = source_root_input.root(db);
            let file_set = source_root.file_set();

            for file_id in file_set.iter() {
                let Some(vfs_path) = file_set.path_for_file(&file_id) else { continue };
                if path_in_workspace(vfs_path.as_path(), &allowed_roots, &open_paths) {
                    new_file_set.insert(file_id, vfs_path.clone());
                } else {
                    dropped += 1;
                }
            }
        }

        if dropped == 0 {
            return;
        }

        tracing::info!(dropped, "pruning stale files from SourceRoot after workspace reconfig");

        self.analysis_host.request_cancellation();
        let new_source_root = SourceRoot::new_local(new_file_set);
        self.analysis_host.raw_database_mut().set_source_root(source_root_id, new_source_root);
    }

    fn workspace_allowed_roots(&self) -> Vec<PathBuf> {
        let Some(project) = self.project.as_ref() else { return Vec::new() };
        project.source_roots()
    }

    fn open_doc_paths(&self) -> HashSet<PathBuf> {
        self.mem_docs.uris().into_iter().filter_map(|u| u.to_file_path().ok()).collect()
    }

    /// Whether the editor currently has this path open as a document. Checks the
    /// `FileId` open-set (the authority `process_changes` uses to choose overlay
    /// vs disk) as well as the URL-keyed buffer set, because the file-watcher's
    /// URL encoding can differ from the client's didOpen URL — a mismatch would
    /// otherwise let a disk-sourced change overwrite an open file's unsaved
    /// overlay.
    pub fn is_open_document_path(&self, std_path: &Path, vfs_path: &VfsPath) -> bool {
        let by_url =
            Url::from_file_path(std_path).map(|url| self.mem_docs.contains(&url)).unwrap_or(false);
        let by_id = self
            .vfs
            .read()
            .file_id(vfs_path)
            .is_some_and(|file_id| self.open_files.contains(&file_id));
        by_url || by_id
    }

    pub fn opened_document_uris(&self) -> Vec<Url> {
        self.mem_docs.uris()
    }

    pub fn init_source_root(&mut self) {
        use base_db::{SourceDatabase, SourceRoot, BSL_SOURCE_ROOT};

        let start = Instant::now();

        let allowed_roots = self.workspace_allowed_roots();
        let open_paths = self.open_doc_paths();

        let vfs = self.vfs.read();

        // Rebuild root(0) FRESH from the current VFS, `.bsl` sources only. A fresh
        // rebuild (no merge with the previous set) means a renamed/removed file's
        // stale entry cannot linger across a reload, and excluding metadata XML
        // keeps it out of the BSL iterators that scan root(0). Metadata composing
        // files live in the dedicated metadata root(1), owned by
        // [`bootstrap_metadata_substrate`].
        let mut bsl_file_set = vfs::file_set::FileSet::new();

        let mut vfs_files_skipped = 0;

        for file_id_raw in 0..vfs.num_file_ids() {
            let file_id = vfs::FileId(file_id_raw);
            if !vfs.exists(file_id) {
                continue;
            }
            let path = vfs.file_path(file_id);
            if !path_in_workspace(path.as_path(), &allowed_roots, &open_paths) {
                vfs_files_skipped += 1;
                continue;
            }

            if project_model::is_bsl_source_path(path.as_path()) {
                bsl_file_set.insert(file_id, path.clone());
            } else {
                vfs_files_skipped += 1;
            }
        }

        let bsl_files = bsl_file_set.len();
        drop(vfs);

        if bsl_files == 0 {
            tracing::warn!(
                elapsed_ms = start.elapsed().as_millis() as u64,
                "no .bsl files in VFS during init_source_root (root(0) rebuilt empty)",
            );
        }

        {
            let db = self.analysis_host.raw_database_mut();

            // Publish the fresh set unconditionally — even empty — so a reload that
            // removed the last `.bsl` file clears the previous root(0) instead of
            // leaving stale entries alive.
            db.set_source_root(BSL_SOURCE_ROOT, SourceRoot::new_local(bsl_file_set));

            let bsl_ids: Vec<_> = db.source_root_input(BSL_SOURCE_ROOT).root(db).iter().collect();
            for file_id in bsl_ids {
                db.set_file_source_root(file_id, BSL_SOURCE_ROOT);
            }
        }
        self.supersede_call_hierarchy_index(BSL_SOURCE_ROOT);

        tracing::info!(
            bsl_files,
            vfs_files_skipped,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "rebuilt root(0) from VFS (.bsl only, fresh)",
        );
    }

    /// Build the metadata Salsa substrate from the filesystem for every config root.
    /// Thin delegation to the shared [`ide_host_core::AnalysisHost`] policy; this
    /// frontend only supplies its VFS. Runs after `init_source_root` and re-runs on
    /// reload.
    pub fn bootstrap_metadata_substrate(&mut self) {
        let vfs = LspVfs(&self.vfs);
        self.analysis_host.bootstrap_metadata_substrate(&vfs);
    }

    /// Incrementally refresh the metadata substrate for the config roots owning any
    /// of `changed_paths`. Thin delegation to the shared
    /// [`ide_host_core::AnalysisHost`] policy; returns whether any substrate input
    /// actually changed, so callers can gate a diagnostics refresh.
    pub fn refresh_metadata_substrate(&mut self, changed_paths: &[PathBuf]) -> bool {
        let vfs = LspVfs(&self.vfs);
        self.analysis_host.refresh_metadata_substrate(&vfs, changed_paths)
    }

    pub fn warm_metadata_cache(&mut self) {
        let Some(ref project) = self.project else {
            tracing::debug!("no project, skipping metadata warmup");
            return;
        };

        // A base is optional: an extension-only project has none. Only the base's own
        // warm-up is conditional on it — skipping the whole pass would leave every
        // root of such a project cold, which is the shape the shared-configuration
        // work exists to serve.
        let config_path = project.configuration_path().map(Path::to_path_buf);
        let _span = tracing::info_span!("warm_metadata_cache", ?config_path).entered();
        let start = Instant::now();

        let db = self.analysis_host.raw_database();
        if let Some(config_path) = config_path.as_deref() {
            let path_input = ide_db::metadata::intern_configuration_path(
                db,
                &config_path.to_string_lossy(),
                db.config_root_revision_for_path(config_path),
            );

            let config = ide_db::metadata::load_configuration(db, path_input);

            tracing::info!(
                common_modules = config.common_modules().len(),
                metadata_objects = config.metadata_objects().len(),
                registers = config.registers().len(),
                "metadata cache warmed"
            );
        }

        let extension_paths: Vec<_> = project.extension_paths().to_vec();
        for (name, ext_path) in &extension_paths {
            let ext_path_input = ide_db::metadata::intern_configuration_path(
                db,
                &ext_path.to_string_lossy(),
                db.config_root_revision_for_path(ext_path),
            );
            let ext_config = ide_db::metadata::load_configuration(db, ext_path_input);
            tracing::info!(
                extension = %name,
                common_modules = ext_config.common_modules().len(),
                metadata_objects = ext_config.metadata_objects().len(),
                "extension metadata cache warmed"
            );
        }

        let external_paths: Vec<_> = project.external_paths().to_vec();
        for (name, path) in &external_paths {
            let path_input = ide_db::metadata::intern_configuration_path(
                db,
                &path.to_string_lossy(),
                db.config_root_revision_for_path(path),
            );
            let config = ide_db::metadata::load_configuration(db, path_input);
            tracing::info!(
                external = %name,
                metadata_objects = config.metadata_objects().len(),
                "external metadata cache warmed"
            );
        }

        // Eager-warm the global context: a global common module extends the global context,
        // so its exported methods are callable unqualified. Building their symbol trees here —
        // through the SAME visibility-correct resolver path completion, hover and inference
        // take — means the first unqualified-call completion hits a warm cache instead of
        // parsing those modules on the keystroke. Cost is tiny (tens of ms, ~1 MB): global
        // modules are deliberately thin.
        {
            use base_db::{SourceDatabase, SourceRootId};
            use hir::{ConfigsDatabase, ModuleId, Resolver};

            // Any config-covered workspace file anchors the resolver's configuration
            // visibility; the global module set is configuration-wide, not file-specific.
            let anchor = db
                .source_root_input(SourceRootId(0))
                .root(db)
                .iter()
                .find(|&file_id| db.file_has_visible_config(file_id));

            if let Some(anchor) = anchor {
                let warm_start = Instant::now();
                let rss_before = crate::smoke::read_rss_bytes().unwrap_or(0);

                // The helper builds each global module's symbol tree to collect its exports —
                // calling it is the warm-up; the returned list only feeds the metrics below.
                let exports = Resolver::with_workspace_scope(ModuleId::new(anchor))
                    .global_common_module_exports(db);
                let modules: std::collections::HashSet<String> = exports
                    .entries
                    .iter()
                    .map(|entry| entry.module.as_str().to_lowercase())
                    .collect();

                let exported_methods = exports
                    .entries
                    .iter()
                    .filter(|entry| {
                        matches!(entry.definition, hir::GlobalExportDefinition::Method(_))
                    })
                    .count();
                let rss_after = crate::smoke::read_rss_bytes().unwrap_or(0);
                tracing::info!(
                    global_modules = modules.len(),
                    exported_methods,
                    elapsed_ms = warm_start.elapsed().as_millis() as u64,
                    rss_delta_mb = rss_after.saturating_sub(rss_before) / 1_048_576,
                    "global common modules warmed",
                );
            }
        }

        tracing::info!(
            elapsed_ms = start.elapsed().as_millis() as u64,
            "warm_metadata_cache complete",
        );
    }

    pub fn vfs_file_for_url(&mut self, url: &Url) -> Result<FileId> {
        let path = url.to_file_path().map_err(|_| anyhow!("Invalid file URL: {}", url))?;
        if !project_model::is_bsl_source_path(&path) {
            return Err(anyhow!("File is not BSL, LSP unsupported: {}", url));
        }

        let vfs_path = VfsPath::new(path);

        let mut vfs = self.vfs.write();

        if let Some(file_id) = vfs.file_id(&vfs_path) {
            Ok(file_id)
        } else {
            Ok(vfs.alloc_file_id(vfs_path))
        }
    }

    pub fn url_for_file_id(&self, file_id: FileId) -> Result<Url> {
        let vfs = self.vfs.read();
        let path = vfs.file_path(file_id);

        let std_path = path.as_path();

        Url::from_file_path(std_path)
            .map_err(|_| anyhow!("Failed to convert path to URL: {:?}", std_path))
    }
}

fn path_in_workspace(
    path: &Path,
    allowed_roots: &[PathBuf],
    open_paths: &HashSet<PathBuf>,
) -> bool {
    if allowed_roots.is_empty() {
        return true;
    }
    if allowed_roots.iter().any(|root| path.starts_with(root)) {
        return true;
    }
    open_paths.contains(path)
}

#[cfg(test)]
mod metadata_warmup_tests {
    use super::*;

    /// Runs `f` with an INFO-level subscriber writing into a buffer, and returns
    /// what it logged. `with_default` is thread-local, so parallel tests do not
    /// see each other's records.
    fn logged_during<T>(f: impl FnOnce() -> T) -> (T, String) {
        use std::sync::{Arc, Mutex};
        #[derive(Clone, Default)]
        struct Buf(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Buf {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buf {
            type Writer = Buf;
            fn make_writer(&'a self) -> Buf {
                self.clone()
            }
        }
        let buf = Buf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_writer(buf.clone())
            .without_time()
            .finish();
        let out = tracing::subscriber::with_default(subscriber, f);
        let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        (out, text)
    }

    fn extension_at(root: &Path, rel: &str) {
        let dir = root.join(rel);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("Configuration.xml"),
            "<Properties><ConfigurationExtensionPurpose>Customization\
             </ConfigurationExtensionPurpose></Properties>",
        )
        .unwrap();
    }

    fn warmup_log(build: impl FnOnce(&Path)) -> String {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        build(root);
        let (sender, _receiver) = crossbeam_channel::unbounded();
        let mut state = GlobalState::new(sender);
        state.init_empty_source_root();
        state.set_workspace_root(root.to_path_buf()).expect("valid project");
        let (_, log) = logged_during(|| state.warm_metadata_cache());
        log
    }

    /// The warm-up must reach every root the project has. A base is optional, so
    /// gating the whole pass on it leaves an extension-only project — the shape the
    /// shared-configuration work exists to serve — entirely cold.
    #[test]
    fn the_warm_up_reaches_extension_roots_with_and_without_a_base() {
        let with_base = warmup_log(|root| {
            let cf = root.join("src/cf");
            std::fs::create_dir_all(&cf).unwrap();
            std::fs::write(cf.join("Configuration.xml"), "<Configuration/>").unwrap();
            extension_at(root, "src/cfe/Feature");
            std::fs::write(
                root.join("bsl-analyzer.toml"),
                "[source]\nroot = \"src/cf\"\nextensions = [\"src/cfe/Feature\"]\n",
            )
            .unwrap();
        });
        // The control: with a base, both the base and the extension are warmed.
        assert!(with_base.contains("metadata cache warmed"), "base warmed: {with_base}");
        assert!(
            with_base.contains("extension metadata cache warmed"),
            "extension warmed alongside a base: {with_base}"
        );

        let without_base = warmup_log(|root| {
            extension_at(root, "Расширения/Feature");
            std::fs::write(
                root.join("bsl-analyzer.toml"),
                "[source]\nextensions = [\"Расширения/Feature\"]\n",
            )
            .unwrap();
        });
        assert!(
            without_base.contains("extension metadata cache warmed"),
            "an extension-only project must still be warmed: {without_base}"
        );
    }

    /// An external object is a root like the others: the warm-up that reaches
    /// every extension reaches it too, or its first request pays the parse.
    #[test]
    fn the_warm_up_reaches_external_roots() {
        let log = warmup_log(|root| {
            let cf = root.join("src/cf");
            std::fs::create_dir_all(&cf).unwrap();
            std::fs::write(cf.join("Configuration.xml"), "<Configuration/>").unwrap();
            let epf = root.join("src/epf/АРМ");
            std::fs::create_dir_all(epf.join("АРМ/Ext")).unwrap();
            std::fs::write(
                epf.join("АРМ.xml"),
                "<MetaDataObject xmlns=\"http://v8.1c.ru/8.3/MDClasses\" version=\"2.20\">\n\
                 <ExternalDataProcessor uuid=\"3696c164-ad14-4a0d-b659-10e3bf6d6ad2\">\n\
                 <Properties><Name>АРМ</Name></Properties>\n\
                 </ExternalDataProcessor>\n</MetaDataObject>\n",
            )
            .unwrap();
            std::fs::write(
                root.join("bsl-analyzer.toml"),
                "[source]\nroot = \"src/cf\"\n\
                 externals = [{ name = \"АРМ\", path = \"src/epf/АРМ\" }]\n",
            )
            .unwrap();
        });
        assert!(log.contains("metadata cache warmed"), "control: the base is warmed: {log}");
        assert!(
            log.contains("external metadata cache warmed"),
            "the external root is warmed alongside the base: {log}"
        );
    }
}

#[cfg(test)]
mod diagnostics_baseline_tests {
    use super::*;
    use ide::diagnostics_baseline::{
        diagnostics_baseline_json, DiagnosticsBaseline, DiagnosticsBaselineScope,
        DIAGNOSTICS_BASELINE_SCHEMA_VERSION,
    };
    use std::io::Write;
    use std::sync::Arc;

    fn partitioned_baseline_reload_reuses_arcs_and_preserves_salsa(selective: bool) {
        use ide::diagnostics_baseline::{
            diagnostic_fingerprint, DiagnosticsBaselineEntry, DiagnosticsBaselineRange,
        };
        use ide::partitioned_diagnostics_baseline::{
            diagnostics_manifest, diagnostics_manifest_json, diagnostics_partition_json,
            partition_object_path, DiagnosticsBaselineManifestEntry,
        };

        let dir = tempfile::tempdir().unwrap();
        let root = &dir.path().canonicalize().unwrap();
        for source in ["src/cf", "src/cfe/Ext", "src/cfe/Dormant"] {
            std::fs::create_dir_all(root.join(source)).unwrap();
            std::fs::write(root.join(source).join("Configuration.xml"), "<Configuration/>")
                .unwrap();
        }
        let include = if selective { "include = [\"main\", \"extension:Ext\"]\n" } else { "" };
        std::fs::write(
            root.join("bsl-analyzer.toml"),
            format!(
                r#"[source]
root = "src/cf"
extensions = [{{ name = "Ext", path = "src/cfe/Ext" }}, {{ name = "Dormant", path = "src/cfe/Dormant" }}]
[diagnostics.baseline]
directory = "baselines"
{include}
"#,
            ),
        )
        .unwrap();
        let project = project_model::Project::new(root).unwrap();
        let plan = project.diagnostics_baseline_partition_plan().unwrap().unwrap();
        if selective {
            assert_eq!(plan.enabled_partition_ids, ["main", "extension:Ext"]);
            assert_eq!(plan.partitions.len(), 3);
        }
        let directory =
            project_model::ManagedBaselineDirectory::open(root, "baselines", true).unwrap();
        let publish = |main: Vec<DiagnosticsBaselineEntry>| {
            let mut entries = Vec::new();
            for partition in &plan.partitions {
                let diagnostics = if partition.id == "main" { main.clone() } else { vec![] };
                let bytes =
                    diagnostics_partition_json(partition.identity.clone(), diagnostics).unwrap();
                let hash = blake3::hash(&bytes).to_hex().to_string();
                let path = partition_object_path(&partition.id, &partition.key, &hash).unwrap();
                if directory.open_file(&path).is_err() {
                    directory.create_file_new(&path).unwrap().write_all(&bytes).unwrap();
                }
                entries.push(DiagnosticsBaselineManifestEntry {
                    partition_id: partition.id.clone(),
                    file: path,
                    blake3: hash,
                });
            }
            let manifest = diagnostics_manifest(plan.project_scope_fingerprint.clone(), entries);
            let bytes = diagnostics_manifest_json(&manifest).unwrap();
            let temp = "manifest.next.json";
            if directory.open_file(temp).is_ok() {
                directory.remove_file(temp).unwrap();
            }
            directory.create_file_new(temp).unwrap().write_all(&bytes).unwrap();
            directory.replace_file(temp, "manifest.json").unwrap();
            (manifest, bytes)
        };
        publish(vec![]);

        let (sender, _receiver) = crossbeam_channel::unbounded();
        let mut state = GlobalState::new(sender);
        state.init_empty_source_root();
        state.set_workspace_root(root.to_path_buf()).unwrap();
        let database = std::ptr::from_ref(state.analysis_host.raw_database());
        let (first, _, _) = state.diagnostics_baseline.ready_set().unwrap();
        let old_extension = first.partitions["extension:Ext"].clone();
        assert_eq!(
            state.diagnostics_baseline.observation_paths().len(),
            if selective { 3 } else { 4 }
        );

        let path = "src/cf/Main.bsl";
        let snippet = "Message(1);";
        let entry = DiagnosticsBaselineEntry {
            fingerprint: diagnostic_fingerprint(path, "LineLength", snippet, 0),
            path: path.to_owned(),
            code: "LineLength".to_owned(),
            snippet: snippet.to_owned(),
            occurrence: 0,
            message: "message".to_owned(),
            severity: "Warning".to_owned(),
            range: DiagnosticsBaselineRange {
                start_line: 0,
                start_column: 0,
                end_line: 0,
                end_column: 1,
            },
        };
        let (second, second_manifest) = publish(vec![entry]);
        let manifest_path = root.join("baselines/manifest.json");
        state.vfs.write().set_file_contents(
            VfsPath::new(manifest_path),
            Some(Arc::from(String::from_utf8(second_manifest).unwrap())),
        );
        let outcome = state.process_changes(false);
        assert!(outcome.diagnostics_baseline_changed);
        let (set, _, _) = state.diagnostics_baseline.ready_set().unwrap();
        assert!(Arc::ptr_eq(&old_extension, &set.partitions["extension:Ext"]));
        assert_eq!(std::ptr::from_ref(state.analysis_host.raw_database()), database);

        let extension =
            second.partitions.iter().find(|entry| entry.partition_id == "extension:Ext").unwrap();
        let extension_path = root.join("baselines").join(&extension.file);
        let valid = std::fs::read_to_string(&extension_path).unwrap();
        std::fs::write(&extension_path, "{broken").unwrap();
        state
            .vfs
            .write()
            .set_file_contents(VfsPath::new(extension_path.clone()), Some(Arc::from("{broken")));
        assert!(state.process_changes(false).diagnostics_baseline_changed);
        assert!(matches!(
            &*state.diagnostics_baseline,
            ide_host_core::diagnostics_baseline::DiagnosticsBaselineSnapshot::Error { .. }
        ));
        std::fs::write(&extension_path, &valid).unwrap();
        state.vfs.write().set_file_contents(VfsPath::new(extension_path), Some(Arc::from(valid)));
        assert!(state.process_changes(false).diagnostics_baseline_changed);
        assert!(state.diagnostics_baseline.ready_set().is_some());
        assert_eq!(std::ptr::from_ref(state.analysis_host.raw_database()), database);
    }

    #[test]
    fn lsp_partitioned_baseline_reload_reuses_arcs_and_preserves_salsa() {
        partitioned_baseline_reload_reuses_arcs_and_preserves_salsa(false);
    }

    #[test]
    fn selective_lsp_enabled_object_reload_reuses_salsa_and_arcs() {
        partitioned_baseline_reload_reuses_arcs_and_preserves_salsa(true);
    }

    #[test]
    fn lsp_diagnostics_baseline_reload_handles_write_replace_and_delete_without_replacing_salsa() {
        let dir = tempfile::tempdir().unwrap();
        let root = &dir.path().canonicalize().unwrap();
        let baseline_path = root.join("baseline.json");
        std::fs::write(
            root.join("bsl-analyzer.toml"),
            "[diagnostics.baseline]\npath = \"baseline.json\"\n",
        )
        .unwrap();
        let baseline = DiagnosticsBaseline {
            schema_version: DIAGNOSTICS_BASELINE_SCHEMA_VERSION,
            scope: DiagnosticsBaselineScope { source_root: None, extensions: vec![] },
            diagnostics: vec![],
        };
        let bytes = diagnostics_baseline_json(&baseline).unwrap();
        std::fs::write(&baseline_path, &bytes).unwrap();

        let (sender, _receiver) = crossbeam_channel::unbounded();
        let mut state = GlobalState::new(sender);
        state.init_empty_source_root();
        state.set_workspace_root(root.to_path_buf()).unwrap();
        let database = std::ptr::from_ref(state.analysis_host.raw_database());
        let first_epoch = state.diagnostics_baseline.epoch().to_owned();

        let mut changed = bytes.clone();
        changed.push(b'\n');
        std::fs::write(&baseline_path, &changed).unwrap();
        state.vfs.write().set_file_contents(
            VfsPath::new(baseline_path.clone()),
            Some(Arc::from(String::from_utf8(changed).unwrap())),
        );
        assert!(state.process_changes(false).affects_open_documents);
        assert_ne!(state.diagnostics_baseline.epoch(), first_epoch);
        assert_eq!(std::ptr::from_ref(state.analysis_host.raw_database()), database);

        let previous_epoch = state.diagnostics_baseline.epoch().to_owned();
        let mut replacement = tempfile::NamedTempFile::new_in(root).unwrap();
        replacement.write_all(b" \n").unwrap();
        replacement.write_all(&bytes).unwrap();
        replacement.persist(&baseline_path).unwrap();
        let replacement_text = std::fs::read_to_string(&baseline_path).unwrap();
        state.vfs.write().set_file_contents(
            VfsPath::new(baseline_path.clone()),
            Some(Arc::from(replacement_text)),
        );
        state.process_changes(false);
        assert_ne!(state.diagnostics_baseline.epoch(), previous_epoch);
        assert_eq!(std::ptr::from_ref(state.analysis_host.raw_database()), database);

        std::fs::remove_file(&baseline_path).unwrap();
        state.vfs.write().set_file_contents(VfsPath::new(baseline_path), None);
        state.process_changes(false);
        assert!(matches!(
            &*state.diagnostics_baseline,
            ide_host_core::diagnostics_baseline::DiagnosticsBaselineSnapshot::Error {
                code,
                ..
            } if code == "missing"
        ));
        assert_eq!(std::ptr::from_ref(state.analysis_host.raw_database()), database);
    }

    #[test]
    fn lsp_diagnostics_baseline_error_notifies_once_per_fingerprint_and_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let baseline_path = root.join("baseline.json");
        std::fs::write(
            root.join("bsl-analyzer.toml"),
            "[diagnostics.baseline]\npath = \"baseline.json\"\n",
        )
        .unwrap();
        std::fs::write(&baseline_path, "{broken").unwrap();

        let (sender, receiver) = crossbeam_channel::unbounded();
        let mut state = GlobalState::new(sender);
        state.init_empty_source_root();
        state.set_workspace_root(root.to_path_buf()).unwrap();
        assert!(matches!(receiver.recv().unwrap(), lsp_server::Message::Notification(_)));

        state.reload_diagnostics_baseline();
        assert!(receiver.try_recv().is_err(), "same fingerprint must not notify twice");

        std::fs::write(&baseline_path, "{different").unwrap();
        state.reload_diagnostics_baseline();
        assert!(matches!(receiver.recv().unwrap(), lsp_server::Message::Notification(_)));

        let valid = diagnostics_baseline_json(&DiagnosticsBaseline {
            schema_version: DIAGNOSTICS_BASELINE_SCHEMA_VERSION,
            scope: DiagnosticsBaselineScope { source_root: None, extensions: vec![] },
            diagnostics: vec![],
        })
        .unwrap();
        std::fs::write(&baseline_path, valid).unwrap();
        state.reload_diagnostics_baseline();
        assert!(matches!(
            &*state.diagnostics_baseline,
            ide_host_core::diagnostics_baseline::DiagnosticsBaselineSnapshot::Ready { .. }
        ));
        assert!(receiver.try_recv().is_err(), "recovery is silent");
    }
}
