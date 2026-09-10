//! Disk probes for constructed conventional paths.
//!
//! Policy: probe the canonical spelling first — a configurator-written tree
//! hits it and pays nothing extra — and only on a miss list the parent once,
//! matching case-insensitively. The returned path always carries the REAL
//! on-disk spelling, because it flows into `module_file` fields and URIs that
//! must agree with the scanned universe.
//!
//! On a case-insensitive filesystem the exact probe hits for any spelling, so
//! behaviour there is unchanged from the historical `exists()` probes: the
//! constructed spelling is returned. The real-spelling guarantee holds on
//! case-sensitive filesystems, where a wrong-case construction actually
//! misses.

use std::path::{Path, PathBuf};

use crate::tree::{DirTree, RealFs};

/// Find a child whose name is a WHOLLY conventional spelling (`Ext`,
/// `Module.bsl`, `Configuration.xml`): the entire name matches
/// case-insensitively. Never use this for a name built from an object name —
/// that is [`find_child_stem_exact`]'s contract.
pub fn find_child_ci(dir: &Path, conventional: &str) -> Option<PathBuf> {
    find_child_ci_in(&RealFs, dir, conventional)
}

/// [`find_child_ci`] against a given tree.
pub fn find_child_ci_in(tree: &dyn DirTree, dir: &Path, conventional: &str) -> Option<PathBuf> {
    let exact = dir.join(conventional);
    if tree.kind_of(&exact).is_some() {
        return Some(exact);
    }
    for name in tree.child_names(dir) {
        if name.to_str().is_some_and(|n| n.eq_ignore_ascii_case(conventional)) {
            return Some(dir.join(name));
        }
    }
    None
}

/// Find a child built as `{stem}.{ext}` where `stem` is an OBJECT name: the
/// stem must match EXACTLY (an object's case is significant), only the
/// extension is compared case-insensitively. `Alpha.xml` therefore never
/// matches a neighbour's `alpha.xml`.
pub fn find_child_stem_exact(dir: &Path, stem: &str, ext: &str) -> Option<PathBuf> {
    find_child_stem_exact_in(&RealFs, dir, stem, ext)
}

/// [`find_child_stem_exact`] against a given tree.
pub fn find_child_stem_exact_in(
    tree: &dyn DirTree,
    dir: &Path,
    stem: &str,
    ext: &str,
) -> Option<PathBuf> {
    let exact = dir.join(format!("{stem}.{ext}"));
    if tree.kind_of(&exact).is_some() {
        return Some(exact);
    }
    for name in tree.child_names(dir) {
        let path: &Path = name.as_ref();
        let stem_matches = path.file_stem().is_some_and(|s| s == std::ffi::OsStr::new(stem));
        let ext_matches =
            path.extension().and_then(|e| e.to_str()).is_some_and(|e| e.eq_ignore_ascii_case(ext));
        if stem_matches && ext_matches {
            return Some(dir.join(name));
        }
    }
    None
}

/// Resolve a chain of wholly conventional components (`["Ext", "Module.bsl"]`)
/// under `dir`, each level by [`find_child_ci`]. A single-level listing cannot
/// survive `EXT/MODULE.BSL` — when the joined probe misses, every component
/// may be misspelled, so each is resolved in turn.
pub fn resolve_chain_ci(dir: &Path, components: &[&str]) -> Option<PathBuf> {
    resolve_chain_ci_in(&RealFs, dir, components)
}

/// [`resolve_chain_ci`] against a given tree.
pub fn resolve_chain_ci_in(tree: &dyn DirTree, dir: &Path, components: &[&str]) -> Option<PathBuf> {
    let mut current = dir.to_path_buf();
    for component in components {
        current = find_child_ci_in(tree, &current, component)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, fs};

    struct ObservedFs {
        typed_listings: Cell<usize>,
    }

    impl DirTree for ObservedFs {
        fn child_names(&self, dir: &Path) -> Vec<std::ffi::OsString> {
            RealFs.child_names(dir)
        }

        fn kind_of(&self, path: &Path) -> Option<crate::EntryKind> {
            RealFs.kind_of(path)
        }

        fn entries(&self, dir: &Path) -> Vec<crate::TreeEntry> {
            self.typed_listings.set(self.typed_listings.get() + 1);
            RealFs.entries(dir)
        }
    }

    #[test]
    fn a_missing_name_does_not_request_sibling_kinds() {
        let dir = tempfile::tempdir().unwrap();
        touch(&dir.path().join("Other.xml"));
        fs::create_dir(dir.path().join("CommonModule")).unwrap();
        let tree = ObservedFs { typed_listings: Cell::new(0) };

        assert_eq!(find_child_ci_in(&tree, dir.path(), "Configuration.xml"), None);
        assert_eq!(tree.typed_listings.get(), 0, "name lookup must not inspect sibling kinds");
    }

    #[test]
    fn a_missing_stem_does_not_request_sibling_kinds() {
        let dir = tempfile::tempdir().unwrap();
        touch(&dir.path().join("Other.xml"));
        let tree = ObservedFs { typed_listings: Cell::new(0) };

        assert_eq!(find_child_stem_exact_in(&tree, dir.path(), "Missing", "xml"), None);
        assert_eq!(tree.typed_listings.get(), 0, "stem lookup must not inspect sibling kinds");
    }

    #[test]
    fn snapshot_fallback_preserves_case_sensitive_names() {
        let tree = crate::PathSetTree::from_files([
            PathBuf::from("/ws/EXT/MODULE.BSL"),
            PathBuf::from("/ws/Товар.XML"),
            PathBuf::from("/ws/alpha.xml"),
        ]);
        assert_eq!(
            resolve_chain_ci_in(&tree, Path::new("/ws"), &["Ext", "Module.bsl"]),
            Some(PathBuf::from("/ws/EXT/MODULE.BSL")),
        );
        assert_eq!(
            find_child_stem_exact_in(&tree, Path::new("/ws"), "Товар", "xml"),
            Some(PathBuf::from("/ws/Товар.XML")),
        );
        assert_eq!(find_child_stem_exact_in(&tree, Path::new("/ws"), "Alpha", "xml"), None);
        assert_eq!(find_child_ci_in(&tree, Path::new("/ws"), "Configuration.xml"), None);
    }

    #[test]
    fn an_exact_name_probe_does_not_list_children() {
        struct ExactOnly;
        impl DirTree for ExactOnly {
            fn kind_of(&self, _: &Path) -> Option<crate::EntryKind> {
                Some(crate::EntryKind::File)
            }
            fn entries(&self, _: &Path) -> Vec<crate::TreeEntry> {
                panic!("exact lookup must not enumerate siblings")
            }
            fn child_names(&self, _: &Path) -> Vec<std::ffi::OsString> {
                panic!("exact lookup must not enumerate siblings")
            }
        }
        assert_eq!(
            find_child_ci_in(&ExactOnly, Path::new("/ws"), "Module.bsl"),
            Some(PathBuf::from("/ws/Module.bsl")),
        );
        assert_eq!(
            find_child_stem_exact_in(&ExactOnly, Path::new("/ws"), "Товар", "xml"),
            Some(PathBuf::from("/ws/Товар.xml")),
        );
    }

    #[test]
    fn missing_directories_and_file_parents_have_no_children() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("Other.xml");
        touch(&file);
        for parent in [dir.path().join("missing"), file] {
            assert!(RealFs.child_names(&parent).is_empty());
            assert_eq!(find_child_ci(&parent, "Module.bsl"), None);
            assert_eq!(find_child_stem_exact(&parent, "Товар", "xml"), None);
        }
    }

    #[cfg(unix)]
    #[test]
    fn name_only_listing_keeps_symlinks() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        touch(&target.path().join("Module.bsl"));
        symlink(target.path(), dir.path().join("EXT")).unwrap();
        symlink(target.path().join("Module.bsl"), dir.path().join("MODULE.BSL")).unwrap();
        symlink(target.path().join("absent"), dir.path().join("Configuration.xml")).unwrap();

        let mut names = RealFs.child_names(dir.path());
        names.sort();
        let mut typed_names: Vec<_> = RealFs
            .entries(dir.path())
            .iter()
            .map(|entry| entry.path.file_name().unwrap().to_owned())
            .collect();
        typed_names.sort();
        assert_eq!(names, typed_names);

        assert_eq!(
            find_child_ci(dir.path(), "Configuration.xml"),
            Some(dir.path().join("Configuration.xml"))
        );
        assert_eq!(find_child_ci(dir.path(), "Missing.xml"), None);
        assert!(find_child_ci(dir.path(), "Module.bsl").unwrap().is_file());
        assert!(resolve_chain_ci(dir.path(), &["Ext", "Module.bsl"]).unwrap().is_file());
        assert!(RealFs
            .entries(dir.path())
            .iter()
            .find(|e| e.path.ends_with("EXT"))
            .unwrap()
            .is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn a_non_unicode_sibling_does_not_hide_a_conventional_name() {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt};
        let parent = Path::new("/ws");
        let non_unicode = OsString::from_vec(vec![0xff]);
        let tree = crate::PathSetTree::from_files([
            parent.join(&non_unicode),
            parent.join("CONFIGURATION.XML"),
        ]);
        assert!(tree.child_names(parent).contains(&non_unicode));
        assert_eq!(
            find_child_ci_in(&tree, parent, "Configuration.xml"),
            Some(parent.join("CONFIGURATION.XML")),
        );
    }

    fn touch(path: &Path) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, "").unwrap();
    }

    /// Does this volume keep two names that differ only in case apart? Where it
    /// does not (APFS and NTFS in their default setup), the exact probe hits
    /// whatever the spelling and the constructed path is what comes back. The
    /// volume is asked rather than the target OS, so a case-sensitive volume
    /// mounted on macOS or Windows still gets the real-spelling assertion.
    fn fs_keeps_case_distinct(dir: &Path) -> bool {
        let probe = dir.join("bsl_case_distinction_probe");
        if fs::write(&probe, "").is_err() {
            return false;
        }
        let distinct = fs::metadata(dir.join("BSL_CASE_DISTINCTION_PROBE")).is_err();
        let _ = fs::remove_file(&probe);
        distinct
    }

    #[test]
    fn an_exact_probe_hits_without_listing() {
        let dir = tempfile::tempdir().unwrap();
        touch(&dir.path().join("Module.bsl"));
        let found = find_child_ci(dir.path(), "Module.bsl").unwrap();
        assert!(found.ends_with("Module.bsl"));
    }

    /// На регистронезависимой ФС точная проба попадает при любом написании и
    /// листинг не выполняется — там возвращается сконструированное написание,
    /// как и до этого хелпера (граница узла).
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    #[test]
    fn a_case_variant_is_found_by_listing_and_keeps_its_real_spelling() {
        let dir = tempfile::tempdir().unwrap();
        touch(&dir.path().join("Module.BSL"));
        let found = find_child_ci(dir.path(), "Module.bsl").unwrap();
        assert_eq!(found.file_name().unwrap(), "Module.BSL", "real spelling, not the probe's");
    }

    #[test]
    fn an_absent_child_is_none() {
        let dir = tempfile::tempdir().unwrap();
        touch(&dir.path().join("Other.bsl"));
        assert_eq!(find_child_ci(dir.path(), "Module.bsl"), None);
    }

    #[test]
    fn a_stem_probe_takes_a_case_variant_extension() {
        let dir = tempfile::tempdir().unwrap();
        touch(&dir.path().join("Товар.XML"));
        let found = find_child_stem_exact(dir.path(), "Товар", "xml").unwrap();
        let expected =
            if fs_keeps_case_distinct(dir.path()) { "Товар.XML" } else { "Товар.xml" };
        assert_eq!(found.file_name().unwrap(), expected);
    }

    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    #[test]
    fn a_stem_probe_never_takes_a_case_variant_stem() {
        let dir = tempfile::tempdir().unwrap();
        touch(&dir.path().join("alpha.xml"));
        assert_eq!(
            find_child_stem_exact(dir.path(), "Alpha", "xml"),
            None,
            "an object name's case is significant: alpha.xml is another object"
        );
    }

    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    #[test]
    fn a_chain_resolves_each_component_by_its_own_listing() {
        let dir = tempfile::tempdir().unwrap();
        touch(&dir.path().join("EXT/MODULE.BSL"));
        let found = resolve_chain_ci(dir.path(), &["Ext", "Module.bsl"]).unwrap();
        assert!(found.ends_with("EXT/MODULE.BSL"), "{}", found.display());
    }

    #[test]
    fn a_chain_with_a_missing_link_is_none() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("EXT")).unwrap();
        assert_eq!(resolve_chain_ci(dir.path(), &["Ext", "Module.bsl"]), None);
    }
}
