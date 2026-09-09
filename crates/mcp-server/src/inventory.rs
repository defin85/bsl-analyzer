use std::path::{Path, PathBuf};

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("source directory is readable") {
        let path = entry.expect("source entry is readable").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs")
            && path.file_name().is_none_or(|name| name != "inventory.rs")
        {
            out.push(path);
        }
    }
}

fn production_sources() -> Vec<PathBuf> {
    let mut sources = Vec::new();
    rust_sources(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src"), &mut sources);
    sources
}

fn production_prefix(source: &str) -> &str {
    ["\n#[cfg(test)]\nmod ", "\r\n#[cfg(test)]\r\nmod "]
        .into_iter()
        .filter_map(|boundary| source.find(boundary))
        .min()
        .map_or(source, |end| &source[..end])
}

#[test]
fn no_generic_or_unclassified_production_lease_callers() {
    let forbidden = ["with_ownership_outcome", "with_ownership_checkpointed", "LeaseOutcome"];
    for path in production_sources() {
        let source = std::fs::read_to_string(&path).expect("Rust source is readable");
        for token in forbidden {
            assert!(!source.contains(token), "{} still contains {token}", path.display());
        }
    }
}

#[test]
fn fence_callers_are_exactly_classified() {
    let expected = [
        ("graph/build.rs", 2),
        ("graph/snapshot.rs", 2),
        ("state/bootstrap.rs", 2),
        ("state/embed.rs", 4),
        ("state/mod.rs", 2),
        ("workspace_lease.rs", 1),
    ];
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for path in production_sources() {
        let source = std::fs::read_to_string(&path).expect("Rust source is readable");
        let source = production_prefix(&source);
        let actual = source.matches(".publish_short(").count()
            + source.matches(".publish_checkpointed(").count();
        let relative = path.strip_prefix(&root).unwrap();
        let classified = expected
            .iter()
            .find_map(|(path, count)| (Path::new(path) == relative).then_some(*count))
            .unwrap_or(0);
        assert_eq!(actual, classified, "unclassified fence caller count in {}", relative.display());
    }
}

#[test]
fn request_paths_do_not_call_lease_or_mutation_helpers() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let lib = root.join("lib.rs");
    let tools = root.join("tools");
    let forbidden = [
        ".publish_short(",
        ".publish_checkpointed(",
        ".owns_caches(",
        ".owns_caches_now(",
        "apply_workspace_search(",
        "apply_workspace_search_checkpointed(",
        "prefetch_resident_overlay",
        "snapshot_blocking(",
    ];
    for path in
        production_sources().into_iter().filter(|path| path == &lib || path.starts_with(&tools))
    {
        let source = std::fs::read_to_string(&path).expect("Rust source is readable");
        let source = production_prefix(&source);
        for token in forbidden {
            assert!(!source.contains(token), "{} contains request-time {token}", path.display());
        }
    }
}

#[test]
fn production_prefix_is_portable_and_keeps_test_only_statics() {
    for newline in ["\n", "\r\n"] {
        let production =
            ["fn runtime() {}", "#[cfg(test)]", "static HOOK: bool = false;"].join(newline);
        let source = format!("{production}{newline}#[cfg(test)]{newline}mod tests {{}}");
        assert_eq!(production_prefix(&source), production);
    }
}
