//! Shared LSP harness for the diagnostics-baseline tests.
//!
//! Lives under `tests/common/` so it is a module, not a test target: included from a
//! sibling test file, every `#[test]` in it would be compiled and RUN once per
//! including target.

#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use serde_json::{json, Value};

pub const BROKEN: &str = "Процедура Тест(\n";

pub fn project() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("src")).unwrap();
    std::fs::write(dir.path().join("src/Main.bsl"), BROKEN).unwrap();
    std::fs::write(
        dir.path().join("bsl-analyzer.toml"),
        "[source]\nroot = \"src\"\n\n[diagnostics.baseline]\npath = \"baseline.json\"\n",
    )
    .unwrap();
    let created = Command::new(env!("CARGO_BIN_EXE_bsl-analyzer-app"))
        .current_dir(dir.path())
        .args(["diagnostics", "baseline", "create", "-s", "."])
        .output()
        .unwrap();
    assert!(created.status.success(), "{}", String::from_utf8_lossy(&created.stderr));
    dir
}

/// An action repeated until the server reacts to it.
///
/// The server's file watcher is armed asynchronously and nothing announces when; a change
/// written before that raises no event at all, so a single act and a long wait would wait
/// out a notification that is never coming. Bounded, so a server that reacts to nothing
/// fails the test with a message rather than spinning.
pub struct Provocation<F> {
    act: F,
    deadline: std::time::Instant,
}

impl<F: Fn()> Provocation<F> {
    pub fn start(act: F) -> Self {
        act();
        Self { act, deadline: std::time::Instant::now() + Duration::from_secs(60) }
    }

    pub fn again(&self) {
        assert!(
            std::time::Instant::now() < self.deadline,
            "the server never reacted to the provocation",
        );
        (self.act)();
    }
}

pub struct Lsp {
    pub child: Child,
    pub stdin: ChildStdin,
    pub messages: Receiver<Value>,
}

impl Lsp {
    pub fn start(root: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_bsl-analyzer-app"))
            .arg("lsp")
            .current_dir(root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, messages) = mpsc::channel();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                let mut length = None;
                loop {
                    let mut header = String::new();
                    if reader.read_line(&mut header).ok().filter(|&n| n > 0).is_none() {
                        return;
                    }
                    if header == "\r\n" {
                        break;
                    }
                    if let Some(value) = header.strip_prefix("Content-Length:") {
                        length = value.trim().parse::<usize>().ok();
                    }
                }
                let Some(length) = length else { return };
                let mut body = vec![0; length];
                if reader.read_exact(&mut body).is_err() {
                    return;
                }
                if let Ok(message) = serde_json::from_slice(&body) {
                    if tx.send(message).is_err() {
                        return;
                    }
                }
            }
        });
        let mut lsp = Self { child, stdin, messages };
        let root_uri = lsp_types::Url::from_directory_path(root).unwrap();
        lsp.send(json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"rootUri": root_uri, "capabilities": {}}
        }));
        lsp.wait_for(|message| message["id"] == 1);
        lsp.send(json!({"jsonrpc": "2.0", "method": "initialized", "params": {}}));
        lsp
    }

    pub fn send(&mut self, message: Value) {
        let body = message.to_string();
        write!(self.stdin, "Content-Length: {}\r\n\r\n{}", body.len(), body).unwrap();
        self.stdin.flush().unwrap();
    }

    /// Wait for a message, allowing the server sixty seconds to produce EACH one.
    ///
    /// Per message, not per wait: a server that is working says so — progress, logs —
    /// and a stand behind a cold build on a loaded machine can spend longer than a
    /// minute reaching what it is waiting for without ever having gone quiet.
    pub fn wait_for(&self, predicate: impl Fn(&Value) -> bool) -> Value {
        loop {
            let message = self
                .wait_for_within(Duration::from_secs(60), |_| true)
                .expect("the server answered nothing for a minute");
            if predicate(&message) {
                return message;
            }
        }
    }

    /// Wait for a message, reporting silence instead of failing the test.
    ///
    /// For a wait whose subject arrives only once the server's file watcher is live:
    /// the watcher is armed asynchronously, after the loader has already announced the
    /// load finished, and nothing tells a client when. A change written into that window
    /// raises no event at all, so a caller that provokes one has to be able to provoke it
    /// again rather than wait out a notification that will never come.
    ///
    /// `None` means silence and nothing else. A server that has exited is not silence —
    /// it is the answer — so it fails here rather than leaving the caller to provoke a
    /// process that is gone, as fast as the loop can go round.
    pub fn wait_for_within(
        &self,
        timeout: Duration,
        predicate: impl Fn(&Value) -> bool,
    ) -> Option<Value> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.checked_duration_since(std::time::Instant::now())?;
            match self.messages.recv_timeout(remaining) {
                Ok(message) if predicate(&message) => return Some(message),
                Ok(_) => {}
                Err(mpsc::RecvTimeoutError::Timeout) => return None,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("the server exited instead of answering")
                }
            }
        }
    }

    /// A notification the server has no work for, sent only to reach its main loop.
    ///
    /// What an editor produces constantly and what a stand needs when its subject is
    /// something the server must notice on its own rather than be told about.
    pub fn poke(&mut self) {
        self.send(json!({
            "jsonrpc": "2.0", "method": "$/setTrace", "params": {"value": "off"}
        }));
    }

    pub fn open(&mut self, path: &Path, text: &str) -> Value {
        let uri = lsp_types::Url::from_file_path(path).unwrap();
        self.send(json!({
            "jsonrpc": "2.0", "method": "textDocument/didOpen",
            "params": {"textDocument": {"uri": uri, "languageId": "bsl", "version": 1, "text": text}}
        }));
        self.wait_for(|message| {
            message["method"] == "textDocument/publishDiagnostics"
                && message["params"]["uri"] == uri.as_str()
        })
    }
}

impl Drop for Lsp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
