//! `graph action=resolve` over the real MCP transport, once it answers from the
//! name dictionary rather than from graph nodes alone.
//!
//! The properties here are properties of the SERVER, not of a function: that the
//! action answers before the graph is built, that it says which sources it could
//! not consult, and that every address it hands out is accepted by the tool it
//! names. None of those survive a unit test — they hold only while the wiring
//! keeps them.

use std::path::Path;
use std::time::Duration;

use mcp_server::{serve_stream, McpProfile, McpServer, SharedState};
use rmcp::model::CallToolRequestParams;
use rmcp::service::RunningService;
use rmcp::{RoleClient, ServiceExt};
use serde_json::{Map, Value};
use tempfile::TempDir;

type Client = RunningService<RoleClient, ()>;

/// Copy the checked-in metadata fixture into a scratch dir, so derived caches
/// never land in the repo tree and each run starts cold.
fn stage_workspace() -> TempDir {
    let src = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../bsl-metadata/fixtures/designer"));
    let dst = TempDir::new().expect("scratch workspace");
    for entry in walkdir::WalkDir::new(src) {
        let entry = entry.expect("walk fixture");
        let rel = entry.path().strip_prefix(src).expect("path under fixture root");
        let target = dst.path().join(rel);
        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&target).expect("mkdir");
        } else {
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent).expect("mkdir parent");
            }
            std::fs::copy(entry.path(), &target).expect("copy fixture file");
        }
    }
    dst
}

async fn workspace_client(root: &Path, warm: bool) -> Client {
    let state = SharedState::workspace(root.to_path_buf()).expect("valid workspace project");
    if warm {
        state.warm_start();
    }
    let server = McpServer::new(McpProfile::Workspace, state);
    let (client_io, server_io) = tokio::io::duplex(4 * 1024 * 1024);
    tokio::spawn(serve_stream(server, server_io));
    ().serve(client_io).await.expect("session initialized")
}

fn args(pairs: &[(&str, Value)]) -> Map<String, Value> {
    pairs.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect()
}

async fn call(client: &Client, tool: &'static str, call_args: Map<String, Value>) -> Value {
    let request = CallToolRequestParams::new(tool).with_arguments(call_args);
    client
        .call_tool(request)
        .await
        .expect("transport ok")
        .structured_content
        .expect("a structured body")
}

async fn resolve(client: &Client, query: &str) -> Value {
    call(
        client,
        "graph",
        args(&[("action", Value::from("resolve")), ("query", Value::from(query))]),
    )
    .await
}

async fn wait_until_resident_is_ready(client: &Client) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    loop {
        let status = call(client, "diagnostics", args(&[("action", Value::from("status"))])).await;
        if status["state"] == "ready" {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the resident never became ready, last status {status}",
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_until_graph_is_ready(client: &Client) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    loop {
        let status = call(client, "graph", args(&[("action", Value::from("status"))])).await;
        if status["state"] == "ready" {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the graph never became ready, last status {status}",
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn candidates(body: &Value) -> &Vec<Value> {
    body["result"]["candidates"]
        .as_array()
        .unwrap_or_else(|| panic!("the answer carries candidates: {body}"))
}

fn provider_state(body: &Value, provider: &str) -> String {
    body["result"]["providers"]
        .as_array()
        .unwrap_or_else(|| panic!("the answer names its providers: {body}"))
        .iter()
        .find(|p| p["provider"] == provider)
        .unwrap_or_else(|| panic!("`{provider}` is named: {body}"))["state"]
        .as_str()
        .expect("a state")
        .to_owned()
}

fn reason_codes(body: &Value) -> Vec<String> {
    body["freshness"]["completeness"]["reasons"]
        .as_array()
        .map(|reasons| {
            reasons.iter().filter_map(|r| r["code"].as_str().map(str::to_owned)).collect()
        })
        .unwrap_or_default()
}

fn categories(body: &Value) -> Vec<String> {
    candidates(body).iter().filter_map(|c| c["category"].as_str().map(str::to_owned)).collect()
}

/// Which source built the row for the first candidate of `category`.
///
/// One entity arrives as one row carrying every address any provider knew for it, and the
/// row keeps the name of the source that built it: providers run in a fixed order with the
/// graph LAST, so a row a dictionary source produced still says so however far the graph's
/// own background build has got.
fn provider_of(body: &Value, category: &str) -> Option<String> {
    candidates(body)
        .iter()
        .find(|c| c["category"] == category)
        .and_then(|c| c["provider"].as_str().map(str::to_owned))
}

/// И2. The platform and the configuration's own tables answer by name — not the graph.
///
/// Which source answered IS the claim: with a built graph a metadata object is also found by
/// its `mdo/…` node, so asserting merely that a candidate came back would pass on an
/// implementation that only relabelled a graph hit. This used to be gated by asking before
/// the graph had published, which is no precondition at all but a bet on which of two
/// background builds finishes first — won on a loaded machine and lost on an idle one. The
/// row names its own source, so the claim can be asserted outright and holds whatever the
/// graph is doing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_configuration_and_the_platform_answer_by_name() {
    let ws = stage_workspace();
    let client = workspace_client(ws.path(), true).await;
    wait_until_resident_is_ready(&client).await;

    let platform = resolve(&client, "СтрНайти").await;
    assert!(
        categories(&platform).iter().any(|c| c == "platform_member"),
        "a platform member is missing: {platform}",
    );
    assert_eq!(
        provider_of(&platform, "platform_member").as_deref(),
        Some("platform"),
        "a platform member must come from the platform, which needs no index: {platform}",
    );

    let object = resolve(&client, "Справочник1").await;
    assert!(
        categories(&object).iter().any(|c| c == "metadata_object"),
        "a metadata object is missing: {object}",
    );
    assert_eq!(
        provider_of(&object, "metadata_object").as_deref(),
        Some("metadata_listing"),
        "a metadata object must come from the configuration's own tables: {object}",
    );
}

/// И3 end to end: every source is NAMED whatever it is doing, and once the graph is built
/// it answers and nothing is building any more.
///
/// What this stand does NOT pin is the graph being unconsulted when the first question is
/// asked. The graph is a second background build racing this one: waiting for the resident
/// and asserting the graph has not published yet is not a precondition, it is a bet on which
/// build finishes first. The half that needs the graph held unconsulted is
/// `tools::graph::tests::an_unconsulted_source_is_named_not_merely_missing`, where that
/// verdict is stated to the dictionary exactly as the handler states it. What is left here
/// is the half only a live server can show.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_source_is_named_before_and_after_the_graph_is_built() {
    let ws = stage_workspace();
    let client = workspace_client(ws.path(), true).await;
    wait_until_resident_is_ready(&client).await;

    let building = resolve(&client, "СтрНайти").await;
    assert!(!candidates(&building).is_empty(), "{building}");
    assert_eq!(provider_state(&building, "platform"), "answered", "{building}");
    // Read out, never asserted: an unnamed source is one a consumer cannot decide to wait
    // for, and `provider_state` fails if the graph is missing from the list at all.
    let _ = provider_state(&building, "graph");

    // Deterministic by construction: the wait makes the graph's verdict an input rather
    // than something to hope for. Without it an implementation that reported `not_ready`
    // for ever would go unnoticed, since nothing above asserts the graph's state.
    wait_until_graph_is_ready(&client).await;
    let built = resolve(&client, "СтрНайти").await;
    assert_eq!(provider_state(&built, "graph"), "answered", "{built}");
    assert!(
        !reason_codes(&built).iter().any(|c| c == "index_building"),
        "nothing is building any more: {built}",
    );
}

/// И4 end to end: once every source has answered, an empty list IS the answer.
///
/// What this stand does NOT pin is the other half — the same emptiness while a source is
/// still unconsulted, which must NOT read as complete. Asking for it before the graph
/// publishes is no precondition: it is a bet on which of two background builds finishes
/// first, won on a loaded machine and lost on an idle one. That half is stated where the
/// graph's verdict is an input rather than a hope: over the dictionary's own answer in
/// `tools::graph::tests::an_empty_list_while_a_source_is_unconsulted_is_not_a_proven_zero`,
/// and over the envelope this action assembles for itself in
/// `resolve_envelope::an_unconsulted_source_makes_the_resolve_envelope_partial`. What is left
/// here is the half only a live server can show: every source really did answer, and the
/// envelope stops hedging.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_list_says_whether_it_is_a_proven_zero() {
    let ws = stage_workspace();
    let client = workspace_client(ws.path(), true).await;
    wait_until_resident_is_ready(&client).await;

    const ABSENT: &str = "ЗаведомоНесуществующееИмяСимвола";

    wait_until_graph_is_ready(&client).await;
    let settled = resolve(&client, ABSENT).await;
    assert!(candidates(&settled).is_empty(), "{settled}");
    assert_eq!(provider_state(&settled, "graph"), "answered", "{settled}");
    assert!(
        !reason_codes(&settled).iter().any(|c| c == "index_building"),
        "with every source answered the same emptiness IS the answer: {settled}",
    );
}

/// И16 end to end: a resident miss carries the platform's answer.
///
/// `symbol_info` used to answer a resident miss from the graph alone, so with no graph it
/// returned an empty list — even for a platform member, which the graph never held in the
/// first place. The name asked here is one the PLATFORM answers by prefix: `СтрНайт`
/// resolves to nothing and is one letter short of `СтрНайти`. A name nothing could hold
/// would leave the list empty under either implementation, which is why the test that asked
/// for one proved nothing.
///
/// What this stand does NOT pin is the graph's own state, and that is deliberate. The graph
/// is a second background build racing this one: waiting for the resident and asserting the
/// graph has not published yet is not a precondition, it is a bet on which build finishes
/// first — won on a loaded machine and lost on an idle one. The half that needs the graph
/// held not-ready is
/// [`crate::tools::symbol_info::tests::a_resident_miss_is_answered_by_the_platform_while_the_graph_is_not_ready`],
/// where that verdict is stated to the dictionary exactly as the handler states it. What is
/// left here is the half only a live server can show: the whole path answers a miss with the
/// platform's candidate, whatever the graph happens to be doing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resident_miss_is_answered_without_the_graph() {
    let ws = stage_workspace();
    let client = workspace_client(ws.path(), true).await;
    wait_until_resident_is_ready(&client).await;

    let miss = call(&client, "symbol_info", args(&[("symbol", Value::from("СтрНайт"))])).await;
    assert_eq!(miss["resolved"], false, "{miss}");

    let categories: Vec<&str> = miss["candidates"]
        .as_array()
        .unwrap_or_else(|| panic!("the miss carries candidates: {miss}"))
        .iter()
        .filter_map(|c| c["category"].as_str())
        .collect();
    assert!(
        categories.contains(&"platform_member"),
        "the platform answered nothing on a miss: {miss}",
    );

    // The graph is NAMED whatever it is doing — an unnamed source is one a consumer cannot
    // decide to wait for. Its state is read out only to say what it was, never asserted.
    let graph = miss["providers"]
        .as_array()
        .unwrap_or_else(|| panic!("the miss names its providers: {miss}"))
        .iter()
        .find(|p| p["provider"] == "graph")
        .unwrap_or_else(|| panic!("`graph` is named among the sources: {miss}"))
        .clone();
    assert!(graph["state"].is_string(), "the graph's state is published whatever it is: {miss}",);
}

/// И1. Every address published is accepted back by the tool it names, and the
/// legacy `id` survives only where it still addresses a node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_published_address_is_accepted_by_the_tool_it_names() {
    let ws = stage_workspace();
    let client = workspace_client(ws.path(), true).await;
    wait_until_resident_is_ready(&client).await;
    wait_until_graph_is_ready(&client).await;

    let mut checked_symbols = 0usize;
    let mut checked_ids = 0usize;
    let mut checked_locations = 0usize;

    for needle in ["Устаревш", "Справочник1", "СтрНайти"] {
        let body = resolve(&client, needle).await;
        for candidate in candidates(&body) {
            let address = &candidate["address"];
            assert!(
                address.as_object().is_some_and(|a| !a.is_empty()),
                "a candidate with no address at all: {candidate}",
            );

            // The `id` key is the one the original defect was reported on: it
            // used to be published for candidates nothing accepted it for.
            match (candidate.get("id"), address.get("graph_id")) {
                (Some(id), Some(graph_id)) => assert_eq!(id, graph_id, "{candidate}"),
                (None, _) => {}
                (Some(id), None) => panic!("`id` published with nothing behind it: {id}"),
            }

            if let Some(symbol) = address.get("symbol").and_then(Value::as_str) {
                let card =
                    call(&client, "symbol_info", args(&[("symbol", Value::from(symbol))])).await;
                // `resolved` is serialised only on a miss, so its ABSENCE is
                // what a card looks like.
                assert!(
                    card.get("resolved").is_none(),
                    "`{symbol}` was published as a symbol and answered nothing: {card}",
                );
                checked_symbols += 1;
            }

            if let Some(graph_id) = address.get("graph_id").and_then(Value::as_str) {
                let node = call(
                    &client,
                    "graph",
                    args(&[("action", Value::from("node")), ("id", Value::from(graph_id))]),
                )
                .await;
                assert!(
                    node["result"].get("error").is_none(),
                    "`{graph_id}` was published as a node id and opened nothing: {node}",
                );
                checked_ids += 1;
            }

            if let Some(location) = address.get("location") {
                let (root_id, path) = (&location["root_id"], &location["path"]);
                let spelled = path.as_str().expect("a path").to_owned();
                let map = call(
                    &client,
                    "outline",
                    args(&[("root_id", root_id.clone()), ("path", path.clone())]),
                )
                .await;
                if spelled.to_lowercase().ends_with(".bsl") {
                    assert!(map.get("error").is_none(), "the pair maps nothing: {map}");
                } else {
                    // `outline` serves `.bsl` only. An XML file is still addressed
                    // by this pair — the refusal is by KIND, and it echoes the very
                    // path it was given, which is what proves the pair arrived.
                    assert_eq!(map["error"], "not_in_workspace", "{map}");
                    // The refusal echoes the path AS RESOLVED — the pair joined
                    // onto its root — so the given spelling is its tail.
                    let echoed = map["path"].as_str().unwrap_or_default().replace('\\', "/");
                    assert!(echoed.ends_with(&spelled), "the refusal names another file: {map}",);
                    assert!(
                        map["detail"].as_str().is_some_and(|d| d.contains("`.bsl`")),
                        "refused for some other reason than its kind: {map}",
                    );
                }
                checked_locations += 1;
            }
        }
    }

    // Guards, so the loop above cannot pass by finding nothing to check.
    assert!(checked_symbols > 0, "no `symbol` was exercised");
    assert!(checked_ids > 0, "no `graph_id` was exercised");
    assert!(checked_locations > 0, "no `location` was exercised");
}

/// The category this search was extended to find has to arrive with a place,
/// not merely with a name.
///
/// A metadata object's place is its XML, and the XML is registered under the
/// metadata root rather than among the `.bsl` sources. Looking a place up in
/// the source root alone answers `source_path_unavailable` for every one of
/// them — an answer that is honest, complete-looking and useless, and which the
/// blanket "some candidate had a location" guard next door does not notice
/// because the module candidates satisfy it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_metadata_object_arrives_with_its_xml_and_not_an_excuse() {
    let ws = stage_workspace();
    let client = workspace_client(ws.path(), true).await;
    wait_until_resident_is_ready(&client).await;
    // Waiting for the resident alone leaves the graph silent — the stand next
    // door asserts exactly that. Without this wait the count below is taken over
    // an answer the graph never contributed to, so it can neither see the split
    // nor confirm the merge.
    wait_until_graph_is_ready(&client).await;

    let body = resolve(&client, "Справочник1").await;
    let objects: Vec<_> =
        candidates(&body).iter().filter(|c| c["category"] == "metadata_object").collect();
    assert_eq!(
        objects.len(),
        1,
        "one object, one candidate — its place and its durable id belong in the same row: {body}",
    );
    let object = objects[0];
    assert!(
        object["address"]["graph_id"].is_string(),
        "the merged row kept the graph id: {object}",
    );

    let address = &object["address"];
    assert!(
        address.get("location_unavailable").is_none(),
        "the object was found and then could not be placed: {address}",
    );
    let path = address["location"]["path"].as_str().unwrap_or_else(|| panic!("{address}"));
    assert!(path.to_lowercase().ends_with(".xml"), "{address}");

    // And the pair is a real pair: `outline` refuses it by KIND, echoing the
    // path it was handed — which is what proves the address arrived.
    let map = call(
        &client,
        "outline",
        args(&[
            ("root_id", address["location"]["root_id"].clone()),
            ("path", address["location"]["path"].clone()),
        ]),
    )
    .await;
    assert_eq!(map["error"], "not_in_workspace", "{map}");
    let echoed = map["path"].as_str().unwrap_or_default().replace('\\', "/");
    assert!(echoed.ends_with(path), "the refusal names another file: {map}");
}
