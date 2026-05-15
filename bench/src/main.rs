use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use futures::StreamExt;
use maple_types::Fill;
use reqwest::Client;
use serde_json::{Value, json};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};

fn spawn_core(addr: &str) -> Child {
    Command::new("cargo")
        .args(["run", "--bin", "maple-core", "--quiet"])
        .env("CORE_ADDR", addr)
        .env("RUST_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn maple-core")
}

fn spawn_gateway(id: u16, core_addr: &str, http_addr: &str) -> Child {
    Command::new("cargo")
        .args(["run", "--bin", "maple-gateway", "--quiet"])
        .env("GATEWAY_ID", id.to_string())
        .env("CORE_ADDR", core_addr)
        .env("HTTP_ADDR", http_addr)
        .env("RUST_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn maple-gateway")
}

async fn wait_ready(url: &str, timeout: Duration) {
    let client = Client::new();
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if client.get(url).send().await.is_ok() {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("service at {url} did not become ready within {timeout:?}");
}

async fn submit(client: &Client, gw: &str, side: &str, price: u64, qty: u64) -> u64 {
    let resp: Value = client
        .post(format!("http://{gw}/orders"))
        .json(&json!({ "side": side, "price": price, "qty": qty }))
        .send()
        .await
        .expect("POST /orders failed")
        .json()
        .await
        .expect("response not JSON");
    resp["id"].as_u64().expect("no id in response")
}

async fn orderbook(client: &Client, gw: &str) -> Value {
    client
        .get(format!("http://{gw}/orderbook"))
        .send()
        .await
        .expect("GET /orderbook failed")
        .json()
        .await
        .expect("response not JSON")
}

// Connect to /ws, signal `ready_tx` once the handshake is done, then return
// the first Fill received within `timeout`. The caller must await `ready_rx`
// before submitting the order that triggers the fill.
async fn ws_subscribe_ready(
    gw: &str,
    ready_tx: tokio::sync::oneshot::Sender<()>,
    timeout: Duration,
) -> anyhow::Result<Fill> {
    let url = format!("ws://{gw}/ws");
    let (mut ws, _) = connect_async(&url).await.context("ws connect failed")?;
    let _ = ready_tx.send(());
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .context("timed out waiting for fill")?;
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                if let Ok(f) = serde_json::from_str::<Fill>(&t) {
                    return Ok(f);
                }
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => bail!("ws error: {e}"),
            Ok(None) => bail!("ws closed"),
            Err(_) => bail!("timed out waiting for fill"),
        }
    }
}

// ── scenarios

async fn scenario_correctness(client: &Client, gw: &str) {
    println!("\n[1] correctness");

    submit(client, gw, "Buy", 100, 5).await;

    let book = orderbook(client, gw).await;
    assert_eq!(book["bids"][0]["price"], 100, "bid not in book");
    assert_eq!(book["bids"][0]["qty"], 5, "bid qty wrong");
    assert!(book["asks"].as_array().unwrap().is_empty(), "asks should be empty");

    // Connect WS and wait for handshake before submitting the crossing sell.
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let fill_task = tokio::spawn({
        let gw = gw.to_string();
        async move { ws_subscribe_ready(&gw, ready_tx, Duration::from_secs(5)).await }
    });
    ready_rx.await.unwrap();

    submit(client, gw, "Sell", 100, 5).await;

    let fill = fill_task.await.unwrap().expect("no fill received");
    assert_eq!(fill.price, 100, "fill price wrong");
    assert_eq!(fill.qty, 5, "fill qty wrong");

    let book = orderbook(client, gw).await;
    assert!(book["bids"].as_array().unwrap().is_empty(), "bids not cleared");
    assert!(book["asks"].as_array().unwrap().is_empty(), "asks not cleared");

    println!("    fill price={} qty={} maker={} taker={}", fill.price, fill.qty, fill.maker_order_id, fill.taker_order_id);
    println!("    PASS");
}

async fn scenario_latency(client: &Client, gw: &str, runs: u32) {
    println!("\n[2] order-to-fill latency ({runs} runs)");

    let mut samples: Vec<u128> = Vec::with_capacity(runs as usize);

    for _ in 0..runs {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let fill_task = tokio::spawn({
            let gw = gw.to_string();
            async move {
                let url = format!("ws://{gw}/ws");
                let (mut ws, _) = connect_async(&url).await.unwrap();
                let _ = ready_tx.send(());
                while let Some(Ok(Message::Text(t))) = ws.next().await {
                    if serde_json::from_str::<Fill>(&t).is_ok() {
                        return;
                    }
                }
            }
        });
        ready_rx.await.unwrap();

        submit(client, gw, "Buy", 200, 1).await;

        let t0 = Instant::now();
        submit(client, gw, "Sell", 200, 1).await;
        fill_task.await.unwrap();
        samples.push(t0.elapsed().as_micros());
    }

    let mut sorted = samples.clone();
    sorted.sort_unstable();
    let mean = samples.iter().sum::<u128>() / samples.len() as u128;
    let p50 = sorted[sorted.len() / 2];
    let p99 = sorted[(sorted.len() * 99) / 100];

    println!("    mean={mean}µs  p50={p50}µs  p99={p99}µs  min={}µs  max={}µs", sorted[0], sorted.last().unwrap());
    println!("    PASS");
}

async fn scenario_multi_gateway(gw1: &str, gw2: &str) {
    println!("\n[3] multi-gateway cross-fill");
    let c = Client::new();

    // Connect both gateways' WS and wait for both handshakes before submitting.
    let (ready1_tx, ready1_rx) = tokio::sync::oneshot::channel();
    let (ready2_tx, ready2_rx) = tokio::sync::oneshot::channel();

    let fill1_task = tokio::spawn({
        let g = gw1.to_string();
        async move { ws_subscribe_ready(&g, ready1_tx, Duration::from_secs(5)).await }
    });
    let fill2_task = tokio::spawn({
        let g = gw2.to_string();
        async move { ws_subscribe_ready(&g, ready2_tx, Duration::from_secs(5)).await }
    });

    ready1_rx.await.unwrap();
    ready2_rx.await.unwrap();

    submit(&c, gw1, "Buy", 300, 3).await;
    submit(&c, gw2, "Sell", 300, 3).await;

    let f1 = fill1_task.await.unwrap().expect("gw1 did not receive fill");
    let f2 = fill2_task.await.unwrap().expect("gw2 did not receive fill");
    assert_eq!(f1.price, 300);
    assert_eq!(f1.qty, 3);
    assert_eq!(f1.maker_order_id, f2.maker_order_id, "fills refer to different maker");
    assert_eq!(f1.taker_order_id, f2.taker_order_id, "fills refer to different taker");

    println!("    gw1 fill price={} qty={}", f1.price, f1.qty);
    println!("    gw2 fill price={} qty={}", f2.price, f2.qty);
    println!("    PASS");
}

async fn scenario_snapshot_consistency(client: &Client, gw1: &str, core_addr: &str, gw2_http: &str) {
    println!("\n[4] snapshot consistency — new gateway sees live book");

    submit(client, gw1, "Buy", 400, 10).await;
    submit(client, gw1, "Buy", 401, 5).await;
    submit(client, gw1, "Sell", 500, 7).await;

    sleep(Duration::from_millis(50)).await;

    let mut gw2 = spawn_gateway(2, core_addr, gw2_http);
    wait_ready(&format!("http://{gw2_http}/orderbook"), Duration::from_secs(10)).await;

    let book = orderbook(client, gw2_http).await;
    let bids = book["bids"].as_array().unwrap();
    let asks = book["asks"].as_array().unwrap();

    assert!(!bids.is_empty(), "gw2 book has no bids");
    assert!(!asks.is_empty(), "gw2 book has no asks");
    assert_eq!(bids[0]["price"], 401, "gw2 best bid wrong");
    assert_eq!(asks[0]["price"], 500, "gw2 best ask wrong");

    println!("    gw2 best_bid={} qty={}", bids[0]["price"], bids[0]["qty"]);
    println!("    gw2 best_ask={} qty={}", asks[0]["price"], asks[0]["qty"]);
    println!("    PASS");

    gw2.kill().ok();
}

// ── main

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let core_addr = "127.0.0.1:17000";
    let gw1_http = "127.0.0.1:18080";
    let gw2_http = "127.0.0.1:18081";

    println!("building binaries...");
    let status = Command::new("cargo")
        .args(["build", "--bin", "maple-core", "--bin", "maple-gateway", "--quiet"])
        .status()
        .expect("cargo build failed");
    if !status.success() {
        bail!("build failed");
    }

    println!("starting core + gateway-1...");
    let mut core = spawn_core(core_addr);
    let mut gw1 = spawn_gateway(1, core_addr, gw1_http);
    wait_ready(&format!("http://{gw1_http}/orderbook"), Duration::from_secs(15)).await;
    println!("ready.");

    let client = Client::new();

    scenario_correctness(&client, gw1_http).await;
    scenario_latency(&client, gw1_http, 200).await;

    println!("\nstarting gateway-2 for multi-gateway test...");
    let mut gw2 = spawn_gateway(2, core_addr, gw2_http);
    wait_ready(&format!("http://{gw2_http}/orderbook"), Duration::from_secs(10)).await;

    scenario_multi_gateway(gw1_http, gw2_http).await;
    gw2.kill().ok();

    scenario_snapshot_consistency(&client, gw1_http, core_addr, gw2_http).await;

    println!("\nall scenarios passed.");

    gw1.kill().ok();
    core.kill().ok();
    Ok(())
}