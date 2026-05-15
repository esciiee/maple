mod book;
mod reader;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use futures::StreamExt;
use maple_proto::{CoreMsg, GatewayMsg};
use maple_types::{Fill, Order, OrderbookSnapshot, Side};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::info;

use book::LocalBook;

struct Config {
    gateway_id: u16,
    core_addr: String,
    http_addr: String,
}

fn load_config() -> anyhow::Result<Config> {
    let gateway_id: u16 = std::env::var("GATEWAY_ID")
        .map_err(|_| anyhow::anyhow!("GATEWAY_ID env var is required (u16)"))?
        .parse()
        .map_err(|_| anyhow::anyhow!("GATEWAY_ID must be a valid u16"))?;

    let core_addr = std::env::var("CORE_ADDR").unwrap_or_else(|_| "127.0.0.1:7000".to_string());
    let http_addr = std::env::var("HTTP_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());

    Ok(Config { gateway_id, core_addr, http_addr })
}

#[derive(Clone)]
struct AppState {
    book: Arc<RwLock<LocalBook>>,
    core_write: Arc<Mutex<FramedWrite<tokio::net::tcp::OwnedWriteHalf, LengthDelimitedCodec>>>,
    fill_tx: Arc<broadcast::Sender<Fill>>,
    gateway_id: u16,
    order_counter: Arc<AtomicU64>,
}

fn next_order_id(gateway_id: u16, counter: &AtomicU64) -> u64 {
    let local = counter.fetch_add(1, Ordering::Relaxed);
    (gateway_id as u64) << 48 | (local & 0x0000_FFFF_FFFF_FFFF)
}

fn make_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .length_field_type::<u32>()
        .big_endian()
        .max_frame_length(maple_transport::MAX_FRAME_LEN)
        .new_codec()
}

async fn bootstrap(
    cfg: &Config,
) -> anyhow::Result<(
    FramedRead<tokio::net::tcp::OwnedReadHalf, LengthDelimitedCodec>,
    FramedWrite<tokio::net::tcp::OwnedWriteHalf, LengthDelimitedCodec>,
    OrderbookSnapshot,
)> {
    let stream = TcpStream::connect(&cfg.core_addr).await?;
    info!(core_addr = %cfg.core_addr, "connected to core");

    let (read_half, write_half) = stream.into_split();
    let mut read_framed = FramedRead::new(read_half, make_codec());
    let mut write_framed = FramedWrite::new(write_half, make_codec());

    let subscribe_msg = GatewayMsg::Subscribe { connection_id: cfg.gateway_id as u64 };
    let payload = serde_json::to_vec(&subscribe_msg)?;
    futures::SinkExt::send(&mut write_framed, Bytes::from(payload)).await?;
    info!(gateway_id = cfg.gateway_id, "sent Subscribe");

    // Discard any Event frames that arrive before the SnapshotReply.
    let snap = loop {
        match read_framed.next().await {
            Some(Ok(frame)) => match serde_json::from_slice::<CoreMsg>(&frame)? {
                CoreMsg::SnapshotReply { snap } => break snap,
                CoreMsg::Event { .. } => {}
            },
            Some(Err(e)) => return Err(e.into()),
            None => anyhow::bail!("core disconnected before sending snapshot"),
        }
    };

    info!(snap_seq = snap.seq, "received snapshot");
    Ok((read_framed, write_framed, snap))
}

#[derive(Deserialize)]
struct SubmitRequest {
    side: Side,
    price: u64,
    qty: u64,
}

async fn post_orders(
    State(state): State<AppState>,
    Json(req): Json<SubmitRequest>,
) -> impl IntoResponse {
    if req.price == 0 || req.qty == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "price and qty must be non-zero"})),
        )
            .into_response();
    }

    let id = next_order_id(state.gateway_id, &state.order_counter);
    let order = Order { id, side: req.side, price: req.price, qty: req.qty };
    let msg = GatewayMsg::SubmitOrder { order };
    let payload = match serde_json::to_vec(&msg) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(error = %e, "failed to encode order");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let mut w = state.core_write.lock().await;
    if futures::SinkExt::send(&mut *w, Bytes::from(payload)).await.is_err() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "core unavailable"})),
        )
            .into_response();
    }

    (StatusCode::OK, Json(serde_json::json!({"id": id}))).into_response()
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_connection(socket, state.fill_tx.subscribe()))
}

async fn ws_connection(mut socket: WebSocket, mut rx: broadcast::Receiver<Fill>) {
    loop {
        tokio::select! {
            result = rx.recv() => match result {
                Ok(fill) => {
                    let json = match serde_json::to_string(&fill) {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!(error = %e, "failed to serialize fill");
                            break;
                        }
                    };
                    if socket.send(Message::Text(json.into())).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(skipped = n, "ws client lagged; disconnecting");
                    let _ = socket.send(Message::Close(None)).await;
                    break;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
            msg = socket.recv() => match msg {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(Message::Ping(d))) => {
                    if socket.send(Message::Pong(d)).await.is_err() {
                        break;
                    }
                }
                _ => {}
            },
        }
    }
}

async fn get_orderbook(State(state): State<AppState>) -> impl IntoResponse {
    let b = state.book.read().await;
    Json(serde_json::json!({
        "seq":  b.seq,
        "bids": b.bids,
        "asks": b.asks,
    }))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("maple_gateway=info")),
        )
        .init();

    let cfg = load_config()?;
    info!(
        gateway_id = cfg.gateway_id,
        core_addr = %cfg.core_addr,
        http_addr = %cfg.http_addr,
        "maple-gateway starting"
    );

    let (read_framed, write_framed, snap) = bootstrap(&cfg).await?;

    let book = Arc::new(RwLock::new(LocalBook::default()));
    book.write().await.apply_snapshot(&snap);
    info!(snap_seq = snap.seq, bids = snap.bids.len(), asks = snap.asks.len(), "bootstrap complete");

    let (fill_tx, _) = broadcast::channel::<Fill>(4096);
    let fill_tx = Arc::new(fill_tx);

    tokio::spawn(reader::core_reader_loop(read_framed, book.clone(), (*fill_tx).clone()));

    let state = AppState {
        book,
        core_write: Arc::new(Mutex::new(write_framed)),
        fill_tx,
        gateway_id: cfg.gateway_id,
        order_counter: Arc::new(AtomicU64::new(1)),
    };

    let router = Router::new()
        .route("/orders", post(post_orders))
        .route("/orderbook", get(get_orderbook))
        .route("/ws", get(ws_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&cfg.http_addr).await?;
    info!(http_addr = %cfg.http_addr, "HTTP listener bound");

    axum::serve(listener, router).await?;
    Ok(())
}