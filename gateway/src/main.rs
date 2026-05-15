mod book;
mod reader;

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use maple_proto::{CoreMsg, GatewayMsg};
use maple_types::{Fill, OrderbookSnapshot};
use tokio::net::TcpStream;
use tokio::sync::{RwLock, broadcast};
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
    // See DESIGN.md "KNOWN ORDERING HAZARD" — the select! in core's write_loop
    // can deliver a broadcast event before the targeted snapshot reply.
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

    let (read_framed, _write_framed, snap) = bootstrap(&cfg).await?;

    let book = Arc::new(RwLock::new(LocalBook::default()));
    book.write().await.apply_snapshot(&snap);
    info!(
        snap_seq = snap.seq,
        bids = snap.bids.len(),
        asks = snap.asks.len(),
        "bootstrap complete"
    );

    let (fill_tx, _fill_rx) = broadcast::channel::<Fill>(4096);
    tokio::spawn(reader::core_reader_loop(read_framed, book.clone(), fill_tx.clone()));

    // post orders, get orderbook, ws feeds for fills are pending.
    tokio::signal::ctrl_c().await?;
    info!("shutting down");
    Ok(())
}