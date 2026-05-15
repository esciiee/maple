use futures::StreamExt;
use maple_proto::{CoreMsg, GatewayMsg};
use maple_transport::FramedStream;
use maple_types::OrderbookSnapshot;
use tokio::net::TcpStream;
use tracing::info;

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

async fn bootstrap(cfg: &Config) -> anyhow::Result<(FramedStream, OrderbookSnapshot)> {
    let stream = TcpStream::connect(&cfg.core_addr).await?;
    info!(core_addr = %cfg.core_addr, "connected to core");

    let mut framed = maple_transport::frame(stream);

    let subscribe_msg = GatewayMsg::Subscribe { connection_id: cfg.gateway_id as u64 };
    let payload = serde_json::to_vec(&subscribe_msg)?;
    futures::SinkExt::send(&mut framed, bytes::Bytes::from(payload)).await?;
    info!(gateway_id = cfg.gateway_id, "sent Subscribe");

    // the event reply may be fetched before the snapshot reply even though the publish processor enqueues them in order.
    // look for snapshot reply in the stream and buffer any events until it arrives, then return the snapshot and buffered events together.
    let snap = loop {
        match framed.next().await {
            Some(Ok(frame)) => match serde_json::from_slice::<CoreMsg>(&frame)? {
                CoreMsg::SnapshotReply { snap } => break snap,
                CoreMsg::Event { .. } => {
                    // discard — delta is for a seq before our snapshot baseline
                }
            },
            Some(Err(e)) => return Err(e.into()),
            None => anyhow::bail!("core disconnected before sending snapshot"),
        }
    };

    info!(snap_seq = snap.seq, "received snapshot");
    Ok((framed, snap))
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

    let (_framed, snap) = bootstrap(&cfg).await?;
    info!(
        snap_seq = snap.seq,
        bids = snap.bids.len(),
        asks = snap.asks.len(),
        "bootstrap complete"
    );

    Ok(())
}