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

    Ok(Config {
        gateway_id,
        core_addr,
        http_addr,
    })
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

    Ok(())
}