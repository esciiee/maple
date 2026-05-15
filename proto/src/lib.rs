use maple_types::{EngineEvent, Order, OrderbookSnapshot};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GatewayMsg {
    SubmitOrder { order: Order },
    Subscribe { connection_id: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CoreMsg {
    Event { event: EngineEvent },
    SnapshotReply { snap: OrderbookSnapshot },
}

#[derive(Debug, Error)]
pub enum ProtoError {
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

pub fn encode_to_vec<T: Serialize>(msg: &T) -> Result<Vec<u8>, ProtoError> {
    let payload = serde_json::to_vec(msg)?;
    let len = payload.len() as u32;
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}