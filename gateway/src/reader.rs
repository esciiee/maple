use std::sync::Arc;

use futures::StreamExt;
use maple_proto::CoreMsg;
use maple_types::{EngineEvent, Fill};
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::{RwLock, broadcast};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tracing::warn;

use crate::book::LocalBook;

pub async fn core_reader_loop(
    mut read_framed: FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    book: Arc<RwLock<LocalBook>>,
    fill_tx: broadcast::Sender<Fill>,
) {
    loop {
        match read_framed.next().await {
            Some(Ok(frame)) => match serde_json::from_slice::<CoreMsg>(&frame) {
                Ok(CoreMsg::Event { event }) => match event {
                    EngineEvent::Filled { fills, delta, .. } => {
                        book.write().await.apply_delta(&delta);
                        for fill in fills {
                            let _ = fill_tx.send(fill);
                        }
                    }
                    EngineEvent::Resting { delta, .. } => {
                        book.write().await.apply_delta(&delta);
                    }
                },
                Ok(CoreMsg::SnapshotReply { snap }) => {
                    // Gap recovery path — unexpected outside bootstrap but handled.
                    warn!(snap_seq = snap.seq, "applying gap-recovery snapshot");
                    book.write().await.apply_snapshot(&snap);
                }
                Err(e) => {
                    warn!(error = %e, "failed to decode frame from core");
                }
            },
            Some(Err(e)) => {
                warn!(error = %e, "read error from core; loop exiting");
                break;
            }
            None => {
                warn!("core connection closed; loop exiting");
                break;
            }
        }
    }
}