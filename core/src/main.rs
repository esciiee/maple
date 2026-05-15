use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use disruptor::{BusySpin, MultiProducer, Producer, SingleConsumerBarrier, build_multi_producer};
use futures::{SinkExt, StreamExt, stream::SplitSink};
use maple_journal::Journal;
use maple_orderbook::OrderBook;
use maple_proto::{CoreMsg, GatewayMsg};
use maple_transport::FramedStream;
use maple_types::{EngineEvent, Order, OrderbookSnapshot};
use parking_lot::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};

#[derive(Default)]
struct Event {
    kind: EventKind,
    conn_id: u64,
    seq: u64,
    order: Order,
    result: Option<Box<OutMsg>>,
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
enum EventKind {
    #[default]
    Empty,
    Order,
    SnapshotRequest,
}

enum OutMsg {
    Event(EngineEvent),
    Snapshot(OrderbookSnapshot),
}

type ConnMap = Arc<Mutex<HashMap<u64, mpsc::Sender<CoreMsg>>>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("maple_core=info")),
        )
        .init();

    info!("maple-core starting");

    let mut journal = Journal::open("maple-core.journal")?;
    let mut next_seq: u64 = 0;

    // 1. Journalling
    let journal_closure = move |event: &mut Event, _seq: i64, _eob: bool| {
        if event.kind == EventKind::Empty {
            return;
        }
        event.seq = next_seq;
        next_seq += 1;
        if event.kind == EventKind::Order {
            if let Err(e) = journal.append(event.seq, &event.order) {
                error!(error = %e, "journal append failed; aborting");
                std::process::abort();
            }
        }
    };

    let mut book = OrderBook::new();

    // 2. Matching and snapshot generation
    let matching_closure = move |event: &mut Event, _seq: i64, _eob: bool| match event.kind {
        EventKind::Order => {
            book.set_seq(event.seq);
            let order = std::mem::take(&mut event.order);
            let ee = book.process(order);
            event.result = Some(Box::new(OutMsg::Event(ee)));
        }
        EventKind::SnapshotRequest => {
            book.set_seq(event.seq);
            let snap = book.snapshot(20);
            event.result = Some(Box::new(OutMsg::Snapshot(snap)));
        }
        EventKind::Empty => {}
    };

    let (bcast_tx, _bcast_rx_template) = broadcast::channel::<CoreMsg>(4096);
    let conn_map: ConnMap = Arc::new(Mutex::new(HashMap::new()));

    let publish_bcast_tx = bcast_tx.clone();
    let publish_conn_map = conn_map.clone();

    // 3. Publishing results back to Gateway
    let publish_closure = move |event: &mut Event, _seq: i64, _eob: bool| {
        let Some(out) = event.result.take() else {
            return;
        };
        match *out {
            OutMsg::Event(ee) => {
                let msg = CoreMsg::Event { event: ee };
                let _ = publish_bcast_tx.send(msg);
            }
            OutMsg::Snapshot(snap) => {
                let msg = CoreMsg::SnapshotReply { snap };
                let sender = publish_conn_map.lock().get(&event.conn_id).cloned();
                if let Some(tx) = sender {
                    if let Err(e) = tx.try_send(msg) {
                        warn!(conn_id = event.conn_id, error = %e, "snapshot drop");
                    }
                } else {
                    warn!(conn_id = event.conn_id, "snapshot reply: unknown conn_id");
                }
            }
        }
    };

    let producer = build_multi_producer(1024, Event::default, BusySpin)
        .handle_events_with(journal_closure)
        .and_then()
        .handle_events_with(matching_closure)
        .and_then()
        .handle_events_with(publish_closure)
        .build();

    info!("disruptor pipeline built");

    let addr = std::env::var("CORE_ADDR").unwrap_or_else(|_| "0.0.0.0:7000".to_string());
    let listener = TcpListener::bind(&addr).await?;
    info!(%addr, "listening");

    let conn_counter = Arc::new(AtomicU64::new(1));

    loop {
        let (stream, peer) = listener.accept().await?;
        let conn_id = conn_counter.fetch_add(1, Ordering::Relaxed);
        info!(conn_id, %peer, "accepted");
        let producer = producer.clone();
        let bcast_tx = bcast_tx.clone();
        let conn_map = conn_map.clone();
        tokio::spawn(async move {
            handle_connection(stream, conn_id, producer, bcast_tx, conn_map).await;
            info!(conn_id, "connection closed");
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    conn_id: u64,
    mut producer: MultiProducer<Event, SingleConsumerBarrier>,
    bcast_tx: broadcast::Sender<CoreMsg>,
    conn_map: ConnMap,
) {
    let framed = maple_transport::frame(stream);
    let (sink, mut read_stream) = framed.split();

    let (targeted_tx, targeted_rx) = mpsc::channel::<CoreMsg>(64);
    conn_map.lock().insert(conn_id, targeted_tx);

    let bcast_rx = bcast_tx.subscribe();
    let write_task = tokio::spawn(write_loop(sink, bcast_rx, targeted_rx));

    loop {
        match read_stream.next().await {
            Some(Ok(frame)) => match serde_json::from_slice::<GatewayMsg>(&frame) {
                Ok(GatewayMsg::SubmitOrder { order }) => {
                    producer.publish(|e| {
                        e.kind = EventKind::Order;
                        e.conn_id = conn_id;
                        e.order = order.clone();
                    });
                }
                Ok(GatewayMsg::Subscribe { .. }) => {
                    producer.publish(|e| {
                        e.kind = EventKind::SnapshotRequest;
                        e.conn_id = conn_id;
                    });
                }
                Err(e) => {
                    warn!(conn_id, error = %e, "decode failed; closing");
                    break;
                }
            },
            Some(Err(e)) => {
                warn!(conn_id, error = %e, "read error; closing");
                break;
            }
            None => break,
        }
    }

    conn_map.lock().remove(&conn_id);
    write_task.abort();
}

async fn write_loop(
    mut sink: SplitSink<FramedStream, bytes::Bytes>,
    mut bcast_rx: broadcast::Receiver<CoreMsg>,
    mut targeted_rx: mpsc::Receiver<CoreMsg>,
) {
    loop {
        let msg = tokio::select! {
            r = bcast_rx.recv() => match r {
                Ok(m) => m,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(skipped = n, "broadcast lagged");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
            r = targeted_rx.recv() => match r {
                Some(m) => m,
                None => break,
            },
        };
        let payload = match serde_json::to_vec(&msg) {
            Ok(p) => p,
            Err(e) => {
                error!(error = %e, "encode failed");
                break;
            }
        };
        if let Err(e) = sink.send(bytes::Bytes::from(payload)).await {
            warn!(error = %e, "write failed; closing");
            break;
        }
    }
}