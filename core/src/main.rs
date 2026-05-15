use disruptor::{BusySpin, Producer, build_multi_producer};
use maple_types::{EngineEvent, Order, OrderbookSnapshot};
use tracing::info;

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

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("maple_core=info")),
        )
        .init();

    info!("maple-core starting");

    let journal_closure = |event: &mut Event, _seq: i64, _eob: bool| {
        if event.kind == EventKind::Empty {
            return;
        }
        // journal.append(event.seq, &event.order) when kind == Order.
    };

    let matching_closure = |event: &mut Event, _seq: i64, _eob: bool| {
        if event.kind == EventKind::Empty {
            return;
        }
        // book.process / book.snapshot — produce OutMsg into event.result.
    };

    let publish_closure = |event: &mut Event, _seq: i64, _eob: bool| {
        if event.kind == EventKind::Empty {
            return;
        }
        let _result = event.result.take();
        // route OutMsg::Event via broadcast, OutMsg::Snapshot via conn_map.
    };

    let mut producer = build_multi_producer(1024, Event::default, BusySpin)
        .handle_events_with(journal_closure)
        .and_then()
        .handle_events_with(matching_closure)
        .and_then()
        .handle_events_with(publish_closure)
        .build();

    info!("disruptor pipeline built");

    producer.publish(|e| {
        e.kind = EventKind::Empty;
    });

    info!("tested with empty event");
    Ok(())
}