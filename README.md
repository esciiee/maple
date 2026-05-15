# maple

A prediction market limit order matching engine, built for correctness and low latency. Accepts orders over HTTP, matches them with price-time priority, streams fills over WebSocket, and serves a live order book from a local replica, with no round-trip to the matching core per read.

![architecture](./public/image.png)

---

## Architecture

Two processes. One library stack.

```
                    clients
                       │
          ┌────────────┴────────────┐
          │                         │
    maple-gateway              maple-gateway        (N instances)
    POST /orders               POST /orders
    GET  /orderbook            GET  /orderbook
    GET  /ws  (fills)          GET  /ws  (fills)
          │                         │
          └────────────┬────────────┘
                       │ TCP :7000
                       │ length-prefixed JSON frames
                       ▼
                  maple-core
             disruptor ring buffer
          journal → matching → publish
```

### maple-core

Single process, single TCP listener. All order processing happens here. All processors (journalling, matching engine, events publisher) run on their own thread, without any lock contention.

**Pipeline stages (strictly sequential):**

```
gateway TCP tasks  (tokio, one per connected gateway)
    │
    │  producer.publish()  — lock-free CAS on ring buffer head
    ▼
Ring Buffer  (1024 slots, power-of-two, BusySpin)
    │
    ▼ journal processor  (OS thread)
        stamp seq, call journal.append(seq, order)
        on failure: process::abort()
    │
    ▼ matching processor  (OS thread)
        book.set_seq(seq)
        book.process(order)   →  EngineEvent
        book.snapshot(20)     →  OrderbookSnapshot   (on Subscribe)
    │
    ▼ publish processor  (OS thread)
        EngineEvent       → fan-out to all gateways via ConnMap
        SnapshotReply     → targeted send to requesting gateway only
```

Each stage is chained with `.and_then()` — one processor holds `&mut Event` at a time. The ring buffer slot carries the result forward through the pipeline inside a `Box<OutMsg>`, keeping slot size fixed regardless of how large the engine event is.

**Why disruptor?**

The disruptor pattern eliminates the cost of a traditional work queue: no `Mutex`, no condition variable, no allocation per message. Processor chaining via sequence dependency. JournalProcessor → MatchingProcessor → PublishProcessor run on pinned OS threads. Each stage waits on the previous stage's cursor with a spin-read (load(Acquire)), Also at each levels there could be multiple shards of the same processor effectively increasing overall throughput

**Snapshot sequencing:**

When a gateway connects, it sends `Subscribe`. Core publishes a `SnapshotRequest` slot into the ring buffer. The matching processor services it in sequence — never mid-match — calling `book.snapshot(20)`. The result is sent directly to the requesting gateway's channel. Because the snapshot enters and exits the pipeline in order, the gateway always receives its snapshot before any events with a higher sequence number. No gaps, no out-of-order delivery.

### maple-gateway

Multiple instances, each stateless with respect to matching. Each gateway holds a local order book replica (`LocalBook`) maintained via a delta stream from core. `GET /orderbook` is served from this replica with no network round-trip.

**Startup sequence (completes before HTTP binds):**

1. Connect TCP to core, split into owned read/write halves
2. Send `Subscribe { connection_id: GATEWAY_ID }`
3. Read frames, discard any `Event` frames, wait for `SnapshotReply`
4. Apply snapshot to `LocalBook`
5. Spawn `core_reader_loop` on the read half
6. Bind HTTP listener

**Order ID assignment:**

```
order_id = (GATEWAY_ID as u64) << 48 | local_atomic_counter
```

The gateway assigns the ID before sending to core. `POST /orders` returns `{"id": N}` immediately — no round-trip wait, no correlation. Core receives the order with the ID already set and never modifies it.

**`core_reader_loop`:**

Owns the read half exclusively. For every `EngineEvent` from core:
- `Filled { fills, delta }` → apply delta to `LocalBook`, fan each fill to all WebSocket subscribers via `broadcast::Sender<Fill>`
- `Resting { delta }` → apply delta to `LocalBook` only

**WebSocket:**

Each connected client subscribes to a `broadcast::Receiver<Fill>`. Fills are pushed as JSON text frames. A lagging client (channel full) is disconnected — they reconnect and re-subscribe rather than blocking the engine.

---

## Crate Layout

```
maple/
├── types/       maple-types      — all shared domain types (no I/O)
├── orderbook/   maple-orderbook  — BTreeMap book, matching, delta construction
├── journal/     maple-journal    — append-only WAL (stub; disk writes deferred)
├── proto/       maple-proto      — wire protocol message types
├── transport/   maple-transport  — tokio framing via LengthDelimitedCodec
├── core/        maple-core       — matching engine binary
├── gateway/     maple-gateway    — HTTP + WebSocket gateway binary
└── bench/       maple-bench      — correctness and latency test binary
```

**Dependency graph:**

```
maple-types
    ↑
    ├── maple-orderbook
    ├── maple-journal
    └── maple-proto
            ↑
        maple-transport
                ↑
        ┌───────┴────────┐
    maple-core       maple-gateway
```

---

## Q&A

### 1. How does the system handle multiple gateway instances without double-matching?


Each gateway has its own TCP connection to core, and each TCP connection is handled by a dedicated tokio task that holds a cloned `Producer` handle to the ring buffer. When two gateway tasks call `producer.publish()` simultaneously, the disruptor's internal atomic sequence counter resolves the race — one task claims the next slot, the other spins until the slot after it is free. By the time either task returns from `publish()`, its order occupies a unique, non-overlapping position in the buffer. There is no application-level lock, no deduplication check, and no coordination between gateways — the sequencer makes it structurally impossible for two orders to share a slot.

On core's side, the matching processor is the only thread that ever touches the order book. It reads slots in strict sequence order, one at a time, and writes results back into the same slot for the publish processor. Because no other thread reads or writes the book, and because the processor chain is serial (each stage waits on the previous stage's cursor before advancing), a match can never interleave with another match — not from the same gateway, not from different gateways. The book is always in a consistent state between slots.


### 2. What data structure did you use for the order book and why?

`BTreeMap<u64, VecDeque<Order>>` per side — one map for bids, one for asks.

`BTreeMap` keeps price levels sorted automatically. Best bid is `.last_key_value()` (highest price), best ask is `.first_key_value()` (lowest price) — both O(log n). Insert and remove of a price level are also O(log n). For a prediction market with a bounded number of distinct prices, this is fast enough and the implementation is straightforward.

Within each price level, `VecDeque` gives O(1) push-back (new resting order joins the back of the queue) and O(1) pop-front (oldest maker at that price is consumed first). This is the standard price-time priority structure.

The alternative would be a sorted `Vec` of `(price, queue)` pairs and binary search — faster iteration but O(n) insert/remove at arbitrary price levels. Not worth the complexity here. For higer volumes where the number of active price levels is small and known in advance, an array-backed structure with a fixed price grid would be faster, but that assumes a much more additional time.

### 3. What breaks first under real production load?

1. **The gateway → core TCP connection.** Each gateway instance has one TCP connection to core. All `POST /orders` calls from that gateway serialise through that socket — `Arc<Mutex<FramedWrite>>` means only one write is in-flight at a time. Under high concurrent order submission from a single gateway, this becomes the bottleneck before the matching engine itself is stressed.

2. **The `ConnMap` lock in the publish processor.** On every matched event, the publish processor acquires `parking_lot::Mutex<ConnMap>` and iterates all connected gateways to fan out the event. This is a sync lock on a disruptor thread — if a large number of gateways are connected, the iteration cost grows and the lock hold time increases, starving other pipeline progress.

### 4. What would you build next with another 4 hours?

**User balance and risk management in core.** Right now anyone can submit any order — there is no concept of a user or account. Adding an in-memory balance store to core (a `HashMap<user_id, u64>` checked in the journal/matching processor before the order enters the book) is the minimal change that makes the system usable as a real exchange. Fills would debit and credit balances atomically inside the pipeline.

**Persistence layer.** The journal is currently a no-op stub. Completing it — JSON line per order, flushed to disk, replayed on startup — gives crash recovery for the order book state. Balances and settled positions would go to a proper store (Postgres for durability, Redis for low-latency reads of current balances).

**Market data gateway.** `GET /orderbook` is currently served from each gateway's local replica, which means each gateway does its own delta application and serves its own snapshot. A dedicated read-only market data gateway — subscribing to the same delta stream but serving many more clients, with a Redis-backed snapshot for instant bootstrap — would offload this from the order-entry gateways and scale the read path independently from the write path.

**Transport.** Replace the TCP + JSON framing with Aeron for the core ↔ gateway link. Aeron is a message transport designed for exactly this: reliable, ordered, low-latency IPC and UDP multicast, with backpressure built in. It would eliminate the per-connection TCP serialisation bottleneck and allow the matching engine to broadcast fills to all gateways via multicast rather than N unicast writes.

**JSON serialisation / deserialisation.** Every order crosses the wire as JSON and is parsed on both ends. Under sustained load this is measurable CPU. Replacing with a compact binary encoding (FlatBuffers, Cap'n Proto, SBE) would reduce this cost but the implementation would take additional time.


---

## Running

**Core** (listens on `0.0.0.0:7000` by default):

```sh
cargo run --bin maple-core
```

**Gateway** (listens on `0.0.0.0:8080` by default):

```sh
GATEWAY_ID=1 cargo run --bin maple-gateway
```

**Two gateways simultaneously:**

```sh
GATEWAY_ID=1 cargo run --bin maple-gateway &
GATEWAY_ID=2 HTTP_ADDR=0.0.0.0:8081 cargo run --bin maple-gateway
```

Environment variables:

| Variable     | Default          | Description                              |
|--------------|------------------|------------------------------------------|
| `CORE_ADDR`  | `127.0.0.1:7000` | Address core listens / gateway dials     |
| `HTTP_ADDR`  | `0.0.0.0:8080`   | Address gateway HTTP server binds        |
| `GATEWAY_ID` | *(required)*     | Unique `u16` per gateway instance        |

---

## API

### `POST /orders`

```sh
curl -s -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"side":"Buy","price":100,"qty":5}'
# {"id":281474976710657}
```

Returns immediately with the assigned order ID. No wait for a match.

### `GET /orderbook`

Served from the gateway's local replica — no round-trip to core.

```sh
curl -s http://localhost:8080/orderbook | jq
# {
#   "seq": 3,
#   "bids": [{"price":100,"qty":5}],
#   "asks": []
# }
```

### `GET /ws`

WebSocket stream of fill events. One JSON message per fill.

```sh
websocat ws://localhost:8080/ws
```

---

## Testing & latency

Runs four end-to-end scenarios against real spawned binaries:

1. **Correctness** — rest a bid, assert book state, cross with ask, assert fill and empty book
2. **Latency** — 200 runs of taker-POST → fill-received, reports mean / p50 / p99
3. **Multi-gateway** — two gateways submit on opposite sides, both receive the same fill
4. **Snapshot consistency** — build book state, spin up a fresh gateway, assert it bootstraps with the correct top-of-book

```sh
cargo run --bin maple-bench
```