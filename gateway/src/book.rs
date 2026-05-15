use maple_types::{BookDelta, Level, LevelUpdate, OrderbookSnapshot, Side};
use tracing::warn;

pub struct LocalBook {
    pub seq: u64,
    pub bids: Vec<Level>, // descending price
    pub asks: Vec<Level>, // ascending price
}

impl Default for LocalBook {
    fn default() -> Self {
        Self { seq: 0, bids: Vec::new(), asks: Vec::new() }
    }
}

impl LocalBook {
    pub fn apply_snapshot(&mut self, snap: &OrderbookSnapshot) {
        self.seq = snap.seq;
        self.bids = snap.bids.clone();
        self.asks = snap.asks.clone();
    }

    // Returns false on sequence gap, caller logs and continues.
    pub fn apply_delta(&mut self, delta: &BookDelta) -> bool {
        if delta.seq != self.seq + 1 {
            warn!(expected = self.seq + 1, got = delta.seq, "seq gap in delta");
            return false;
        }
        self.seq = delta.seq;
        for update in &delta.changes {
            match update.side {
                Side::Buy => apply_level_update(&mut self.bids, update, true),
                Side::Sell => apply_level_update(&mut self.asks, update, false),
            }
        }
        true
    }
}

fn apply_level_update(levels: &mut Vec<Level>, update: &LevelUpdate, bids: bool) {
    let pos = levels.iter().position(|l| l.price == update.price);
    match (pos, update.qty) {
        (Some(i), 0) => {
            levels.remove(i);
        }
        (Some(i), qty) => {
            levels[i].qty = qty;
        }
        (None, 0) => {
            // no-op
        }
        (None, qty) => {
            let level = Level { price: update.price, qty };
            let insert_at = if bids {
                // bids: descending — insert before first price less than ours
                levels.iter().position(|l| l.price < update.price)
            } else {
                // asks: ascending — insert before first price greater than ours
                levels.iter().position(|l| l.price > update.price)
            };
            match insert_at {
                Some(i) => levels.insert(i, level),
                None => levels.push(level),
            }
            levels.truncate(20);
        }
    }
}