use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Side {
    #[default]
    Buy,
    Sell,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Order {
    pub id: u64,
    pub side: Side,
    pub price: u64,
    pub qty: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub maker_order_id: u64,
    pub taker_order_id: u64,
    pub price: u64,
    pub qty: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Level {
    pub price: u64,
    pub qty: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LevelUpdate {
    pub side: Side,
    pub price: u64,
    pub qty: u64,
}

// Book Delta is the change in orderbook after an event. 
// The data contained is sufficient to update the orderbook to the new state after the event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookDelta {
    pub seq: u64,
    pub changes: Vec<LevelUpdate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookSnapshot {
    pub seq: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EngineEvent {
    Filled {
        taker_id: u64,
        fills: Vec<Fill>,
        delta: BookDelta,
    },
    Resting {
        order_id: u64,
        delta: BookDelta,
    },
}
