use std::collections::{BTreeMap, VecDeque};
use maple_types::{BookDelta, EngineEvent, Fill, Level, LevelUpdate, Order, OrderbookSnapshot, Side};

pub struct OrderBook {
    bids: BTreeMap<u64, VecDeque<Order>>,
    asks: BTreeMap<u64, VecDeque<Order>>,
    seq: u64,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            seq: 0,
        }
    }

    pub fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }

    pub fn snapshot(&self, depth: usize) -> OrderbookSnapshot {
        let bids = self
            .bids
            .iter()
            .rev()
            .take(depth)
            .map(|(&price, orders)| Level {
                price,
                qty: orders.iter().map(|o| o.qty).sum(),
            })
            .collect();

        let asks = self
            .asks
            .iter()
            .take(depth)
            .map(|(&price, orders)| Level {
                price,
                qty: orders.iter().map(|o| o.qty).sum(),
            })
            .collect();

        OrderbookSnapshot {
            seq: self.seq,
            bids,
            asks,
        }
    }

    pub fn process(&mut self, order: Order) -> EngineEvent {
        let taker_id = order.id;
        let taker_side = order.side;
        let taker_price = order.price;
        let mut remaining = order.qty;

        let mut fills: Vec<Fill> = Vec::new();
        // required for updates to local orderbook state in gateway.
        let mut changes: Vec<LevelUpdate> = Vec::new();

        let maker_side = match taker_side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        loop {
            if remaining == 0 {
                break;
            }

            let best_maker_price = match taker_side {
                Side::Buy => self.asks.keys().next().copied(),
                Side::Sell => self.bids.keys().next_back().copied(),
            };

            let Some(maker_price) = best_maker_price else {
                break;
            };

            let within_limit = match taker_side {
                Side::Buy => maker_price <= taker_price,
                Side::Sell => maker_price >= taker_price,
            };

            if !within_limit {
                break;
            }

            let queue = match taker_side {
                Side::Buy => self.asks.get_mut(&maker_price).unwrap(),
                Side::Sell => self.bids.get_mut(&maker_price).unwrap(),
            };

            while remaining > 0 {
                let Some(maker) = queue.front_mut() else {
                    break;
                };
                let trade_qty = remaining.min(maker.qty);
                fills.push(Fill {
                    maker_order_id: maker.id,
                    taker_order_id: taker_id,
                    price: maker_price,
                    qty: trade_qty,
                });
                maker.qty -= trade_qty;
                remaining -= trade_qty;
                if maker.qty == 0 {
                    queue.pop_front();
                }
            }

            let level_qty: u64 = queue.iter().map(|o| o.qty).sum();
            if level_qty == 0 {
                match taker_side {
                    Side::Buy => {
                        self.asks.remove(&maker_price);
                    }
                    Side::Sell => {
                        self.bids.remove(&maker_price);
                    }
                }
            }
            changes.push(LevelUpdate {
                side: maker_side,
                price: maker_price,
                qty: level_qty,
            });
        }

        if remaining > 0 {
            let resting_order = Order {
                id: taker_id,
                side: taker_side,
                price: taker_price,
                qty: remaining,
            };
            let book = match taker_side {
                Side::Buy => &mut self.bids,
                Side::Sell => &mut self.asks,
            };
            let queue = book.entry(taker_price).or_default();
            queue.push_back(resting_order);
            let level_qty: u64 = queue.iter().map(|o| o.qty).sum();
            changes.push(LevelUpdate {
                side: taker_side,
                price: taker_price,
                qty: level_qty,
            });
        }

        let delta = BookDelta {
            seq: self.seq,
            changes,
        };

        if fills.is_empty() {
            EngineEvent::Resting {
                order_id: taker_id,
                delta,
            }
        } else {
            EngineEvent::Filled {
                taker_id,
                fills,
                delta,
            }
        }
    }
}

impl Default for OrderBook {
    fn default() -> Self {
        Self::new()
    }
}
