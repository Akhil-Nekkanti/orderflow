//! Differential test: the optimized engine vs. a deliberately naive
//! reference implementation.
//!
//! The reference book is a flat `Vec` of orders with O(n) scans — slow but
//! obviously correct. Both books consume an identical stream of randomized
//! operations (seeded SplitMix64, fully deterministic); every operation's
//! event stream must match exactly, and the engine's structural invariants
//! are re-checked periodically.

use lob::{Event, EventSink, OrderBook, OrderId, Price, Qty, RejectReason, Side, Tif, VecSink};

// ---------------------------------------------------------------------------
// Reference model
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct RefOrder {
    id: OrderId,
    side: Side,
    price: Price,
    qty: Qty,
    seq: u64, // arrival order; lower = earlier = higher time priority
}

#[derive(Default)]
struct RefBook {
    orders: Vec<RefOrder>,
    seq: u64,
}

impl RefBook {
    fn find(&self, id: OrderId) -> Option<usize> {
        self.orders.iter().position(|o| o.id == id)
    }

    /// Index of the best opposite-side order for a taker: best price first,
    /// then earliest arrival.
    fn best_opposite(&self, taker_side: Side, limit: Option<Price>) -> Option<usize> {
        let maker_side = taker_side.opposite();
        self.orders
            .iter()
            .enumerate()
            .filter(|(_, o)| o.side == maker_side)
            .filter(|(_, o)| match (limit, taker_side) {
                (None, _) => true,
                (Some(lim), Side::Bid) => o.price <= lim,
                (Some(lim), Side::Ask) => o.price >= lim,
            })
            .min_by_key(|(_, o)| {
                let price_key = match taker_side {
                    Side::Bid => o.price,  // buy cheapest first
                    Side::Ask => -o.price, // sell to highest first
                };
                (price_key, o.seq)
            })
            .map(|(i, _)| i)
    }

    fn crossable_qty(&self, taker_side: Side, limit: Price) -> Qty {
        let maker_side = taker_side.opposite();
        self.orders
            .iter()
            .filter(|o| o.side == maker_side)
            .filter(|o| match taker_side {
                Side::Bid => o.price <= limit,
                Side::Ask => o.price >= limit,
            })
            .map(|o| o.qty)
            .sum()
    }

    fn do_match<S: EventSink>(
        &mut self,
        taker: OrderId,
        side: Side,
        limit: Option<Price>,
        mut qty: Qty,
        sink: &mut S,
    ) -> Qty {
        while qty > 0 {
            let Some(i) = self.best_opposite(side, limit) else {
                break;
            };
            let fill = qty.min(self.orders[i].qty);
            let maker = self.orders[i].id;
            let price = self.orders[i].price;
            self.orders[i].qty -= fill;
            qty -= fill;
            sink.on_event(Event::Trade {
                maker,
                taker,
                price,
                qty: fill,
            });
            if self.orders[i].qty == 0 {
                self.orders.remove(i);
            }
        }
        qty
    }

    fn submit_limit<S: EventSink>(
        &mut self,
        id: OrderId,
        side: Side,
        price: Price,
        qty: Qty,
        tif: Tif,
        sink: &mut S,
    ) {
        if qty == 0 {
            sink.on_event(Event::Rejected {
                id,
                reason: RejectReason::ZeroQty,
            });
            return;
        }
        if self.find(id).is_some() {
            sink.on_event(Event::Rejected {
                id,
                reason: RejectReason::DuplicateId,
            });
            return;
        }
        if tif == Tif::Fok && self.crossable_qty(side, price) < qty {
            sink.on_event(Event::Rejected {
                id,
                reason: RejectReason::FokUnfillable,
            });
            return;
        }
        sink.on_event(Event::Accepted { id });
        let remaining = self.do_match(id, side, Some(price), qty, sink);
        if remaining > 0 {
            match tif {
                Tif::Gtc => {
                    let seq = self.seq;
                    self.seq += 1;
                    self.orders.push(RefOrder {
                        id,
                        side,
                        price,
                        qty: remaining,
                        seq,
                    });
                }
                Tif::Ioc | Tif::Fok => sink.on_event(Event::Canceled { id, remaining }),
            }
        }
    }

    fn submit_market<S: EventSink>(&mut self, id: OrderId, side: Side, qty: Qty, sink: &mut S) {
        if qty == 0 {
            sink.on_event(Event::Rejected {
                id,
                reason: RejectReason::ZeroQty,
            });
            return;
        }
        if self.find(id).is_some() {
            sink.on_event(Event::Rejected {
                id,
                reason: RejectReason::DuplicateId,
            });
            return;
        }
        sink.on_event(Event::Accepted { id });
        let remaining = self.do_match(id, side, None, qty, sink);
        if remaining > 0 {
            sink.on_event(Event::Canceled { id, remaining });
        }
    }

    fn cancel<S: EventSink>(&mut self, id: OrderId, sink: &mut S) {
        match self.find(id) {
            Some(i) => {
                let remaining = self.orders[i].qty;
                self.orders.remove(i);
                sink.on_event(Event::Canceled { id, remaining });
            }
            None => sink.on_event(Event::Rejected {
                id,
                reason: RejectReason::UnknownOrder,
            }),
        }
    }

    fn replace<S: EventSink>(&mut self, id: OrderId, new_price: Price, new_qty: Qty, sink: &mut S) {
        let Some(i) = self.find(id) else {
            sink.on_event(Event::Rejected {
                id,
                reason: RejectReason::UnknownOrder,
            });
            return;
        };
        if new_qty == 0 {
            sink.on_event(Event::Rejected {
                id,
                reason: RejectReason::ZeroQty,
            });
            return;
        }
        let old = self.orders[i];
        if new_price == old.price && new_qty < old.qty {
            self.orders[i].qty = new_qty;
            sink.on_event(Event::Replaced { id });
            return;
        }
        self.orders.remove(i);
        sink.on_event(Event::Replaced { id });
        let remaining = self.do_match(id, old.side, Some(new_price), new_qty, sink);
        if remaining > 0 {
            let seq = self.seq;
            self.seq += 1;
            self.orders.push(RefOrder {
                id,
                side: old.side,
                price: new_price,
                qty: remaining,
                seq,
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Deterministic RNG (SplitMix64)
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// ---------------------------------------------------------------------------
// The differential run
// ---------------------------------------------------------------------------

fn run_differential(seed: u64, ops: usize) {
    let mut rng = Rng(seed);
    let mut engine = OrderBook::new();
    let mut reference = RefBook::default();
    let mut live: Vec<u64> = Vec::new();
    let mut next_id: u64 = 1;

    for step in 0..ops {
        let mut engine_events = VecSink::default();
        let mut ref_events = VecSink::default();
        let mid: i64 = 10_000;
        let roll = rng.below(100);

        if roll < 60 || live.is_empty() {
            let side = if rng.below(2) == 0 {
                Side::Bid
            } else {
                Side::Ask
            };
            let offset = rng.below(12) as i64;
            let aggressive = rng.below(4) == 0;
            let price = match (side, aggressive) {
                (Side::Bid, false) => mid - offset,
                (Side::Bid, true) => mid + offset,
                (Side::Ask, false) => mid + offset,
                (Side::Ask, true) => mid - offset,
            };
            let tif = match rng.below(10) {
                0..=6 => Tif::Gtc,
                7..=8 => Tif::Ioc,
                _ => Tif::Fok,
            };
            let qty = 1 + rng.below(20);
            let id = next_id;
            next_id += 1;
            if tif == Tif::Gtc {
                live.push(id);
            }
            engine.submit_limit(id, side, price, qty, tif, &mut engine_events);
            reference.submit_limit(id, side, price, qty, tif, &mut ref_events);
        } else if roll < 75 {
            let at = rng.below(live.len() as u64) as usize;
            let id = live.swap_remove(at);
            engine.cancel(id, &mut engine_events);
            reference.cancel(id, &mut ref_events);
        } else if roll < 90 {
            let at = rng.below(live.len() as u64) as usize;
            let id = live[at];
            let price = mid + rng.below(12) as i64 - 6;
            let qty = 1 + rng.below(20);
            engine.replace(id, price, qty, &mut engine_events);
            reference.replace(id, price, qty, &mut ref_events);
        } else {
            let id = next_id;
            next_id += 1;
            let side = if rng.below(2) == 0 {
                Side::Bid
            } else {
                Side::Ask
            };
            let qty = 1 + rng.below(40);
            engine.submit_market(id, side, qty, &mut engine_events);
            reference.submit_market(id, side, qty, &mut ref_events);
        }

        assert_eq!(
            engine_events.events, ref_events.events,
            "event divergence at step {step} (seed {seed})"
        );

        if step % 512 == 0 {
            check_books_equal(&engine, &reference, step, seed);
            engine.assert_consistent();
        }
    }
    check_books_equal(&engine, &reference, ops, seed);
    engine.assert_consistent();
}

fn check_books_equal(engine: &OrderBook, reference: &RefBook, step: usize, seed: u64) {
    assert_eq!(
        engine.len(),
        reference.orders.len(),
        "resting count divergence at step {step} (seed {seed})"
    );
    for o in &reference.orders {
        assert_eq!(
            engine.order_qty(o.id),
            Some(o.qty),
            "qty divergence for order {} at step {step} (seed {seed})",
            o.id
        );
    }
    let best_ref_bid = reference
        .orders
        .iter()
        .filter(|o| o.side == Side::Bid)
        .map(|o| o.price)
        .max();
    let best_ref_ask = reference
        .orders
        .iter()
        .filter(|o| o.side == Side::Ask)
        .map(|o| o.price)
        .min();
    assert_eq!(
        engine.best_bid(),
        best_ref_bid,
        "best bid divergence at step {step}"
    );
    assert_eq!(
        engine.best_ask(),
        best_ref_ask,
        "best ask divergence at step {step}"
    );
}

#[test]
fn differential_seed_1() {
    run_differential(1, 20_000);
}

#[test]
fn differential_seed_2() {
    run_differential(0xDEAD_BEEF, 20_000);
}

#[test]
fn differential_seed_3() {
    run_differential(20_260_809, 20_000);
}
