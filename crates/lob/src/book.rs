//! The order book and matching engine.
//!
//! # Design
//!
//! - **Slab storage.** All resting orders live in one contiguous
//!   `Vec<OrderNode>` with an intrusive free list. Orders are referred to by
//!   `u32` slab index, so per-order overhead is one 48-byte node and there
//!   are no per-order heap allocations after warm-up.
//! - **Intrusive FIFO queues.** Each price level is a doubly-linked list
//!   threaded through the slab (`prev`/`next` are slab indices, `NIL`
//!   terminated). Price-time priority falls out of always filling from
//!   `head` and resting at `tail`. Cancel is O(1) given the slab index.
//! - **Price levels in `BTreeMap`s.** Best bid/ask lookup and in-order level
//!   traversal are O(log n) with n = number of *distinct prices*, which is
//!   small (tens to low hundreds) even when the book holds millions of
//!   orders. The id -> slab index map uses a fast integer hasher.
//! - **No allocation on the hot path.** Matching mutates the slab and level
//!   maps in place and reports fills through the caller's [`EventSink`].

use std::collections::BTreeMap;

use crate::events::{Event, EventSink};
use crate::hash::IdMap;
use crate::types::{OrderId, Price, Qty, RejectReason, Side, Tif};

/// Sentinel slab index ("null pointer") for intrusive lists.
const NIL: u32 = u32::MAX;

#[derive(Clone, Copy, Debug)]
struct OrderNode {
    id: OrderId,
    price: Price,
    qty: Qty,
    side: Side,
    /// Previous order at this price level (toward the head / oldest).
    prev: u32,
    /// Next order at this price level (toward the tail / newest). Doubles as
    /// the free-list link while the node is unallocated.
    next: u32,
}

/// Per-price FIFO queue plus cached aggregates. `Copy` so the matching loop
/// can work on a local copy without holding a borrow of the level map.
#[derive(Clone, Copy, Debug)]
struct Level {
    head: u32,
    tail: u32,
    qty: Qty,
    orders: u32,
}

impl Level {
    const EMPTY: Level = Level {
        head: NIL,
        tail: NIL,
        qty: 0,
        orders: 0,
    };
}

pub struct OrderBook {
    nodes: Vec<OrderNode>,
    free_head: u32,
    /// Buy side; best bid is the *last* (highest) key.
    bids: BTreeMap<Price, Level>,
    /// Sell side; best ask is the *first* (lowest) key.
    asks: BTreeMap<Price, Level>,
    /// Live order id -> slab index.
    ids: IdMap<u32>,
}

impl Default for OrderBook {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderBook {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Pre-allocates the slab and id map so steady-state operation never
    /// reallocates.
    pub fn with_capacity(orders: usize) -> Self {
        let mut ids = IdMap::default();
        ids.reserve(orders);
        OrderBook {
            nodes: Vec::with_capacity(orders),
            free_head: NIL,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            ids,
        }
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /// Submits a limit order. Matches against the opposite side while the
    /// price crosses, then rests (GTC), cancels (IOC), or has already been
    /// rejected (FOK) per `tif`.
    pub fn submit_limit<S: EventSink>(
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
        if self.ids.contains_key(&id) {
            sink.on_event(Event::Rejected {
                id,
                reason: RejectReason::DuplicateId,
            });
            return;
        }
        // FOK: prove the full quantity is crossable before touching the book,
        // so an unfillable order has zero side effects.
        if tif == Tif::Fok && self.crossable_qty(side, price, qty) < qty {
            sink.on_event(Event::Rejected {
                id,
                reason: RejectReason::FokUnfillable,
            });
            return;
        }
        sink.on_event(Event::Accepted { id });
        let remaining = self.match_incoming(id, side, Some(price), qty, sink);
        if remaining > 0 {
            match tif {
                Tif::Gtc => self.rest(id, side, price, remaining),
                // FOK is unreachable here (pre-checked above); treat it like
                // IOC rather than trusting that invariant with UB.
                Tif::Ioc | Tif::Fok => sink.on_event(Event::Canceled { id, remaining }),
            }
        }
    }

    /// Submits a market order: matches at any price, never rests.
    pub fn submit_market<S: EventSink>(&mut self, id: OrderId, side: Side, qty: Qty, sink: &mut S) {
        if qty == 0 {
            sink.on_event(Event::Rejected {
                id,
                reason: RejectReason::ZeroQty,
            });
            return;
        }
        if self.ids.contains_key(&id) {
            sink.on_event(Event::Rejected {
                id,
                reason: RejectReason::DuplicateId,
            });
            return;
        }
        sink.on_event(Event::Accepted { id });
        let remaining = self.match_incoming(id, side, None, qty, sink);
        if remaining > 0 {
            sink.on_event(Event::Canceled { id, remaining });
        }
    }

    /// Cancels a resting order in O(1) (plus level-map maintenance).
    pub fn cancel<S: EventSink>(&mut self, id: OrderId, sink: &mut S) {
        let Some(idx) = self.ids.remove(&id) else {
            sink.on_event(Event::Rejected {
                id,
                reason: RejectReason::UnknownOrder,
            });
            return;
        };
        let remaining = self.nodes[idx as usize].qty;
        self.unlink(idx);
        self.free(idx);
        sink.on_event(Event::Canceled { id, remaining });
    }

    /// Cancel/replace. A pure quantity *decrease* at the same price is done
    /// in place and keeps queue priority; any other change loses priority and
    /// re-enters the matching path (so it may trade immediately).
    pub fn replace<S: EventSink>(
        &mut self,
        id: OrderId,
        new_price: Price,
        new_qty: Qty,
        sink: &mut S,
    ) {
        let Some(&idx) = self.ids.get(&id) else {
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
        let node = self.nodes[idx as usize];
        if new_price == node.price && new_qty < node.qty {
            let shrink = node.qty - new_qty;
            self.nodes[idx as usize].qty = new_qty;
            let map = match node.side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            map.get_mut(&node.price)
                .expect("level of resting order")
                .qty -= shrink;
            sink.on_event(Event::Replaced { id });
            return;
        }
        // Price move or size increase: lose priority, re-match as a fresh
        // GTC limit under the same id.
        self.ids.remove(&id);
        self.unlink(idx);
        self.free(idx);
        sink.on_event(Event::Replaced { id });
        let remaining = self.match_incoming(id, node.side, Some(new_price), new_qty, sink);
        if remaining > 0 {
            self.rest(id, node.side, new_price, remaining);
        }
    }

    // ------------------------------------------------------------------
    // Read-side accessors
    // ------------------------------------------------------------------

    pub fn best_bid(&self) -> Option<Price> {
        self.bids.last_key_value().map(|(&p, _)| p)
    }

    pub fn best_ask(&self) -> Option<Price> {
        self.asks.first_key_value().map(|(&p, _)| p)
    }

    /// Total resting quantity at a price, 0 if the level does not exist.
    pub fn level_qty(&self, side: Side, price: Price) -> Qty {
        let map = match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        };
        map.get(&price).map_or(0, |l| l.qty)
    }

    /// Remaining quantity of a live order.
    pub fn order_qty(&self, id: OrderId) -> Option<Qty> {
        self.ids.get(&id).map(|&idx| self.nodes[idx as usize].qty)
    }

    /// Number of resting orders.
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Bid levels as `(price, total_qty)`, best (highest) first.
    pub fn bid_levels(&self) -> impl Iterator<Item = (Price, Qty)> + '_ {
        self.bids.iter().rev().map(|(&p, l)| (p, l.qty))
    }

    /// Ask levels as `(price, total_qty)`, best (lowest) first.
    pub fn ask_levels(&self) -> impl Iterator<Item = (Price, Qty)> + '_ {
        self.asks.iter().map(|(&p, l)| (p, l.qty))
    }

    // ------------------------------------------------------------------
    // Matching internals
    // ------------------------------------------------------------------

    /// Matches an incoming order against the opposite side while its limit
    /// crosses (`limit == None` means market: cross anything). Returns the
    /// unfilled remainder. Fills strictly in price-time order at the maker's
    /// price.
    fn match_incoming<S: EventSink>(
        &mut self,
        taker: OrderId,
        side: Side,
        limit: Option<Price>,
        mut qty: Qty,
        sink: &mut S,
    ) -> Qty {
        while qty > 0 {
            let best = match side {
                Side::Bid => self.asks.first_key_value().map(|(&p, _)| p),
                Side::Ask => self.bids.last_key_value().map(|(&p, _)| p),
            };
            let Some(price) = best else { break };
            if let Some(lim) = limit {
                let crosses = match side {
                    Side::Bid => lim >= price,
                    Side::Ask => lim <= price,
                };
                if !crosses {
                    break;
                }
            }
            // Work on a copy of the level so no borrow of the level map is
            // held while we mutate the slab / id map / free list.
            let mut level = match side {
                Side::Bid => *self.asks.get(&price).expect("best level exists"),
                Side::Ask => *self.bids.get(&price).expect("best level exists"),
            };
            while qty > 0 && level.head != NIL {
                let head = level.head;
                let node = &mut self.nodes[head as usize];
                let fill = qty.min(node.qty);
                node.qty -= fill;
                let maker = node.id;
                let maker_done = node.qty == 0;
                let next = node.next;
                qty -= fill;
                level.qty -= fill;
                sink.on_event(Event::Trade {
                    maker,
                    taker,
                    price,
                    qty: fill,
                });
                if !maker_done {
                    break; // taker exhausted against a partially-filled head
                }
                level.head = next;
                if next == NIL {
                    level.tail = NIL;
                } else {
                    self.nodes[next as usize].prev = NIL;
                }
                level.orders -= 1;
                self.ids.remove(&maker);
                self.free(head);
            }
            let map = match side {
                Side::Bid => &mut self.asks,
                Side::Ask => &mut self.bids,
            };
            if level.head == NIL {
                map.remove(&price);
            } else {
                *map.get_mut(&price).expect("level still present") = level;
            }
        }
        qty
    }

    /// Quantity fillable against the opposite side within `limit`, capped at
    /// `want` (stops summing once satisfied). Used for the FOK pre-check.
    fn crossable_qty(&self, side: Side, limit: Price, want: Qty) -> Qty {
        let mut acc: Qty = 0;
        match side {
            Side::Bid => {
                for (&p, l) in self.asks.iter() {
                    if p > limit || acc >= want {
                        break;
                    }
                    acc += l.qty;
                }
            }
            Side::Ask => {
                for (&p, l) in self.bids.iter().rev() {
                    if p < limit || acc >= want {
                        break;
                    }
                    acc += l.qty;
                }
            }
        }
        acc
    }

    /// Rests an order at the tail of its price level (lowest time priority).
    fn rest(&mut self, id: OrderId, side: Side, price: Price, qty: Qty) {
        let idx = self.alloc(OrderNode {
            id,
            price,
            qty,
            side,
            prev: NIL,
            next: NIL,
        });
        let map = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let level = map.entry(price).or_insert(Level::EMPTY);
        let old_tail = level.tail;
        level.tail = idx;
        if level.head == NIL {
            level.head = idx;
        }
        level.qty += qty;
        level.orders += 1;
        // Map borrow ends here; now thread the node into the list.
        self.nodes[idx as usize].prev = old_tail;
        if old_tail != NIL {
            self.nodes[old_tail as usize].next = idx;
        }
        self.ids.insert(id, idx);
    }

    /// Detaches a node from its price level, removing the level if it empties.
    /// Does not free the node or touch the id map.
    fn unlink(&mut self, idx: u32) {
        let node = self.nodes[idx as usize];
        if node.prev != NIL {
            self.nodes[node.prev as usize].next = node.next;
        }
        if node.next != NIL {
            self.nodes[node.next as usize].prev = node.prev;
        }
        let map = match node.side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let level = map.get_mut(&node.price).expect("level of resting order");
        if level.head == idx {
            level.head = node.next;
        }
        if level.tail == idx {
            level.tail = node.prev;
        }
        level.qty -= node.qty;
        level.orders -= 1;
        let empty = level.head == NIL;
        if empty {
            map.remove(&node.price);
        }
    }

    // ------------------------------------------------------------------
    // Slab allocator
    // ------------------------------------------------------------------

    fn alloc(&mut self, node: OrderNode) -> u32 {
        if self.free_head != NIL {
            let idx = self.free_head;
            self.free_head = self.nodes[idx as usize].next;
            self.nodes[idx as usize] = node;
            idx
        } else {
            let idx = u32::try_from(self.nodes.len()).expect("slab capacity exceeded u32");
            self.nodes.push(node);
            idx
        }
    }

    fn free(&mut self, idx: u32) {
        self.nodes[idx as usize].next = self.free_head;
        self.free_head = idx;
    }

    // ------------------------------------------------------------------
    // Debug validation
    // ------------------------------------------------------------------

    /// Exhaustively checks structural invariants. O(book size) — used by
    /// tests and the randomized differential harness, never on the hot path.
    ///
    /// Panics with a description of the first violated invariant.
    pub fn assert_consistent(&self) {
        if let (Some(b), Some(a)) = (self.best_bid(), self.best_ask()) {
            assert!(b < a, "book is crossed: best bid {b} >= best ask {a}");
        }
        let mut seen = 0usize;
        for (side, map) in [(Side::Bid, &self.bids), (Side::Ask, &self.asks)] {
            for (&price, level) in map {
                assert_ne!(level.head, NIL, "empty level at {price:?} not removed");
                let mut qty: Qty = 0;
                let mut orders = 0u32;
                let mut idx = level.head;
                let mut prev = NIL;
                while idx != NIL {
                    let node = &self.nodes[idx as usize];
                    assert_eq!(node.side, side, "node on wrong side at {price}");
                    assert_eq!(node.price, price, "node price mismatch at {price}");
                    assert!(node.qty > 0, "resting node with zero qty at {price}");
                    assert_eq!(node.prev, prev, "broken prev link at {price}");
                    assert_eq!(
                        self.ids.get(&node.id),
                        Some(&idx),
                        "id map out of sync for order {}",
                        node.id
                    );
                    qty += node.qty;
                    orders += 1;
                    prev = idx;
                    idx = node.next;
                }
                assert_eq!(level.tail, prev, "level tail mismatch at {price}");
                assert_eq!(level.qty, qty, "level qty cache mismatch at {price}");
                assert_eq!(
                    level.orders, orders,
                    "level order count mismatch at {price}"
                );
                seen += orders as usize;
            }
        }
        assert_eq!(seen, self.ids.len(), "id map size != resting orders");
    }
}
