//! Engine output events.
//!
//! The engine never allocates for output: it pushes events into an
//! [`EventSink`] supplied by the caller. Production callers write into a
//! ring buffer or wire encoder; tests use [`VecSink`].

use crate::types::{OrderId, Price, Qty, RejectReason};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Event {
    /// Order passed validation and entered the matching path.
    Accepted {
        id: OrderId,
    },
    /// A match occurred. Price is always the resting (maker) order's price.
    Trade {
        maker: OrderId,
        taker: OrderId,
        price: Price,
        qty: Qty,
    },
    /// Order left the book with quantity unfilled (explicit cancel, IOC
    /// remainder, or unfillable market remainder).
    Canceled {
        id: OrderId,
        remaining: Qty,
    },
    /// A cancel/replace was applied; the order keeps its id.
    Replaced {
        id: OrderId,
    },
    Rejected {
        id: OrderId,
        reason: RejectReason,
    },
}

pub trait EventSink {
    fn on_event(&mut self, ev: Event);
}

/// Collects events into a `Vec`. Used by tests and the replay harness.
#[derive(Default)]
pub struct VecSink {
    pub events: Vec<Event>,
}

impl EventSink for VecSink {
    #[inline]
    fn on_event(&mut self, ev: Event) {
        self.events.push(ev);
    }
}

/// Discards all events; isolates raw matching cost in benchmarks.
pub struct NullSink;

impl EventSink for NullSink {
    #[inline]
    fn on_event(&mut self, _ev: Event) {}
}

/// Counts events without storing them. Cheap enough for the replay harness
/// while still preventing the compiler from eliding event generation.
#[derive(Default)]
pub struct CountingSink {
    pub trades: u64,
    pub traded_qty: u64,
    pub accepts: u64,
    pub cancels: u64,
    pub rejects: u64,
}

impl EventSink for CountingSink {
    #[inline]
    fn on_event(&mut self, ev: Event) {
        match ev {
            Event::Trade { qty, .. } => {
                self.trades += 1;
                self.traded_qty += qty;
            }
            Event::Accepted { .. } => self.accepts += 1,
            Event::Canceled { .. } => self.cancels += 1,
            Event::Rejected { .. } => self.rejects += 1,
            Event::Replaced { .. } => {}
        }
    }
}
