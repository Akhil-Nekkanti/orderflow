//! Core scalar types and enums shared across the engine.
//!
//! Prices are integer ticks and quantities are integer lots. Using integers
//! end-to-end keeps matching exact (no float comparison) and keeps the hot
//! structs `Copy` and cache-friendly.

/// Price in ticks. Signed so spreads and deltas never underflow.
pub type Price = i64;

/// Quantity in lots.
pub type Qty = u64;

/// Client-assigned order id, unique per live order.
pub type OrderId = u64;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    #[inline]
    pub fn opposite(self) -> Side {
        match self {
            Side::Bid => Side::Ask,
            Side::Ask => Side::Bid,
        }
    }
}

/// Time-in-force for limit orders.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Tif {
    /// Rest on the book after matching.
    Gtc,
    /// Match what crosses immediately, cancel the remainder.
    Ioc,
    /// Fill the entire quantity immediately or reject without trading.
    Fok,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RejectReason {
    DuplicateId,
    ZeroQty,
    UnknownOrder,
    /// FOK order could not be filled in full.
    FokUnfillable,
}
