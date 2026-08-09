//! # lob — low-latency limit order book matching engine
//!
//! A single-threaded price-time-priority matching engine designed around the
//! constraints of real trading systems: integer prices, no allocation on the
//! hot path, O(1) cancels, and deterministic, exactly-reproducible output.
//!
//! ```
//! use lob::{OrderBook, Side, Tif, VecSink, Event};
//!
//! let mut book = OrderBook::new();
//! let mut sink = VecSink::default();
//!
//! book.submit_limit(1, Side::Ask, 1005, 10, Tif::Gtc, &mut sink); // rests
//! book.submit_limit(2, Side::Bid, 1005, 4, Tif::Gtc, &mut sink);  // crosses
//!
//! assert!(sink.events.contains(&Event::Trade { maker: 1, taker: 2, price: 1005, qty: 4 }));
//! assert_eq!(book.order_qty(1), Some(6)); // maker partially filled
//! ```

mod book;
mod events;
mod hash;
mod types;

pub use book::OrderBook;
pub use events::{CountingSink, Event, EventSink, NullSink, VecSink};
pub use types::{OrderId, Price, Qty, RejectReason, Side, Tif};
