//! Matching-semantics tests: price-time priority, TIF handling, maker
//! pricing, cancel/replace rules, and exact event sequences.

use lob::{Event, OrderBook, RejectReason, Side, Tif, VecSink};

fn book() -> (OrderBook, VecSink) {
    (OrderBook::new(), VecSink::default())
}

#[test]
fn passive_orders_rest_without_trading() {
    let (mut b, mut s) = book();
    b.submit_limit(1, Side::Bid, 999, 10, Tif::Gtc, &mut s);
    b.submit_limit(2, Side::Ask, 1001, 10, Tif::Gtc, &mut s);
    assert_eq!(
        s.events,
        vec![Event::Accepted { id: 1 }, Event::Accepted { id: 2 }]
    );
    assert_eq!(b.best_bid(), Some(999));
    assert_eq!(b.best_ask(), Some(1001));
    assert_eq!(b.len(), 2);
    b.assert_consistent();
}

#[test]
fn crossing_order_trades_at_maker_price() {
    let (mut b, mut s) = book();
    b.submit_limit(1, Side::Ask, 1000, 10, Tif::Gtc, &mut s);
    s.events.clear();
    // Taker willing to pay 1005 still trades at the resting 1000.
    b.submit_limit(2, Side::Bid, 1005, 10, Tif::Gtc, &mut s);
    assert_eq!(
        s.events,
        vec![
            Event::Accepted { id: 2 },
            Event::Trade {
                maker: 1,
                taker: 2,
                price: 1000,
                qty: 10
            },
        ]
    );
    assert!(b.is_empty());
    b.assert_consistent();
}

#[test]
fn price_priority_beats_time_priority() {
    let (mut b, mut s) = book();
    b.submit_limit(1, Side::Ask, 1002, 5, Tif::Gtc, &mut s); // older, worse price
    b.submit_limit(2, Side::Ask, 1001, 5, Tif::Gtc, &mut s); // newer, better price
    s.events.clear();
    b.submit_limit(3, Side::Bid, 1002, 10, Tif::Gtc, &mut s);
    assert_eq!(
        s.events,
        vec![
            Event::Accepted { id: 3 },
            Event::Trade {
                maker: 2,
                taker: 3,
                price: 1001,
                qty: 5
            },
            Event::Trade {
                maker: 1,
                taker: 3,
                price: 1002,
                qty: 5
            },
        ]
    );
    b.assert_consistent();
}

#[test]
fn time_priority_within_level_is_fifo() {
    let (mut b, mut s) = book();
    b.submit_limit(1, Side::Ask, 1000, 5, Tif::Gtc, &mut s);
    b.submit_limit(2, Side::Ask, 1000, 5, Tif::Gtc, &mut s);
    s.events.clear();
    b.submit_limit(3, Side::Bid, 1000, 7, Tif::Gtc, &mut s);
    assert_eq!(
        s.events,
        vec![
            Event::Accepted { id: 3 },
            Event::Trade {
                maker: 1,
                taker: 3,
                price: 1000,
                qty: 5
            },
            Event::Trade {
                maker: 2,
                taker: 3,
                price: 1000,
                qty: 2
            },
        ]
    );
    // Maker 2 keeps its remaining 3 lots at the head of the level.
    assert_eq!(b.order_qty(2), Some(3));
    assert_eq!(b.level_qty(Side::Ask, 1000), 3);
    b.assert_consistent();
}

#[test]
fn partial_fill_rests_remainder() {
    let (mut b, mut s) = book();
    b.submit_limit(1, Side::Ask, 1000, 4, Tif::Gtc, &mut s);
    b.submit_limit(2, Side::Bid, 1000, 10, Tif::Gtc, &mut s);
    assert_eq!(b.order_qty(2), Some(6));
    assert_eq!(b.best_bid(), Some(1000));
    assert_eq!(b.best_ask(), None);
    b.assert_consistent();
}

#[test]
fn ioc_cancels_remainder() {
    let (mut b, mut s) = book();
    b.submit_limit(1, Side::Ask, 1000, 4, Tif::Gtc, &mut s);
    s.events.clear();
    b.submit_limit(2, Side::Bid, 1000, 10, Tif::Ioc, &mut s);
    assert_eq!(
        s.events,
        vec![
            Event::Accepted { id: 2 },
            Event::Trade {
                maker: 1,
                taker: 2,
                price: 1000,
                qty: 4
            },
            Event::Canceled {
                id: 2,
                remaining: 6
            },
        ]
    );
    assert!(b.is_empty());
    b.assert_consistent();
}

#[test]
fn fok_rejects_without_side_effects() {
    let (mut b, mut s) = book();
    b.submit_limit(1, Side::Ask, 1000, 4, Tif::Gtc, &mut s);
    b.submit_limit(2, Side::Ask, 1001, 4, Tif::Gtc, &mut s);
    s.events.clear();
    // Only 4 lots available within limit 1000; 8 requested.
    b.submit_limit(3, Side::Bid, 1000, 8, Tif::Fok, &mut s);
    assert_eq!(
        s.events,
        vec![Event::Rejected {
            id: 3,
            reason: RejectReason::FokUnfillable
        }]
    );
    assert_eq!(b.level_qty(Side::Ask, 1000), 4);
    assert_eq!(b.level_qty(Side::Ask, 1001), 4);
    b.assert_consistent();
}

#[test]
fn fok_fills_in_full_when_liquidity_suffices() {
    let (mut b, mut s) = book();
    b.submit_limit(1, Side::Ask, 1000, 4, Tif::Gtc, &mut s);
    b.submit_limit(2, Side::Ask, 1001, 4, Tif::Gtc, &mut s);
    s.events.clear();
    b.submit_limit(3, Side::Bid, 1001, 8, Tif::Fok, &mut s);
    let trades: Vec<_> = s
        .events
        .iter()
        .filter(|e| matches!(e, Event::Trade { .. }))
        .collect();
    assert_eq!(trades.len(), 2);
    assert!(b.is_empty());
    b.assert_consistent();
}

#[test]
fn market_order_sweeps_levels_and_cancels_remainder() {
    let (mut b, mut s) = book();
    b.submit_limit(1, Side::Ask, 1000, 3, Tif::Gtc, &mut s);
    b.submit_limit(2, Side::Ask, 1002, 3, Tif::Gtc, &mut s);
    s.events.clear();
    b.submit_market(3, Side::Bid, 10, &mut s);
    assert_eq!(
        s.events,
        vec![
            Event::Accepted { id: 3 },
            Event::Trade {
                maker: 1,
                taker: 3,
                price: 1000,
                qty: 3
            },
            Event::Trade {
                maker: 2,
                taker: 3,
                price: 1002,
                qty: 3
            },
            Event::Canceled {
                id: 3,
                remaining: 4
            },
        ]
    );
    assert!(b.is_empty());
    b.assert_consistent();
}

#[test]
fn cancel_removes_resting_order() {
    let (mut b, mut s) = book();
    b.submit_limit(1, Side::Bid, 999, 10, Tif::Gtc, &mut s);
    s.events.clear();
    b.cancel(1, &mut s);
    assert_eq!(
        s.events,
        vec![Event::Canceled {
            id: 1,
            remaining: 10
        }]
    );
    assert!(b.is_empty());
    assert_eq!(b.best_bid(), None);
    // Second cancel: unknown.
    s.events.clear();
    b.cancel(1, &mut s);
    assert_eq!(
        s.events,
        vec![Event::Rejected {
            id: 1,
            reason: RejectReason::UnknownOrder
        }]
    );
    b.assert_consistent();
}

#[test]
fn cancel_middle_of_queue_preserves_fifo() {
    let (mut b, mut s) = book();
    b.submit_limit(1, Side::Ask, 1000, 1, Tif::Gtc, &mut s);
    b.submit_limit(2, Side::Ask, 1000, 2, Tif::Gtc, &mut s);
    b.submit_limit(3, Side::Ask, 1000, 3, Tif::Gtc, &mut s);
    b.cancel(2, &mut s);
    s.events.clear();
    b.submit_limit(4, Side::Bid, 1000, 4, Tif::Gtc, &mut s);
    assert_eq!(
        s.events,
        vec![
            Event::Accepted { id: 4 },
            Event::Trade {
                maker: 1,
                taker: 4,
                price: 1000,
                qty: 1
            },
            Event::Trade {
                maker: 3,
                taker: 4,
                price: 1000,
                qty: 3
            },
        ]
    );
    b.assert_consistent();
}

#[test]
fn duplicate_id_rejected() {
    let (mut b, mut s) = book();
    b.submit_limit(1, Side::Bid, 999, 10, Tif::Gtc, &mut s);
    s.events.clear();
    b.submit_limit(1, Side::Ask, 1001, 5, Tif::Gtc, &mut s);
    assert_eq!(
        s.events,
        vec![Event::Rejected {
            id: 1,
            reason: RejectReason::DuplicateId
        }]
    );
    b.assert_consistent();
}

#[test]
fn zero_qty_rejected() {
    let (mut b, mut s) = book();
    b.submit_limit(1, Side::Bid, 999, 0, Tif::Gtc, &mut s);
    assert_eq!(
        s.events,
        vec![Event::Rejected {
            id: 1,
            reason: RejectReason::ZeroQty
        }]
    );
}

#[test]
fn replace_qty_decrease_keeps_queue_priority() {
    let (mut b, mut s) = book();
    b.submit_limit(1, Side::Ask, 1000, 10, Tif::Gtc, &mut s);
    b.submit_limit(2, Side::Ask, 1000, 10, Tif::Gtc, &mut s);
    b.replace(1, 1000, 6, &mut s); // shrink in place
    s.events.clear();
    b.submit_limit(3, Side::Bid, 1000, 6, Tif::Gtc, &mut s);
    // Order 1 still fills first: priority kept.
    assert_eq!(
        s.events,
        vec![
            Event::Accepted { id: 3 },
            Event::Trade {
                maker: 1,
                taker: 3,
                price: 1000,
                qty: 6
            },
        ]
    );
    assert_eq!(b.order_qty(2), Some(10));
    b.assert_consistent();
}

#[test]
fn replace_qty_increase_loses_queue_priority() {
    let (mut b, mut s) = book();
    b.submit_limit(1, Side::Ask, 1000, 5, Tif::Gtc, &mut s);
    b.submit_limit(2, Side::Ask, 1000, 5, Tif::Gtc, &mut s);
    b.replace(1, 1000, 8, &mut s); // size up: goes to back of queue
    s.events.clear();
    b.submit_limit(3, Side::Bid, 1000, 5, Tif::Gtc, &mut s);
    assert_eq!(
        s.events,
        vec![
            Event::Accepted { id: 3 },
            Event::Trade {
                maker: 2,
                taker: 3,
                price: 1000,
                qty: 5
            },
        ]
    );
    assert_eq!(b.order_qty(1), Some(8));
    b.assert_consistent();
}

#[test]
fn replace_to_crossing_price_trades_immediately() {
    let (mut b, mut s) = book();
    b.submit_limit(1, Side::Bid, 999, 5, Tif::Gtc, &mut s);
    b.submit_limit(2, Side::Ask, 1001, 5, Tif::Gtc, &mut s);
    s.events.clear();
    b.replace(1, 1001, 5, &mut s); // reprice bid through the ask
    assert_eq!(
        s.events,
        vec![
            Event::Replaced { id: 1 },
            Event::Trade {
                maker: 2,
                taker: 1,
                price: 1001,
                qty: 5
            },
        ]
    );
    assert!(b.is_empty());
    b.assert_consistent();
}

#[test]
fn depth_iterators_are_price_ordered() {
    let (mut b, mut s) = book();
    for (id, price) in [(1, 998), (2, 999), (3, 997)] {
        b.submit_limit(id, Side::Bid, price, 10, Tif::Gtc, &mut s);
    }
    for (id, price) in [(4, 1002), (5, 1001), (6, 1003)] {
        b.submit_limit(id, Side::Ask, price, 10, Tif::Gtc, &mut s);
    }
    let bids: Vec<_> = b.bid_levels().map(|(p, _)| p).collect();
    let asks: Vec<_> = b.ask_levels().map(|(p, _)| p).collect();
    assert_eq!(bids, vec![999, 998, 997]); // best first
    assert_eq!(asks, vec![1001, 1002, 1003]); // best first
}
