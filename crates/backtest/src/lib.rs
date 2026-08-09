//! # backtest — event-driven strategy backtester
//!
//! Uses the `lob` matching engine as a *simulated exchange*: a seeded
//! background order flow populates the book, and the strategy's orders are
//! submitted into the very same book, competing for queue position exactly
//! like any other participant.
//!
//! This is the property that separates it from bar-based backtesters:
//! passive fills only happen when real (simulated) aggressive flow actually
//! reaches the strategy's price level *and* the orders queued ahead of it
//! have been consumed. Queue-position realism falls straight out of the
//! engine's FIFO levels — the backtester adds no fill model of its own.
//!
//! ```text
//! Simulator
//!  ├─ background flow generator (seeded SplitMix64 random-walk mid)
//!  ├─ OrderBook (the exchange)
//!  ├─ Strategy (quotes / takes via Action list)
//!  └─ Account   (cash, position, mark-to-market, equity curve)
//! ```

use lob::{Event, OrderBook, OrderId, Price, Qty, Side, Tif, VecSink};

/// Order ids at or above this base belong to the strategy; everything below
/// is background flow. Keeps participant attribution to one comparison.
pub const STRATEGY_ID_BASE: OrderId = 1 << 40;

// ---------------------------------------------------------------------------
// Strategy interface
// ---------------------------------------------------------------------------

/// What a strategy may do each time it is woken.
#[derive(Clone, Copy, Debug)]
pub enum Action {
    Limit {
        id: OrderId,
        side: Side,
        price: Price,
        qty: Qty,
        tif: Tif,
    },
    Market {
        id: OrderId,
        side: Side,
        qty: Qty,
    },
    Cancel {
        id: OrderId,
    },
    Replace {
        id: OrderId,
        price: Price,
        qty: Qty,
    },
}

/// Read-only view handed to the strategy on each wake-up.
pub struct MarketView<'a> {
    pub book: &'a OrderBook,
    pub step: u64,
    /// Price of the most recent trade by anyone, if any.
    pub last_trade: Option<Price>,
    /// Current position (signed lots) and cash of the strategy's account.
    pub position: i64,
    pub cash: i128,
}

impl MarketView<'_> {
    /// Midpoint of best bid/ask; falls back to last trade.
    pub fn mid(&self) -> Option<Price> {
        match (self.book.best_bid(), self.book.best_ask()) {
            (Some(b), Some(a)) => Some((b + a) / 2),
            _ => self.last_trade,
        }
    }
}

pub trait Strategy {
    /// Called every `wake_every` background messages. Push desired actions
    /// into `out`. Use [`IdGen`] for fresh order ids.
    fn on_wake(&mut self, view: &MarketView, ids: &mut IdGen, out: &mut Vec<Action>);

    /// Called for every engine event that involves one of the strategy's
    /// orders (fills as maker or taker, acks, cancels, rejects).
    fn on_event(&mut self, _ev: &Event) {}
}

/// Hands out strategy order ids from the reserved range.
pub struct IdGen {
    next: OrderId,
}

impl IdGen {
    fn new() -> Self {
        IdGen {
            next: STRATEGY_ID_BASE,
        }
    }

    pub fn next_id(&mut self) -> OrderId {
        let id = self.next;
        self.next += 1;
        id
    }
}

#[inline]
fn is_strategy(id: OrderId) -> bool {
    id >= STRATEGY_ID_BASE
}

// ---------------------------------------------------------------------------
// Account: position, cash, equity
// ---------------------------------------------------------------------------

/// Tracks the strategy's holdings. Prices are ticks, so cash is in
/// tick-lots; convert to currency at the caller's tick value.
#[derive(Default, Debug)]
pub struct Account {
    pub position: i64,
    pub cash: i128,
    pub fills: u64,
    pub traded_lots: u64,
    pub bought_lots: u64,
    pub bought_notional: i128,
    pub sold_lots: u64,
    pub sold_notional: i128,
}

impl Account {
    /// Applies one fill from the strategy's perspective.
    fn on_fill(&mut self, strategy_side: Side, price: Price, qty: Qty) {
        let notional = price as i128 * qty as i128;
        match strategy_side {
            Side::Bid => {
                self.position += qty as i64;
                self.cash -= notional;
                self.bought_lots += qty;
                self.bought_notional += notional;
            }
            Side::Ask => {
                self.position -= qty as i64;
                self.cash += notional;
                self.sold_lots += qty;
                self.sold_notional += notional;
            }
        }
        self.fills += 1;
        self.traded_lots += qty;
    }

    /// Cash plus position marked at `mark`.
    pub fn equity(&self, mark: Price) -> i128 {
        self.cash + self.position as i128 * mark as i128
    }
}

// ---------------------------------------------------------------------------
// Background flow generator (same family as the replay harness)
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

struct FlowGen {
    rng: Rng,
    live: Vec<OrderId>,
    next_id: OrderId,
    mid: Price,
}

enum FlowOp {
    Limit(OrderId, Side, Price, Qty, Tif),
    Market(OrderId, Side, Qty),
    Cancel(OrderId),
    Replace(OrderId, Price, Qty),
}

impl FlowGen {
    fn new(seed: u64, start_mid: Price) -> Self {
        FlowGen {
            rng: Rng(seed),
            live: Vec::new(),
            next_id: 1,
            mid: start_mid,
        }
    }

    /// `informed_pct` of orders are priced off the true (hidden) mid; the
    /// rest are priced off `book_mid`, the mid any participant can observe.
    /// Uninformed flow is what a market maker earns the spread from;
    /// informed flow is what runs it over. The ratio is the toxicity dial.
    fn next_op(&mut self, book_mid: Option<Price>, informed_pct: u64) -> FlowOp {
        if self.rng.below(8) == 0 {
            self.mid += if self.rng.below(2) == 0 { 1 } else { -1 };
        }
        let reference = if self.rng.below(100) < informed_pct {
            self.mid
        } else {
            book_mid.unwrap_or(self.mid)
        };
        let roll = self.rng.below(100);
        if roll < 70 || self.live.is_empty() {
            let side = if self.rng.below(2) == 0 {
                Side::Bid
            } else {
                Side::Ask
            };
            let offset = self.rng.below(16) as i64;
            let aggressive = self.rng.below(5) == 0;
            let price = match (side, aggressive) {
                (Side::Bid, false) => reference - offset,
                (Side::Bid, true) => reference + offset,
                (Side::Ask, false) => reference + offset,
                (Side::Ask, true) => reference - offset,
            };
            let tif = match roll {
                0..=54 => Tif::Gtc,
                55..=64 => Tif::Ioc,
                _ => Tif::Fok,
            };
            let id = self.next_id;
            self.next_id += 1;
            if tif == Tif::Gtc {
                self.live.push(id);
            }
            FlowOp::Limit(id, side, price, 1 + self.rng.below(100), tif)
        } else if roll < 85 {
            let at = self.rng.below(self.live.len() as u64) as usize;
            FlowOp::Cancel(self.live.swap_remove(at))
        } else if roll < 95 {
            let at = self.rng.below(self.live.len() as u64) as usize;
            let id = self.live[at];
            FlowOp::Replace(
                id,
                reference + self.rng.below(16) as i64 - 8,
                1 + self.rng.below(100),
            )
        } else {
            let id = self.next_id;
            self.next_id += 1;
            let side = if self.rng.below(2) == 0 {
                Side::Bid
            } else {
                Side::Ask
            };
            FlowOp::Market(id, side, 1 + self.rng.below(200))
        }
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct Report {
    pub steps: u64,
    pub fills: u64,
    pub traded_lots: u64,
    pub bought_lots: u64,
    pub bought_notional: i128,
    pub sold_lots: u64,
    pub sold_notional: i128,
    pub final_position: i64,
    pub final_equity: i128,
    pub return_ticklots: i128,
    pub max_drawdown_ticklots: i128,
    /// Mean/std of per-sample equity changes (unannualized).
    pub sharpe_per_sample: f64,
    pub equity_curve: Vec<(u64, i128)>,
}

fn compute_metrics(curve: &[(u64, i128)]) -> (i128, f64) {
    let mut max_seen = i128::MIN;
    let mut max_dd = 0i128;
    for &(_, eq) in curve {
        max_seen = max_seen.max(eq);
        max_dd = max_dd.max(max_seen - eq);
    }
    let rets: Vec<f64> = curve.windows(2).map(|w| (w[1].1 - w[0].1) as f64).collect();
    let sharpe = if rets.len() > 1 {
        let mean = rets.iter().sum::<f64>() / rets.len() as f64;
        let var = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (rets.len() - 1) as f64;
        if var > 0.0 { mean / var.sqrt() } else { 0.0 }
    } else {
        0.0
    };
    (max_dd, sharpe)
}

// ---------------------------------------------------------------------------
// Simulator
// ---------------------------------------------------------------------------

pub struct SimConfig {
    /// Background messages to process.
    pub steps: u64,
    pub seed: u64,
    /// Strategy wakes once per this many background messages.
    pub wake_every: u64,
    /// Equity curve sample interval (in steps).
    pub sample_every: u64,
    pub start_mid: Price,
    /// Steps 0..warmup run background flow only, letting the book fill in
    /// before the strategy starts quoting.
    pub warmup: u64,
    /// Percent (0-100) of background orders priced off the *true* hidden mid
    /// rather than the observable book mid. Higher = more toxic flow for a
    /// market maker; 100 reproduces fully-informed flow.
    pub informed_pct: u64,
}

impl Default for SimConfig {
    fn default() -> Self {
        SimConfig {
            steps: 1_000_000,
            seed: 42,
            wake_every: 8,
            sample_every: 1_000,
            start_mid: 10_000,
            warmup: 5_000,
            informed_pct: 100,
        }
    }
}

pub fn run<S: Strategy>(cfg: &SimConfig, strategy: &mut S) -> Report {
    let mut book = OrderBook::with_capacity(1 << 16);
    let mut flow = FlowGen::new(cfg.seed, cfg.start_mid);
    let mut account = Account::default();
    let mut ids = IdGen::new();
    let mut sink = VecSink::default();
    let mut actions: Vec<Action> = Vec::new();
    let mut last_trade: Option<Price> = None;
    let mut last_mark = cfg.start_mid;
    let mut curve: Vec<(u64, i128)> = Vec::new();
    // Side of each open strategy order, needed to book fills where the
    // strategy is the resting maker.
    let mut open_sides: std::collections::HashMap<OrderId, Side> = std::collections::HashMap::new();

    for step in 0..cfg.steps {
        // 1. One background message.
        sink.events.clear();
        let book_mid = match (book.best_bid(), book.best_ask()) {
            (Some(b), Some(a)) => Some((b + a) / 2),
            _ => last_trade,
        };
        match flow.next_op(book_mid, cfg.informed_pct) {
            FlowOp::Limit(id, side, price, qty, tif) => {
                book.submit_limit(id, side, price, qty, tif, &mut sink)
            }
            FlowOp::Market(id, side, qty) => book.submit_market(id, side, qty, &mut sink),
            FlowOp::Cancel(id) => book.cancel(id, &mut sink),
            FlowOp::Replace(id, price, qty) => book.replace(id, price, qty, &mut sink),
        }
        route_events(&sink, &open_sides, &mut account, &mut last_trade, strategy);
        prune_filled(&sink, &mut open_sides);

        // 2. Strategy wake-up.
        if step >= cfg.warmup && step % cfg.wake_every == 0 {
            actions.clear();
            let view = MarketView {
                book: &book,
                step,
                last_trade,
                position: account.position,
                cash: account.cash,
            };
            strategy.on_wake(&view, &mut ids, &mut actions);
            for &action in actions.iter() {
                sink.events.clear();
                match action {
                    Action::Limit {
                        id,
                        side,
                        price,
                        qty,
                        tif,
                    } => {
                        open_sides.insert(id, side);
                        book.submit_limit(id, side, price, qty, tif, &mut sink);
                    }
                    Action::Market { id, side, qty } => {
                        open_sides.insert(id, side);
                        book.submit_market(id, side, qty, &mut sink);
                    }
                    Action::Cancel { id } => book.cancel(id, &mut sink),
                    Action::Replace { id, price, qty } => book.replace(id, price, qty, &mut sink),
                }
                route_events(&sink, &open_sides, &mut account, &mut last_trade, strategy);
                prune_filled(&sink, &mut open_sides);
            }
        }

        // 3. Sample the equity curve.
        if step % cfg.sample_every == 0 {
            if let (Some(b), Some(a)) = (book.best_bid(), book.best_ask()) {
                last_mark = (b + a) / 2;
            } else if let Some(p) = last_trade {
                last_mark = p;
            }
            curve.push((step, account.equity(last_mark)));
        }
    }

    let final_equity = account.equity(last_mark);
    curve.push((cfg.steps, final_equity));
    let (max_dd, sharpe) = compute_metrics(&curve);
    Report {
        steps: cfg.steps,
        fills: account.fills,
        traded_lots: account.traded_lots,
        bought_lots: account.bought_lots,
        bought_notional: account.bought_notional,
        sold_lots: account.sold_lots,
        sold_notional: account.sold_notional,
        final_position: account.position,
        final_equity,
        return_ticklots: final_equity,
        max_drawdown_ticklots: max_dd,
        sharpe_per_sample: sharpe,
        equity_curve: curve,
    }
}

/// Books fills touching strategy orders and forwards their events.
fn route_events<S: Strategy>(
    sink: &VecSink,
    open_sides: &std::collections::HashMap<OrderId, Side>,
    account: &mut Account,
    last_trade: &mut Option<Price>,
    strategy: &mut S,
) {
    for ev in &sink.events {
        if let Event::Trade {
            maker,
            taker,
            price,
            qty,
        } = *ev
        {
            *last_trade = Some(price);
            // Strategy on exactly one side of a trade (it never self-crosses
            // in the demo strategies; if it did, both arms fire and net out).
            if is_strategy(maker) {
                let side = open_sides[&maker];
                account.on_fill(side, price, qty);
            }
            if is_strategy(taker) {
                let side = open_sides[&taker];
                account.on_fill(side, price, qty);
            }
        }
        let involves = match *ev {
            Event::Trade { maker, taker, .. } => is_strategy(maker) || is_strategy(taker),
            Event::Accepted { id }
            | Event::Canceled { id, .. }
            | Event::Replaced { id }
            | Event::Rejected { id, .. } => is_strategy(id),
        };
        if involves {
            strategy.on_event(ev);
        }
    }
}

/// Drops bookkeeping for strategy orders that left the book.
fn prune_filled(sink: &VecSink, open_sides: &mut std::collections::HashMap<OrderId, Side>) {
    for ev in &sink.events {
        if let Event::Canceled { id, .. } = *ev
            && is_strategy(id)
        {
            open_sides.remove(&id);
        }
        // Fully-filled makers no longer need their side entry either, but a
        // stale entry is harmless (ids are never reused) and pruning fills
        // exactly would require tracking remaining qty here. Memory cost is
        // bounded by total strategy orders, fine for a backtest run.
    }
}

// ---------------------------------------------------------------------------
// Example strategies
// ---------------------------------------------------------------------------

/// Symmetric quoter with inventory skew: quotes `qty` on each side
/// `half_spread` ticks around a center shifted against current inventory, so
/// a long book leans quotes downward to shed and vice versa.
pub struct MarketMaker {
    pub half_spread: Price,
    pub qty: Qty,
    /// Ticks to shift the quote center per lot of inventory.
    pub skew_per_lot_x100: i64,
    pub max_position: i64,
    bid: Option<Quote>,
    ask: Option<Quote>,
}

#[derive(Clone, Copy)]
struct Quote {
    id: OrderId,
    price: Price,
    remaining: Qty,
}

impl MarketMaker {
    pub fn new(half_spread: Price, qty: Qty, skew_per_lot_x100: i64, max_position: i64) -> Self {
        MarketMaker {
            half_spread,
            qty,
            skew_per_lot_x100,
            max_position,
            bid: None,
            ask: None,
        }
    }
}

impl Strategy for MarketMaker {
    fn on_wake(&mut self, view: &MarketView, ids: &mut IdGen, out: &mut Vec<Action>) {
        let Some(mid) = view.mid() else { return };
        let center = mid - view.position * self.skew_per_lot_x100 / 100;
        let mut want_bid = center - self.half_spread;
        let mut want_ask = center + self.half_spread;
        // Never quote through the opposite touch: a skewed center after a
        // mid jump would otherwise turn a passive quote into an accidental
        // aggressive order that pays the spread instead of earning it.
        if let Some(best_ask) = view.book.best_ask() {
            want_bid = want_bid.min(best_ask - 1);
        }
        if let Some(best_bid) = view.book.best_bid() {
            want_ask = want_ask.max(best_bid + 1);
        }

        // Re-quote a side only when its price target moved: needless
        // cancel/replace churn would forfeit queue position.
        if let Some(q) = self.bid
            && q.price != want_bid
        {
            out.push(Action::Cancel { id: q.id });
            self.bid = None;
        }
        if let Some(q) = self.ask
            && q.price != want_ask
        {
            out.push(Action::Cancel { id: q.id });
            self.ask = None;
        }
        if self.bid.is_none() && view.position < self.max_position {
            let id = ids.next_id();
            out.push(Action::Limit {
                id,
                side: Side::Bid,
                price: want_bid,
                qty: self.qty,
                tif: Tif::Gtc,
            });
            self.bid = Some(Quote {
                id,
                price: want_bid,
                remaining: self.qty,
            });
        }
        if self.ask.is_none() && view.position > -self.max_position {
            let id = ids.next_id();
            out.push(Action::Limit {
                id,
                side: Side::Ask,
                price: want_ask,
                qty: self.qty,
                tif: Tif::Gtc,
            });
            self.ask = Some(Quote {
                id,
                price: want_ask,
                remaining: self.qty,
            });
        }
    }

    fn on_event(&mut self, ev: &Event) {
        match *ev {
            // Forget a quote once it is gone from the book.
            Event::Canceled { id, .. } | Event::Rejected { id, .. } => {
                if self.bid.map(|q| q.id) == Some(id) {
                    self.bid = None;
                }
                if self.ask.map(|q| q.id) == Some(id) {
                    self.ask = None;
                }
            }
            // Decrement resting size on maker fills; a fully-filled quote is
            // gone from the book and must be forgotten so the next wake can
            // re-quote that side.
            Event::Trade {
                maker, taker, qty, ..
            } => {
                for quote in [&mut self.bid, &mut self.ask] {
                    // Decrement on fills as maker (resting) or as taker
                    // (lots consumed before the remainder rested).
                    if let Some(q) = quote
                        && (q.id == maker || q.id == taker)
                    {
                        q.remaining = q.remaining.saturating_sub(qty);
                        if q.remaining == 0 {
                            *quote = None;
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

/// Naive momentum taker: compares mid to its value `lookback` wakes ago and
/// crosses the spread when the move exceeds `threshold` ticks.
pub struct Momentum {
    pub lookback: usize,
    pub threshold: Price,
    pub clip: Qty,
    pub max_position: i64,
    mids: Vec<Price>,
}

impl Momentum {
    pub fn new(lookback: usize, threshold: Price, clip: Qty, max_position: i64) -> Self {
        Momentum {
            lookback,
            threshold,
            clip,
            max_position,
            mids: Vec::new(),
        }
    }
}

impl Strategy for Momentum {
    fn on_wake(&mut self, view: &MarketView, ids: &mut IdGen, out: &mut Vec<Action>) {
        let Some(mid) = view.mid() else { return };
        self.mids.push(mid);
        if self.mids.len() <= self.lookback {
            return;
        }
        let then = self.mids[self.mids.len() - 1 - self.lookback];
        let mv = mid - then;
        if mv >= self.threshold && view.position < self.max_position {
            out.push(Action::Market {
                id: ids.next_id(),
                side: Side::Bid,
                qty: self.clip,
            });
        } else if mv <= -self.threshold && view.position > -self.max_position {
            out.push(Action::Market {
                id: ids.next_id(),
                side: Side::Ask,
                qty: self.clip,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_across_runs() {
        let cfg = SimConfig {
            steps: 50_000,
            ..SimConfig::default()
        };
        let r1 = run(&cfg, &mut MarketMaker::new(2, 5, 50, 50));
        let r2 = run(&cfg, &mut MarketMaker::new(2, 5, 50, 50));
        assert_eq!(r1.final_equity, r2.final_equity);
        assert_eq!(r1.fills, r2.fills);
        assert_eq!(r1.equity_curve, r2.equity_curve);
    }

    #[test]
    fn accounting_flat_strategy_has_zero_pnl() {
        struct DoNothing;
        impl Strategy for DoNothing {
            fn on_wake(&mut self, _: &MarketView, _: &mut IdGen, _: &mut Vec<Action>) {}
        }
        let cfg = SimConfig {
            steps: 20_000,
            ..SimConfig::default()
        };
        let r = run(&cfg, &mut DoNothing);
        assert_eq!(r.fills, 0);
        assert_eq!(r.final_position, 0);
        assert_eq!(r.final_equity, 0);
    }

    #[test]
    fn market_taker_pays_the_spread() {
        // A strategy that immediately buys and sells at market should lose
        // roughly the spread on each round trip — its equity must be <= 0
        // in a book with a positive spread.
        struct RoundTripper {
            done: bool,
        }
        impl Strategy for RoundTripper {
            fn on_wake(&mut self, view: &MarketView, ids: &mut IdGen, out: &mut Vec<Action>) {
                if self.done || view.book.best_bid().is_none() || view.book.best_ask().is_none() {
                    return;
                }
                out.push(Action::Market {
                    id: ids.next_id(),
                    side: Side::Bid,
                    qty: 10,
                });
                out.push(Action::Market {
                    id: ids.next_id(),
                    side: Side::Ask,
                    qty: 10,
                });
                self.done = true;
            }
        }
        let cfg = SimConfig {
            steps: 20_000,
            ..SimConfig::default()
        };
        let r = run(&cfg, &mut RoundTripper { done: false });
        assert_eq!(r.final_position, 0);
        assert!(
            r.final_equity <= 0,
            "crossing the spread twice cannot profit"
        );
        assert!(r.fills >= 2);
    }
}
