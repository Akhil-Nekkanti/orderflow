//! Microbenchmark suite for the matching engine and wire codec.
//!
//! Deliberately dependency-free: a small harness that runs each workload
//! over fresh state, takes many trials, and reports the median and spread.
//! Medians over trials are robust to OS scheduling noise; per-op cost is
//! amortized over a batch large enough that timer overhead is negligible.
//!
//! Run with `cargo run --release -p bench`.

use std::hint::black_box;
use std::time::Instant;

use lob::{NullSink, OrderBook, Side, Tif};
use wire::{Msg, Reader};

/// Runs `f` (a batch of `ops_per_iter` operations over state built by
/// `setup`) for `trials` timed trials after `warmup` discarded ones, and
/// prints ns/op statistics.
fn bench<S, F: FnMut(&mut S)>(
    name: &str,
    ops_per_iter: u64,
    trials: usize,
    mut setup: impl FnMut() -> S,
    mut f: F,
) {
    const WARMUP: usize = 3;
    let mut samples: Vec<f64> = Vec::with_capacity(trials);
    for i in 0..WARMUP + trials {
        let mut state = setup();
        let t = Instant::now();
        f(&mut state);
        let elapsed = t.elapsed();
        black_box(&state);
        if i >= WARMUP {
            samples.push(elapsed.as_nanos() as f64 / ops_per_iter as f64);
        }
    }
    samples.sort_by(|a, b| a.total_cmp(b));
    let median = samples[samples.len() / 2];
    let p10 = samples[samples.len() / 10];
    let p90 = samples[samples.len() * 9 / 10];
    let mops = 1e3 / median;
    println!(
        "{name:<34} {median:>9.1} ns/op   [p10 {p10:>8.1}, p90 {p90:>8.1}]   {mops:>8.2} Mops/s"
    );
}

/// A book with `levels` price levels per side, `per_level` orders each,
/// one tick apart around mid 10_000.
fn seeded_book(levels: i64, per_level: u64) -> OrderBook {
    let mut book = OrderBook::with_capacity((levels as u64 * per_level * 2 + 1024) as usize);
    let mut sink = NullSink;
    let mut id = 1u64;
    for lvl in 0..levels {
        for _ in 0..per_level {
            book.submit_limit(id, Side::Bid, 9_999 - lvl, 10, Tif::Gtc, &mut sink);
            id += 1;
            book.submit_limit(id, Side::Ask, 10_001 + lvl, 10, Tif::Gtc, &mut sink);
            id += 1;
        }
    }
    book
}

fn bench_insert_cancel() {
    const N: u64 = 10_000;
    bench(
        "insert+cancel (passive, 16 lvls)",
        N * 2,
        30,
        || seeded_book(16, 8),
        |book| {
            let mut sink = NullSink;
            for i in 0..N {
                let (side, price) = if i % 2 == 0 {
                    (Side::Bid, 9_990 - (i % 8) as i64)
                } else {
                    (Side::Ask, 10_010 + (i % 8) as i64)
                };
                book.submit_limit(1_000_000 + i, side, price, 5, Tif::Gtc, &mut sink);
            }
            for i in 0..N {
                book.cancel(1_000_000 + i, &mut sink);
            }
        },
    );
}

fn bench_aggressive_match() {
    // Sides alternate: 125 sweeps per side * 35 lots = 4,375 lots, well under
    // the 10,240 lots per side the seeded book holds — every sweep in the
    // batch hits real liquidity instead of no-opping on an empty book.
    const N: u64 = 250;
    bench(
        "aggressive IOC (fills 3.5 makers)",
        N,
        30,
        || seeded_book(64, 16),
        |book| {
            let mut sink = NullSink;
            for i in 0..N {
                // 35 lots fills 3 whole 10-lot makers plus part of a 4th.
                let (side, price) = if i % 2 == 0 {
                    (Side::Bid, 10_004)
                } else {
                    (Side::Ask, 9_996)
                };
                book.submit_limit(2_000_000 + i, side, price, 35, Tif::Ioc, &mut sink);
            }
        },
    );
}

fn bench_top_of_book() {
    const N: u64 = 1_000_000;
    bench(
        "best_bid + best_ask",
        N,
        30,
        || seeded_book(64, 16),
        |book| {
            for _ in 0..N {
                black_box((book.best_bid(), book.best_ask()));
            }
        },
    );
}

fn sample_stream(n: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    for i in 0..n as u64 {
        let msg = match i % 4 {
            0 => Msg::New {
                id: i,
                side: Side::Bid,
                price: 10_000 + (i % 32) as i64,
                qty: 1 + i % 100,
                tif: Tif::Gtc,
            },
            1 => Msg::Cancel { id: i / 2 },
            2 => Msg::Replace {
                id: i / 2,
                price: 10_000 - (i % 32) as i64,
                qty: 1 + i % 50,
            },
            _ => Msg::Market {
                id: i,
                side: Side::Ask,
                qty: 1 + i % 200,
            },
        };
        wire::encode(&msg, &mut buf);
    }
    buf
}

fn bench_codec() {
    const N: usize = 100_000;
    let stream = sample_stream(N);
    bench(
        "wire decode (mixed msgs)",
        N as u64,
        50,
        || (),
        |_| {
            let mut count = 0u64;
            for msg in Reader::new(&stream) {
                black_box(msg.unwrap());
                count += 1;
            }
            assert_eq!(count, N as u64);
        },
    );
    bench(
        "wire encode (New)",
        N as u64,
        50,
        || Vec::with_capacity(N * 27),
        |buf: &mut Vec<u8>| {
            buf.clear();
            for i in 0..N as u64 {
                wire::encode(
                    &Msg::New {
                        id: i,
                        side: Side::Bid,
                        price: 10_000,
                        qty: 10,
                        tif: Tif::Gtc,
                    },
                    buf,
                );
            }
            black_box(buf.len());
        },
    );
}

fn main() {
    println!(
        "workload                              median            spread                throughput"
    );
    println!("{}", "-".repeat(100));
    bench_insert_cancel();
    bench_aggressive_match();
    bench_top_of_book();
    bench_codec();
}
