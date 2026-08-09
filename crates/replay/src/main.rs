//! Deterministic replay harness.
//!
//! Generates a realistic order-flow workload (seeded, fully reproducible),
//! encodes it to the wire format, then measures the engine consuming the
//! byte stream: end-to-end throughput plus sampled per-message latency
//! percentiles.
//!
//! ```text
//! replay [--msgs N] [--seed S] [--sample K]
//! ```

use std::time::Instant;

use lob::{CountingSink, OrderBook, Side, Tif};
use wire::{Msg, Reader};

/// SplitMix64: tiny, fast, and identical output on every platform, which
/// keeps runs byte-for-byte reproducible from the seed alone.
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

/// Generates `n` messages of mixed order flow around a random-walking mid.
///
/// Mix: 55% new GTC limit, 10% IOC, 5% FOK, 15% cancel, 10% cancel/replace,
/// 5% market. Passive limits land within 16 ticks of mid on their own side;
/// one in five limits is priced through the touch so real matching happens.
fn generate(n: usize, seed: u64) -> Vec<u8> {
    let mut rng = Rng(seed);
    let mut buf = Vec::with_capacity(n * 27);
    let mut live: Vec<u64> = Vec::with_capacity(n / 4);
    let mut next_id: u64 = 1;
    let mut mid: i64 = 10_000;

    for _ in 0..n {
        // Mid drifts one tick every ~8 messages.
        if rng.below(8) == 0 {
            mid += if rng.below(2) == 0 { 1 } else { -1 };
        }
        let roll = rng.below(100);
        let msg = if roll < 70 || live.is_empty() {
            let side = if rng.below(2) == 0 {
                Side::Bid
            } else {
                Side::Ask
            };
            let offset = rng.below(16) as i64;
            let aggressive = rng.below(5) == 0;
            let price = match (side, aggressive) {
                (Side::Bid, false) => mid - offset,
                (Side::Bid, true) => mid + offset,
                (Side::Ask, false) => mid + offset,
                (Side::Ask, true) => mid - offset,
            };
            let tif = match roll {
                0..=54 => Tif::Gtc,
                55..=64 => Tif::Ioc,
                _ => Tif::Fok,
            };
            let id = next_id;
            next_id += 1;
            if tif == Tif::Gtc {
                live.push(id);
            }
            Msg::New {
                id,
                side,
                price,
                qty: 1 + rng.below(100),
                tif,
            }
        } else if roll < 85 {
            let at = rng.below(live.len() as u64) as usize;
            Msg::Cancel {
                id: live.swap_remove(at),
            }
        } else if roll < 95 {
            let at = rng.below(live.len() as u64) as usize;
            let id = live[at];
            Msg::Replace {
                id,
                price: mid + rng.below(16) as i64 - 8,
                qty: 1 + rng.below(100),
            }
        } else {
            let id = next_id;
            next_id += 1;
            Msg::Market {
                id,
                side: if rng.below(2) == 0 {
                    Side::Bid
                } else {
                    Side::Ask
                },
                qty: 1 + rng.below(200),
            }
        };
        wire::encode(&msg, &mut buf);
    }
    buf
}

#[inline]
fn apply(book: &mut OrderBook, msg: Msg, sink: &mut CountingSink) {
    match msg {
        Msg::New {
            id,
            side,
            price,
            qty,
            tif,
        } => book.submit_limit(id, side, price, qty, tif, sink),
        Msg::Cancel { id } => book.cancel(id, sink),
        Msg::Replace { id, price, qty } => book.replace(id, price, qty, sink),
        Msg::Market { id, side, qty } => book.submit_market(id, side, qty, sink),
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[rank]
}

fn main() {
    let mut msgs: usize = 2_000_000;
    let mut seed: u64 = 42;
    let mut sample: usize = 16;

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let value = args.get(i + 1).unwrap_or_else(|| {
            eprintln!("missing value for {}", args[i]);
            std::process::exit(2);
        });
        match args[i].as_str() {
            "--msgs" => msgs = value.parse().expect("--msgs takes an integer"),
            "--seed" => seed = value.parse().expect("--seed takes an integer"),
            "--sample" => sample = value.parse().expect("--sample takes an integer"),
            other => {
                eprintln!("unknown flag {other}; usage: replay [--msgs N] [--seed S] [--sample K]");
                std::process::exit(2);
            }
        }
        i += 2;
    }

    println!("generating {msgs} messages (seed {seed})...");
    let t0 = Instant::now();
    let stream = generate(msgs, seed);
    println!(
        "  {} bytes in {:.2?} ({:.1} MiB)",
        stream.len(),
        t0.elapsed(),
        stream.len() as f64 / (1024.0 * 1024.0)
    );

    let mut book = OrderBook::with_capacity(msgs / 2);
    let mut sink = CountingSink::default();
    let mut latencies: Vec<u64> = Vec::with_capacity(msgs / sample + 1);

    println!("replaying...");
    let run = Instant::now();
    let mut n = 0usize;
    for res in Reader::new(&stream) {
        let msg = res.expect("self-generated stream decodes");
        if n.is_multiple_of(sample) {
            let t = Instant::now();
            apply(&mut book, msg, &mut sink);
            latencies.push(t.elapsed().as_nanos() as u64);
        } else {
            apply(&mut book, msg, &mut sink);
        }
        n += 1;
    }
    let elapsed = run.elapsed();

    latencies.sort_unstable();
    let rate = n as f64 / elapsed.as_secs_f64();

    println!();
    println!("== throughput ==");
    println!(
        "  {n} msgs in {elapsed:.2?}  ->  {:.2} M msgs/s",
        rate / 1e6
    );
    println!();
    println!(
        "== per-message latency (decode+match, sampled 1/{sample}, n={}) ==",
        latencies.len()
    );
    for (label, p) in [
        ("p50", 0.50),
        ("p90", 0.90),
        ("p99", 0.99),
        ("p99.9", 0.999),
    ] {
        println!("  {label:>6}: {:>6} ns", percentile(&latencies, p));
    }
    println!(
        "  {:>6}: {:>6} ns",
        "max",
        latencies.last().copied().unwrap_or(0)
    );
    println!();
    println!("== engine totals ==");
    println!("  accepts:  {}", sink.accepts);
    println!("  trades:   {} ({} lots)", sink.trades, sink.traded_qty);
    println!("  cancels:  {}", sink.cancels);
    println!("  rejects:  {}", sink.rejects);
    println!();
    println!("== final book (determinism check) ==");
    println!("  resting orders: {}", book.len());
    println!(
        "  best bid/ask:   {:?} / {:?}",
        book.best_bid(),
        book.best_ask()
    );
    // Order-sensitive digest over the full depth: identical across runs and
    // platforms for the same seed and message count.
    let mut digest: u64 = 0xcbf2_9ce4_8422_2325;
    for (p, q) in book.bid_levels().chain(book.ask_levels()) {
        for word in [p as u64, q] {
            digest ^= word;
            digest = digest.wrapping_mul(0x0000_0100_0000_01B3);
        }
    }
    println!("  depth digest:   {digest:#018x}");
}
