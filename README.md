# orderflow — a low-latency limit order book matching engine in Rust

A single-threaded, price-time-priority matching engine plus a fixed-layout
binary order-entry protocol and a deterministic replay/benchmark harness.
Built with **zero third-party dependencies** — everything from the intrusive
data structures to the benchmark statistics is implemented from `std` up.

```
crates/
  lob       the matching engine: slab-allocated order storage, intrusive
            per-price FIFO queues, GTC/IOC/FOK, cancel and cancel/replace
  wire      binary order-entry codec (OUCH-style fixed layouts, zero-alloc
            streaming decode)
  replay    deterministic workload generator + end-to-end throughput and
            latency-percentile measurement
  bench     dependency-free microbenchmark suite (median-of-trials)
  backtest  event-driven backtester using the engine as a simulated
            exchange, with queue-position-accurate fills
```

## Design

**Matching semantics.** Strict price-time priority. Incoming limit orders
match against the opposite side while their price crosses, filling resting
orders FIFO within each level, always trading at the resting (maker) price.
Unfilled remainders rest (GTC), cancel (IOC), or the whole order is rejected
up front with no side effects (FOK, checked against visible liquidity before
any fill). Market orders sweep at any price and never rest. Cancel/replace
keeps queue priority only for a pure size decrease at the same price —
anything else loses priority and re-enters the matching path, exactly like
real exchange semantics.

**Data layout.** The engine is built around three structures:

- *Slab order storage.* Every resting order is a 48-byte node in one
  contiguous `Vec`, addressed by `u32` index, recycled through an intrusive
  free list. No per-order allocation, no pointer chasing across the heap.
- *Intrusive FIFO queues.* Each price level is a doubly-linked list threaded
  through the slab. Filling from the head gives time priority; O(1) unlink
  given the index gives cheap cancels — and cancels dominate real order flow.
- *`BTreeMap` price ladders* keyed by integer ticks, one per side. Lookups
  are O(log L) in the number of *distinct price levels*, which stays small
  regardless of how many orders rest. The order-id index is a `HashMap` with
  a SplitMix64-finalizer hasher (ids are internal, so SipHash's DoS
  resistance is wasted cost).

**Hot path discipline.** No allocation during matching; all output flows
through a caller-supplied `EventSink` trait (monomorphized, inlined), so the
engine composes with a ring buffer, a wire encoder, or a null sink in
benchmarks without paying for any of them.

**Prices are integer ticks, quantities integer lots** — no floating point
anywhere in the matching path, so behavior is exact and identical across
platforms.

## Correctness

- 17 semantic tests pin down matching behavior: priority, maker pricing,
  TIF edge cases, FIFO preservation across mid-queue cancels, replace
  priority rules, event ordering.
- A **differential test** replays tens of thousands of randomized seeded
  operations through both the engine and a deliberately naive reference
  implementation (flat `Vec`, O(n) scans, obviously correct) and asserts the
  *entire event stream* matches exactly, plus periodic deep equality of book
  state.
- `OrderBook::assert_consistent` exhaustively validates structural
  invariants (link integrity, cached aggregates, id-index coherence,
  uncrossed book) and runs throughout the differential test.
- The replay harness prints an order-sensitive FNV digest of full book depth:
  same seed, same digest, on any platform — regressions in matching logic
  are immediately visible.

## Performance

Measured on a 13th-gen Intel i7-13700HX (Windows 11, `--release` with fat
LTO, single thread). Reproduce with `cargo run --release -p bench` and
`cargo run --release -p replay`.

| workload | median | throughput |
|---|---|---|
| passive insert + cancel | ~33 ns/op | ~30 M ops/s |
| aggressive IOC filling 3.5 resting orders | ~11 ns/op | ~93 M ops/s |
| best bid + best ask | ~1.8 ns | — |
| wire decode (mixed stream) | ~1.0 ns/msg | ~1000 M msgs/s |

End-to-end replay — decode from bytes + full matching, realistic mixed flow
(55% new GTC, 10% IOC, 5% FOK, 15% cancel, 10% replace, 5% market, random-walk
mid):

```
2,000,000 msgs in 113 ms  ->  17.7 M msgs/s

per-message latency (sampled):
  p50      100 ns      (timer granularity floor)
  p99      300 ns
  p99.9    900 ns
```

The latency histogram is quantized by the ~100 ns Windows `Instant`
resolution; the throughput number (56 ns/msg mean, including decode) is the
more precise figure.

## Backtester

`backtest` turns the engine into a simulated exchange: a seeded background
flow populates the book, and the strategy's orders enter the *same* book,
competing for queue position like any other participant. Passive fills only
happen when simulated aggressive flow actually consumes the orders queued
ahead — no fill model is bolted on, the realism falls out of the engine's
FIFO levels.

The flow generator has a **toxicity dial**: `informed_pct` controls what
fraction of background orders are priced off the *true* (hidden) mid versus
the observable book mid. That single knob reproduces adverse selection from
first principles — the same naive symmetric market maker flips from
profitable to badly negative as the flow gets more informed:

```
market maker (half-spread 2, inventory skew) vs flow toxicity:
  informed   0%    equity     52549   maxdd      282   avg sell-buy +0.32t
  informed  20%    equity     97615   maxdd      235   avg sell-buy +0.53t
  informed  40%    equity     91554   maxdd      258   avg sell-buy +0.49t
  informed  60%    equity     46599   maxdd     1133   avg sell-buy +0.27t
  informed  80%    equity   -145810   maxdd   152168   avg sell-buy -0.94t
  informed 100%    equity   -349838   maxdd   351020   avg sell-buy -2.50t
```

Strategies implement a two-method trait (`on_wake` → actions, `on_event` →
fills); the account tracks position, cash, mark-to-market equity, drawdown,
and a buy/sell price decomposition that shows *where* P&L comes from.
Runs are fully deterministic from the seed.

```bash
cargo run --release -p backtest
```

## Running

```bash
cargo test --workspace          # semantic + differential + codec tests
cargo run --release -p bench    # microbenchmarks
cargo run --release -p replay -- --msgs 5000000 --seed 7
```

Determinism check: run `replay` twice with the same `--seed` — byte-identical
totals and depth digest every time.

## Notes on scope

Single-threaded by design: real matching engines serialize all order flow
through one matching thread per instrument (the book is inherently a serial
data structure); concurrency lives in the I/O layer around it. The natural
extensions — an SPSC ring between a network thread and the matching thread,
per-instrument sharding, an array-backed price ladder for dense books — are
I/O and layout work on top of the same core.

## Toolchain note (Windows)

Builds on the `stable-x86_64-pc-windows-gnu` toolchain out of the box (no
Visual Studio Build Tools required). With MSVC build tools installed, the
default `stable-msvc` toolchain works too.
