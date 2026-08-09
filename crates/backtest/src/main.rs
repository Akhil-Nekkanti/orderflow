//! Backtest demo: a market maker across flow-toxicity levels, plus a
//! momentum taker, all against the same seeded synthetic market.
//!
//! ```text
//! backtest [--steps N] [--seed S]
//! ```

use backtest::{MarketMaker, Momentum, Report, SimConfig, Strategy, run};

fn report_line(label: &str, r: &Report) {
    let avg_buy = r.bought_notional as f64 / r.bought_lots.max(1) as f64;
    let avg_sell = r.sold_notional as f64 / r.sold_lots.max(1) as f64;
    println!(
        "  {label:<16} equity {:>9}   maxdd {:>8}   fills {:>6}   avg sell-buy {:+.2}t",
        r.final_equity,
        r.max_drawdown_ticklots,
        r.fills,
        avg_sell - avg_buy
    );
}

fn run_with(cfg: &SimConfig, informed_pct: u64, strategy: &mut impl Strategy) -> Report {
    let cfg = SimConfig {
        informed_pct,
        steps: cfg.steps,
        seed: cfg.seed,
        ..SimConfig::default()
    };
    run(&cfg, strategy)
}

fn main() {
    let mut steps: u64 = 1_000_000;
    let mut seed: u64 = 42;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let value = args.get(i + 1).unwrap_or_else(|| {
            eprintln!("missing value for {}", args[i]);
            std::process::exit(2);
        });
        match args[i].as_str() {
            "--steps" => steps = value.parse().expect("--steps takes an integer"),
            "--seed" => seed = value.parse().expect("--seed takes an integer"),
            other => {
                eprintln!("unknown flag {other}; usage: backtest [--steps N] [--seed S]");
                std::process::exit(2);
            }
        }
        i += 2;
    }

    let cfg = SimConfig {
        steps,
        seed,
        ..SimConfig::default()
    };

    println!(
        "{} background messages per run (seed {}); equity in tick-lots\n",
        cfg.steps, cfg.seed
    );

    println!("market maker (half-spread 2, inventory skew) vs flow toxicity:");
    for informed in [0, 20, 40, 60, 80, 100] {
        let mut mm = MarketMaker::new(2, 5, 50, 50);
        let r = run_with(&cfg, informed, &mut mm);
        report_line(&format!("informed {informed:>3}%"), &r);
    }
    println!();
    println!("naive momentum taker (lookback 16, threshold 4t), 40% informed flow:");
    let mut momo = Momentum::new(16, 4, 5, 50);
    let r = run_with(&cfg, 40, &mut momo);
    report_line("momentum", &r);
    println!();
    println!("Takeaway: the same passive strategy flips from profitable to badly");
    println!("negative purely as a function of how informed the flow is — adverse");
    println!("selection, reproduced from first principles by the matching engine.");
}
