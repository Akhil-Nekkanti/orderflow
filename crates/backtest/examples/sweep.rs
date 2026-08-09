use backtest::{MarketMaker, SimConfig, run};
fn main() {
    println!("MM (half-spread 2, skew 0.5t/lot, maxpos 50) vs flow toxicity:");
    for informed in [0u64, 20, 40, 60, 80, 100] {
        let cfg = SimConfig {
            steps: 1_000_000,
            informed_pct: informed,
            ..SimConfig::default()
        };
        let r = run(&cfg, &mut MarketMaker::new(2, 5, 50, 50));
        let avg_buy = r.bought_notional as f64 / r.bought_lots.max(1) as f64;
        let avg_sell = r.sold_notional as f64 / r.sold_lots.max(1) as f64;
        println!(
            "informed={informed:>3}%: equity={:>9} dd={:>8} fills={:>6} sell-buy={:+.2}t",
            r.final_equity,
            r.max_drawdown_ticklots,
            r.fills,
            avg_sell - avg_buy
        );
    }
}
