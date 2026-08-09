//! Live dashboard for the matching engine + backtester.
//!
//! A simulation thread paces the backtest in real time and publishes a
//! pre-serialized JSON snapshot; a `std::net::TcpListener` HTTP server (no
//! frameworks, no dependencies) serves the embedded single-page UI and the
//! snapshot. The browser polls `/state` and renders the depth ladder,
//! equity curve, and trade tape.
//!
//! ```text
//! GET /                     the dashboard page
//! GET /state                current JSON snapshot
//! GET /set?informed=NN      restart the sim with a new flow toxicity
//! GET /pause                toggle pause
//! ```

use std::fmt::Write as _;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use backtest::{MarketMaker, Sim, SimConfig};

const PORT: u16 = 7878;
/// Sim steps per UI frame: ~60k steps/sec at 16ms — fast enough to watch
/// the P&L develop, slow enough to see the book breathe.
const BATCH: u64 = 1_000;

struct Control {
    informed_pct: u64,
    paused: bool,
    restart: bool,
}

struct Shared {
    snapshot: Mutex<String>,
    control: Mutex<Control>,
}

fn main() {
    let shared = Arc::new(Shared {
        snapshot: Mutex::new(String::from("{}")),
        control: Mutex::new(Control {
            informed_pct: 40,
            paused: false,
            restart: true,
        }),
    });

    {
        let shared = Arc::clone(&shared);
        std::thread::spawn(move || sim_loop(&shared));
    }

    let listener = TcpListener::bind(("127.0.0.1", PORT))
        .unwrap_or_else(|e| panic!("cannot bind 127.0.0.1:{PORT}: {e}"));
    println!("dashboard: http://127.0.0.1:{PORT}");
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let shared = Arc::clone(&shared);
        std::thread::spawn(move || {
            let _ = handle(stream, &shared);
        });
    }
}

fn sim_loop(shared: &Shared) {
    let mut sim = new_sim(40);
    loop {
        let (paused, restart_to) = {
            let mut c = shared.control.lock().unwrap();
            let r = c.restart.then_some(c.informed_pct);
            c.restart = false;
            (c.paused, r)
        };
        if let Some(informed) = restart_to {
            sim = new_sim(informed);
        }
        if !paused {
            for _ in 0..BATCH {
                sim.step();
            }
        }
        let json = serialize(&sim, paused);
        *shared.snapshot.lock().unwrap() = json;
        std::thread::sleep(Duration::from_millis(16));
    }
}

fn new_sim(informed_pct: u64) -> Sim<MarketMaker> {
    let cfg = SimConfig {
        informed_pct,
        ..SimConfig::default()
    };
    Sim::new(cfg, MarketMaker::new(2, 5, 50, 50))
}

/// Hand-rolled JSON: the shape is fixed and flat, so a formatter is all the
/// serialization this needs.
fn serialize(sim: &Sim<MarketMaker>, paused: bool) -> String {
    let book = sim.book();
    let acct = sim.account();
    let mark = sim.mark();
    let mut s = String::with_capacity(8 * 1024);
    s.push('{');
    let _ = write!(
        s,
        "\"step\":{},\"paused\":{},\"informed\":{},\"mark\":{},\"position\":{},\"cash\":{},\"equity\":{},\"fills\":{},\"lots\":{}",
        sim.current_step(),
        paused,
        sim.config().informed_pct,
        mark,
        acct.position,
        acct.cash,
        acct.equity(mark),
        acct.fills,
        acct.traded_lots,
    );
    let _ = write!(
        s,
        ",\"best_bid\":{},\"best_ask\":{}",
        book.best_bid().map_or(0, |p| p),
        book.best_ask().map_or(0, |p| p)
    );

    s.push_str(",\"bids\":[");
    for (i, (p, q)) in book.bid_levels().take(15).enumerate() {
        if i > 0 {
            s.push(',');
        }
        let _ = write!(s, "[{p},{q}]");
    }
    s.push_str("],\"asks\":[");
    for (i, (p, q)) in book.ask_levels().take(15).enumerate() {
        if i > 0 {
            s.push(',');
        }
        let _ = write!(s, "[{p},{q}]");
    }

    s.push_str("],\"tape\":[");
    for (i, t) in sim.tape.iter().rev().take(24).enumerate() {
        if i > 0 {
            s.push(',');
        }
        let _ = write!(
            s,
            "[{},{},{},{}]",
            t.step,
            t.price,
            t.qty,
            u8::from(t.strategy_involved)
        );
    }

    s.push_str("],\"curve\":[");
    let curve = sim.equity_curve();
    let stride = (curve.len() / 400).max(1);
    let mut first = true;
    for (i, (step, eq)) in curve.iter().enumerate() {
        if i % stride != 0 && i != curve.len() - 1 {
            continue;
        }
        if !first {
            s.push(',');
        }
        first = false;
        let _ = write!(s, "[{step},{eq}]");
    }
    s.push_str("]}");
    s
}

fn handle(stream: TcpStream, shared: &Shared) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let path = line.split_whitespace().nth(1).unwrap_or("/");

    let (status, ctype, body): (&str, &str, String) = if path == "/" {
        ("200 OK", "text/html; charset=utf-8", UI.to_string())
    } else if path == "/state" {
        (
            "200 OK",
            "application/json",
            shared.snapshot.lock().unwrap().clone(),
        )
    } else if let Some(q) = path.strip_prefix("/set?informed=") {
        match q.parse::<u64>() {
            Ok(v) if v <= 100 => {
                let mut c = shared.control.lock().unwrap();
                c.informed_pct = v;
                c.restart = true;
                ("200 OK", "text/plain", "ok".to_string())
            }
            _ => (
                "400 Bad Request",
                "text/plain",
                "informed must be 0-100".to_string(),
            ),
        }
    } else if path == "/pause" {
        let mut c = shared.control.lock().unwrap();
        c.paused = !c.paused;
        ("200 OK", "text/plain", format!("paused={}", c.paused))
    } else {
        ("404 Not Found", "text/plain", "not found".to_string())
    };

    let mut stream = stream;
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body.as_bytes())
}

const UI: &str = include_str!("ui.html");
