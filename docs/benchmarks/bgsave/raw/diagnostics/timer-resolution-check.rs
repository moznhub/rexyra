// How long do a 1 ms Condvar::wait_timeout (with nobody notifying) and a
// 1 ms thread::sleep really take on this machine?
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

fn report(name: &str, mut v: Vec<f64>) {
    v.sort_by(f64::total_cmp);
    let p = |q: f64| v[((q * v.len() as f64).ceil() as usize).clamp(1, v.len()) - 1];
    println!(
        "{name}: n={} min={:.3} p50={:.3} p90={:.3} p99={:.3} max={:.3} ms",
        v.len(), v[0], p(0.5), p(0.9), p(0.99), v[v.len() - 1]
    );
}

fn main() {
    let m = Mutex::new(());
    let c = Condvar::new();
    let mut waits = Vec::new();
    for _ in 0..300 {
        let g = m.lock().unwrap();
        let t = Instant::now();
        let _ = c.wait_timeout_while(g, Duration::from_millis(1), |_| true).unwrap();
        waits.push(t.elapsed().as_secs_f64() * 1e3);
    }
    report("Condvar::wait_timeout_while(1 ms)", waits);
    let mut sleeps = Vec::new();
    for _ in 0..300 {
        let t = Instant::now();
        std::thread::sleep(Duration::from_millis(1));
        sleeps.push(t.elapsed().as_secs_f64() * 1e3);
    }
    report("thread::sleep(1 ms)", sleeps);
}
