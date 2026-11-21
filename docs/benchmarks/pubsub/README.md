# Pub/Sub fan-out

One publisher, many subscribers to one channel: how many messages get through, how long delivery takes, and what it costs ordinary clients meanwhile.

## How it was measured

[`tools/rexyra-fanout`](../../../tools/rexyra-fanout) connects N subscribers, then publishes 64-byte messages to their channel for 20 seconds, pipelined 32 at a time and as fast as the server takes them, unless a rate is given. Every message carries its sequence number and the time it was sent, so each subscriber checks that it received every message, in order, and records the delivery latency of every 16th. Meanwhile one more client sends `GET`s one at a time, 1 ms apart, and records their latency. A fresh server was started for each run.

```powershell
cargo build --release -p rexyra-server -p rexyra-fanout
target\release\rexyra-server.exe --port 6406 --save '""'
target\release\rexyra-fanout.exe --port 6406 --subscribers 1000 --seconds 20 --out result.json
```

Machine: AMD Ryzen 9 7900X (12 cores, 24 threads), 64 GB, Windows 11. Server and benchmark ran on the same machine, so they compete for the processor. Rexyra at commit `a5d4fc8` (branch `feature/pubsub`), release build, default settings. Raw results: [raw](raw).

## Results

| Subscribers | Published/s | Deliveries/s | Delivery latency p50 / p99 / max | `GET` latency meanwhile p50 / p99 | Server memory (RSS) |
|---:|---:|---:|---|---|---:|
| 1 | 347,759 | 344,079 | 0.07 / 0.31 / 3.7 ms | 0.05 / 0.36 ms | 8 MB |
| 10 | 159,607 | 1,579,154 | 0.22 / 0.33 / 1.7 ms | 0.10 / 0.21 ms | 8 MB |
| 100 | 29,944 | 2,962,997 | 1.38 / 2.03 / 4.8 ms | 0.60 / 1.28 ms | 13 MB |
| 1,000 | 3,256 | 3,219,073 | 13.8 / 27.1 / 46.5 ms | 6.1 / 17.7 ms | 39 MB |

In every run every subscriber received every message, in the order published; none was disconnected, and every `PUBLISH` reported all N receivers. After the publisher stopped, the last messages arrived within 0.25 s.

### 1,000 subscribers at a steadier rate

Publishing flat out keeps the single command thread busy all the time, which is what the `GET` latency in the last row shows. Held to 1,000 messages a second, still a million deliveries a second:

| Publisher | Published/s | Delivery latency p50 / p99 | `GET` latency p50 / p99 |
|---|---:|---|---|
| bursts of 32 | 999 | 11.8 / 17.0 ms | 2.0 / 11.0 ms |
| one message at a time | 247 (could not keep up) | 7.4 / 35.8 ms | 5.4 / 35.4 ms |

Sent one at a time, each message to 1,000 subscribers takes about 4 ms from `PUBLISH` to its reply, so the publisher falls short of the rate. Redis behaves the same way in this setup (below), so this is mostly the cost of 1,000 separate socket writes and reads per message on one machine, not something particular to Rexyra.

## Compared with Redis

The same runs against Redis 7.4.12 in Docker (`redis:7-alpine`), 1,000 subscribers. Redis's traffic goes through Docker's network forwarding, which costs it a good deal, so this is no fair contest; it shows the shape, not who is faster.

| 1,000 subscribers | Rexyra | Redis 7.4 (Docker) |
|---|---:|---:|
| Flat out: deliveries/s | 3,219,073 | 1,673,935 |
| Flat out: delivery latency p50 / p99 | 13.8 / 27.1 ms | 27.4 / 39.7 ms |
| One at a time: published/s | 247 | 119 |
| One at a time: delivery latency p50 / p99 | 7.4 / 35.8 ms | 15.2 / 23.6 ms |

## What it costs in processor time

The I/O runtime starts one worker thread per logical processor, 24 here. Measured over 5 seconds of the 1,000-subscriber runs, and repeated with fewer workers (`TOKIO_WORKER_THREADS`):

| Workers | Flat out: deliveries/s | Server CPU | One at a time: server CPU |
|---:|---:|---:|---:|
| 24 (default) | 3.2 million | 8.4 cores | 17.2 cores |
| 4 | 3.5 million | 3.9 cores | (run failed to connect all subscribers) |
| 2 | 3.0 million | 2.5 cores | 2.0 cores |

With 24 workers, most of that processor time is spent by idle workers looking for work each time a delivery wakes one of them, not by delivering. Four workers delivered as much using under half the processor time. Worth acting on: see the roadmap.

## Not covered

- Pattern subscriptions, and many channels at once.
- Large messages, and many publishers at once.
- A slow subscriber: covered by a test instead (`a_subscriber_that_does_not_read_is_dropped_at_its_limit` in `crates/server/tests/pubsub.rs`), which checks that one that never reads is dropped at the 32 MB limit without holding up the publisher.
