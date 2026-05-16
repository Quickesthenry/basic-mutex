# basic_mutex

[![CI](https://github.com/Quickesthenry/basic-mutex/actions/workflows/rust.yml/badge.svg)](https://github.com/Quickesthenry/basic-mutex/actions/workflows/rust.yml)
[![Crates.io](https://img.shields.io/crates/v/basic-mutex.svg)](https://crates.io/crates/basic-mutex)
[![CodeRabbit Pull Requests](https://img.shields.io/coderabbit/prs/github/Quickesthenry/basic-mutex?label=CodeRabbit+Reviews)](https://coderabbit.ai)

A lightweight, fair mutex for Rust with FIFO-style ordering under contention.

`basic_mutex` is a synchronization primitive designed for predictable scheduling and starvation-free locking behavior under contention. It prioritizes fairness over raw throughput.

---

## Key Features

* Fair locking behavior with FIFO-style queueing under contention
* Hybrid strategy: fast path, short spinning, queueing, and OS parking
* Starvation-resistant design
* RAII-based safe API
* No external dependencies

---

## Design Goals

This crate is built for situations where:

* Fairness matters more than maximum throughput
* Thread starvation must be avoided
* Lock acquisition order should be predictable under contention

FIFO behavior is enforced under the current implementation and validated under stress testing.

---

## Installation

```toml
[dependencies]
basic_mutex = "0.1"
```

---

## Basic Usage

```rust
use basic_mutex::BasicMutex;
use std::sync::Arc;
use std::thread;

fn main() {
    let mutex = Arc::new(BasicMutex::new(0));
    let m2 = Arc::clone(&mutex);

    let handle = thread::spawn(move || {
        let mut guard = m2.lock();
        *guard += 1;
    });

    handle.join().unwrap();

    assert_eq!(*mutex.lock(), 1);
}
```

---

## Scoped Usage

```rust
use basic_mutex::BasicMutex;

fn main() {
    let mutex = BasicMutex::new(vec![1, 2, 3]);

    {
        let mut guard = mutex.lock();
        guard.push(4);
    }

    assert_eq!(mutex.lock().len(), 4);
}
```

---

## How It Works

`BasicMutex` uses a hybrid synchronization strategy:

1. **Fast path** – attempt immediate lock acquisition
2. **Spinning phase** – short busy wait with backoff under light contention
3. **Queueing phase** – contending threads are placed into a FIFO-style wait queue
4. **Parking phase** – threads sleep via OS scheduler when contention persists
5. **Handoff wakeup** – next queued thread is explicitly unparked on unlock

This design balances latency under light contention with fairness and CPU efficiency under heavy contention.

---

## Fairness Model

Under contention, threads are served in arrival order as enforced by the internal queueing and handoff protocol.

The implementation is designed to prevent lock barging (where later threads bypass queued threads).

Stress tests are used to validate ordering behavior under concurrent load.

---

## Trade-offs

* Lower throughput than unfair mutexes in some workloads
* Additional overhead due to queue management and atomic coordination
* Not intended for ultra-low-latency or lock-free systems

---

## Performance

`basic_mutex` prioritizes fairness over maximum throughput.

### Representative Benchmark Results

*Highly dependent on hardware, OS scheduling, and contention pattern.*

| Implementation     | Avg Time | Fairness Model             |
| ------------------ | -------- | -------------------------- |
| parking_lot::Mutex | ~163 µs  | Unfair (barging allowed)   |
| std::sync::Mutex   | ~162 µs  | Unfair (barging allowed)   |
| basic_mutex        | ~195 µs  | Fair (FIFO-style ordering) |

Fairness introduces overhead but provides predictable ordering under contention.

---

## Why Use This Crate?

Use `basic_mutex` when:

* Thread starvation is unacceptable
* Predictable lock acquisition order is required
* You prefer correctness and fairness over peak throughput

---

## Safety

* Uses `UnsafeCell` internally for controlled interior mutability
* Safe API enforced via RAII guard (`BasicMutexGuard`)
* No poisoning: mutex remains usable after panics

---

## Testing

The crate includes tests for:

* Mutual exclusion correctness
* Contention behavior
* Wakeup correctness
* FIFO ordering under stress conditions

Run tests with:

```bash
cargo test
```

---

## Contributing

Contributions are welcome, especially in the following areas:

* Reducing internal atomic complexity (e.g. bitfield / `AtomicU64` design)
* Improving stress tests for FIFO correctness
* Documentation and visualization of the handoff protocol
* Performance tuning under contention

For significant changes, please open an issue first to discuss design constraints and correctness requirements.

---

## Notes

This crate is intended for:

* Learning and experimentation with concurrency primitives
* Fair scheduling experiments
* Workloads requiring strict ordering guarantees

It is not a drop-in replacement for `std::sync::Mutex` in all environments.
