# basic_mutex [![CI](https://github.com/Quickesthenry/basic-mutex/actions/workflows/rust.yml/badge.svg)](https://github.com/Quickesthenry/basic-mutex/actions/workflows/rust.yml) [![Crates.io](https://img.shields.io/crates/v/basic-mutex.svg)](https://crates.io/crates/basic-mutex) [![CodeRabbit Pull Request Reviews](https://img.shields.io/coderabbit/prs/github/Quickesthenry/basic-mutex?utm_source=oss&utm_medium=github&utm_campaign=Quickesthenry%2Fbasic-mutex&labelColor=171717&color=FF570A&link=https%3A%2F%2Fcoderabbit.ai&label=CodeRabbit+Reviews)](https://coderabbit.ai)

A lightweight, fair, FIFO-ordered mutex for Rust.

`basic_mutex` provides a minimal mutual exclusion primitive designed to guarantee predictable scheduling and starvation-free locking. Unlike many standard mutex implementations that may allow lock barging, this crate enforces strict First-In, First-Out (FIFO) ordering of waiting threads.

## Features

- FIFO fairness — threads acquire the lock in arrival order  
- Hybrid locking strategy — spinning, exponential backoff, and OS parking  
- Starvation-free design  
- Lightweight and dependency-free  
- RAII-based API for safe access to protected data  

## Installation

Add this to your Cargo.toml:

    [dependencies]
    basic_mutex = "0.1"

## Usage

Basic example:

    use basic_mutex::BasicMutex;
    use std::sync::Arc;
    use std::thread;

    let mutex = Arc::new(BasicMutex::new(0));
    let c_mutex = Arc::clone(&mutex);

    let handle = thread::spawn(move || {
        let mut guard = c_mutex.lock();
        *guard += 1;
    });

    handle.join().unwrap();

    assert_eq!(*mutex.lock(), 1);

Scoped locking:

    use basic_mutex::BasicMutex;

    let mutex = BasicMutex::new(vec![1, 2, 3]);

    {
        let mut guard = mutex.lock();
        guard.push(4);
    }

    assert_eq!(mutex.lock().len(), 4);

## How it works

`BasicMutex` uses a hybrid synchronization strategy:

1. Fast path: attempts immediate lock acquisition  
2. Spin phase: short busy-wait with exponential backoff  
3. Queueing: threads are placed into a FIFO queue  
4. Parking: threads sleep using the OS scheduler  
5. Wake-up: the next thread is explicitly unparked on unlock  

This balances low latency under light contention with CPU efficiency under heavy contention, while preserving fairness.

## Trade-offs

- Lower throughput than unfair mutexes in some workloads  
- Additional overhead from maintaining a queue  
- Not intended for lock-free or ultra-low-latency scenarios
- The Lock including 3 other items aside from the main value  

Use this crate when fairness and predictability are more important than raw throughput.

## Performance

`basic-mutex` is designed for **fairness**, not raw maximum throughput. It enforces strict FIFO ordering to prevent thread starvation, which introduces measurable overhead compared to unfair mutexes that allow "lock barging."

### Benchmark Results (Illustrative)
*Environment: 4 threads, high contention, Windows/Linux (results vary by hardware).*

| Implementation | Avg. Time | Fairness Model |
| :--- | :--- | :--- |
| `parking_lot::Mutex` | ~163 µs | Unfair (Barging allowed) |
| `std::sync::Mutex` | ~162 µs | Unfair (Barging allowed) |
| **`basic-mutex`** | **~195 µs** | **Strict FIFO (Starvation-free)** |

> **Note:** The ~20% difference observed here represents the cost of maintaining a fair queue and preventing overtaking. In low-contention scenarios, this gap narrows significantly. In high-contention scenarios, `basic-mutex` provides predictable latency at the expense of total throughput.

### Why choose `basic-mutex`?
*   **Prevent Starvation:** In unfair mutexes, a "fast" thread can repeatedly steal the lock from waiting threads. `basic-mutex` guarantees that every thread gets its turn in order.
*   **Predictable Latency:** Because threads are served in order, worst-case wait times are bounded and predictable, which is critical for real-time or responsive systems.
*   **Hybrid Efficiency:** It uses exponential backoff spinning for short waits and OS-level parking for long waits, balancing CPU usage with responsiveness.

Use `basic-mutex` when **correctness and fairness** are more important than squeezing out the last few nanoseconds of throughput.

## Safety

- Uses `UnsafeCell` internally for interior mutability  
- Safe access enforced via RAII (`BasicMutexGuard`)  
- No poisoning: the mutex remains usable after a panic  

## Testing

The crate includes tests for:

- Basic functionality  
- Mutual exclusion  
- High contention scenarios  
- Lost wakeup detection  

Run tests with:

    cargo test

## License

[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)

## Contributing

Contributions and feedback are welcome. Open an issue or submit a pull request.

## Notes

This crate is suitable for:

- Learning and experimentation with synchronization primitives  
- Workloads requiring strict fairness guarantees  
- Exploring low-level concurrency patterns in Rust  

It is not a drop-in replacement for `std::sync::Mutex` in all cases.