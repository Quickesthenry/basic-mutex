# basic-mutex

[![CI](https://github.com/Quickesthenry/basic-mutex/actions/workflows/rust.yml/badge.svg)](https://github.com/Quickesthenry/basic-mutex/actions/workflows/rust.yml)
[![Crates.io](https://img.shields.io/crates/v/basic-mutex.svg)](https://crates.io/crates/basic-mutex)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A high-performance, **FIFO-ordered** mutex for Rust.

`basic-mutex` provides a **FIFO-ordered** synchronization primitive. Threads waiting on `lock()` acquire it in strict enqueue order — eliminating starvation *for queued waiters* under contention. Unlike standard mutexes which may allow "lock barging," `BasicMutex` ensures predictable scheduling without sacrificing significant performance.

## 🚀 Key Features

*   **FIFO Ordering Under Contention:** Threads waiting in `lock()` are served in strict FIFO order, eliminating starvation for queued waiters.
*   **High Performance:** Competitive with `std::sync::Mutex` and `parking_lot` in uncontended scenarios (~38ns/lock).
*   **Hybrid Waiting Strategy:** Uses efficient CPU spinning for short waits and OS-level parking for long waits.
*   **Zero Dependencies:** Built entirely on `std::sync::atomic`. No external crates required.
*   **Safe & Ergonomic:** RAII-based API (`BasicMutexGuard`) ensures locks are always released.

## 📦 Installation

Add the crate to your project using cargo:

    cargo add basic-mutex

## 🛠️ Usage

### Basic Locking

    use basic_mutex::BasicMutex;
    use std::sync::Arc;
    use std::thread;

    let counter = Arc::new(BasicMutex::new(0));
    let mut handles = vec![];

    for _ in 0..10 {
        let counter_clone = Arc::clone(&counter);
        let handle = thread::spawn(move || {
            let mut guard = counter_clone.lock();
            *guard += 1;
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(*counter.lock(), 10);

### Try Lock

    use basic_mutex::BasicMutex;

    let mutex = BasicMutex::new(42);

    // Attempt to lock without blocking
    if let Some(mut guard) = mutex.try_lock() {
        *guard = 100;
        println!("Value updated to: {}", *guard);
    } else {
        println!("Lock was already held.");
    }

## ⚖️ Fairness vs. Throughput

Most standard mutexes (like `std::sync::Mutex` or `parking_lot::Mutex`) are **unfair**. This means a thread trying to lock might "barge" in ahead of threads that have been waiting longer. This maximizes throughput but can lead to **starvation** under high contention.

`basic-mutex` is **fair**. It maintains an internal queue of waiters. When the lock is released, it is handed off to the next thread in line.

### When to use `basic-mutex`:

*   You need to guarantee that requests are processed in order (e.g., transaction processing).
*   You want to prevent starvation *for queued waiters* under high contention.
*   You prefer predictable latency over raw maximum throughput.

### When to use `std::sync::Mutex` or `parking_lot`:

*   You need the absolute highest possible throughput under low contention.
*   Lock ordering does not matter for your application logic.

## ⚙️ How It Works

`BasicMutex` uses a single `AtomicU8` as a compact state machine, replacing the previous design that used three separate `AtomicBool` fields. All synchronization state is packed into four bitflags:

| Flag           | Bit mask   | Meaning                                                    |
| -------------- | ---------- | ---------------------------------------------------------- |
| `LOCKED`       | `0b0000_0001` | The mutex is currently held by a thread.                |
| `HAS_WAITERS`  | `0b0000_0010` | At least one thread is queued and waiting.              |
| `QUEUE_LOCKED` | `0b0000_0100` | A thread is currently modifying the waiter queue.       |
| `WOKEN`        | `0b0000_1000` | The front-of-queue waiter has been signaled to wake up. |

The `lock()` path operates in three phases:

1. **Fast path:** If no bits are set, atomically set `LOCKED` and return immediately.
2. **Enqueue:** Acquire the `QUEUE_LOCKED` spinlock, push a `Waiter` onto the internal `VecDeque`, set `HAS_WAITERS`, then release `QUEUE_LOCKED`.
3. **Wait loop:** Spin briefly (up to 100 iterations), then park the OS thread. On wake, attempt to claim the lock via `try_claim_lock`, which verifies the thread is at the front of the queue before popping it and setting `LOCKED`.

On `drop` of `BasicMutexGuard`, the holder acquires `QUEUE_LOCKED`, peeks at the next waiter, atomically clears `LOCKED` (and sets `WOKEN` + `HAS_WAITERS` if a waiter exists), then unparks that thread.

## 📊 Performance

`basic-mutex` is optimized for low overhead. In uncontended benchmarks (single-threaded lock/unlock cycles), it performs within ~20% of highly optimized system mutexes.

**Benchmark Provenance:**
- Environment: Windows x86_64
- Command: `cargo test test_comparative_performance -- --nocapture`
- Parameters: 100,000 iterations per operation, single-threaded uncontended test

| Implementation     | Lock (ns) | Try Lock (ns) | Fairness |
| ------------------ | --------- | ------------- | -------- |
| `std::sync::Mutex` | ~30.10    | ~34.35        | Unfair   |
| `parking_lot`      | ~29.92    | ~38.01        | Unfair   |
| `basic-mutex`      | ~38.10    | ~38.61        | **FIFO** |

*(Benchmarks run on Windows x86_64, 100,000 iterations, uncontended)*

> **Note:** Under heavy contention, `basic-mutex` may show slightly lower throughput than unfair mutexes due to the overhead of maintaining the FIFO queue and context switching. However, it guarantees that no thread will wait indefinitely.

## 🧪 Testing & Correctness

This crate includes a rigorous test suite covering:
*   **Mutual Exclusion:** Ensuring data races are impossible.
*   **FIFO Ordering:** Verifying that FIFO-style behavior is observed under contention.
*   **Lost Wakeup Torture:** Stress-testing the hybrid spin/park mechanism under extreme contention.
*   **Reentrancy Checks:** Ensuring deadlocks are handled as expected.

Run the tests with:

    cargo test

Run the performance benchmark with:

    cargo test test_comparative_performance -- --nocapture

## 🤝 Contributing

Contributions are welcome! Areas of interest include:
*   Further optimization of the atomic state machine.
*   Additional stress tests for edge cases on different OS schedulers.
*   Documentation improvements.

Please open an issue or PR to discuss major changes.

## 📄 License

Licensed under the [Apache License, Version 2.0](LICENSE).