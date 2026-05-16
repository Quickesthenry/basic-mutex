# basic_mutex [![CI](https://github.com/Quickesthenry/basic-mutex/actions/workflows/rust.yml/badge.svg)](https://github.com/Quickesthenry/basic-mutex/actions/workflows/rust.yml) [![Crates.io](https://img.shields.io/crates/v/basic-mutex.svg)](https://crates.io/crates/basic-mutex)

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

Use this crate when fairness and predictability are more important than raw throughput.

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