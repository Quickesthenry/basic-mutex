use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use std::sync::{Arc, Mutex as StdMutex};
use parking_lot::Mutex as PlMutex;
use basic_mutex::BasicMutex;

/// Benchmarks std::sync::Mutex try_lock overhead.
fn bench_std_try_lock(c: &mut Criterion) {
    let mutex = Arc::new(StdMutex::new(0u64));
    c.bench_function("std_mutex_try_lock_uncontended", |b| {
        b.iter(|| {
            // In an uncontended scenario, try_lock should almost always succeed.
            // We loop to measure the cost of the atomic operation + guard creation.
            for _ in 0..10_000 {
                if let Ok(mut guard) = mutex.try_lock() {
                    *guard += 1;
                    black_box(*guard);
                }
            }
        });
    });
}

/// Benchmarks parking_lot::Mutex try_lock overhead.
/// parking_lot is known for extremely fast uncontended paths.
fn bench_pl_try_lock(c: &mut Criterion) {
    let mutex = Arc::new(PlMutex::new(0u64));
    c.bench_function("parking_lot_try_lock_uncontended", |b| {
        b.iter(|| {
            for _ in 0..10_000 {
                if let Some(mut guard) = mutex.try_lock() {
                    *guard += 1;
                    black_box(*guard);
                }
            }
        });
    });
}

/// Benchmarks basic_mutex try_lock overhead.
/// This reveals the cost of your specific atomic/state-check implementation.
fn bench_basic_try_lock(c: &mut Criterion) {
    let mutex = Arc::new(BasicMutex::new(0u64));
    c.bench_function("basic_mutex_try_lock_uncontended", |b| {
        b.iter(|| {
            for _ in 0..10_000 {
                // Adjust 'Some' to 'Ok' if your API returns Result
                if let Some(mut guard) = mutex.try_lock() { 
                    *guard += 1;
                    black_box(*guard);
                }
            }
        });
    });
}

criterion_group!(
    benches, 
    bench_std_try_lock, 
    bench_pl_try_lock, 
    bench_basic_try_lock
);
criterion_main!(benches);