use basic_mutex::BasicMutex;
use criterion::{Criterion, criterion_group, criterion_main};
use parking_lot::Mutex as PlMutex;
use std::hint::black_box;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier, atomic::AtomicBool};

fn bench_parking_lot_mutex(c: &mut Criterion) {
    let mutex = Arc::new(PlMutex::new(0u64));
    let n_threads = 4;
    let n_iterations_per_thread = 1000;

    let start_barrier = Arc::new(Barrier::new(n_threads + 1));
    let done_barrier = Arc::new(Barrier::new(n_threads + 1));
    let stop = Arc::new(AtomicBool::new(false));

    // Spawn the threads
    let mut handles = vec![];
    for _ in 0..n_threads {
        let m = Arc::clone(&mutex);
        let start = Arc::clone(&start_barrier);
        let done = Arc::clone(&done_barrier);
        let stop = Arc::clone(&stop);
        handles.push(std::thread::spawn(move || {
            loop {
                start.wait();
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                for _ in 0..n_iterations_per_thread {
                    let mut guard = m.lock();
                    *guard += 1;
                    black_box(*guard);
                }
                done.wait();
            }
        }));
    }

    // Benchmark: each iteration of b.iter is one epoch (start and done barrier)
    c.bench_function("parking_lot_mutex_4_threads", |b| {
        b.iter(|| {
            start_barrier.wait();
            done_barrier.wait();
        })
    });

    // Signal the threads to stop
    stop.store(true, Ordering::Relaxed);
    start_barrier.wait(); // Wake up the threads so they can check the stop flag

    // Join the threads
    for h in handles {
        h.join().unwrap();
    }
}

fn bench_std_mutex(c: &mut Criterion) {
    let mutex = Arc::new(std::sync::Mutex::new(0u64));
    let n_threads = 4;
    let n_iterations_per_thread = 1000;

    let start_barrier = Arc::new(Barrier::new(n_threads + 1));
    let done_barrier = Arc::new(Barrier::new(n_threads + 1));
    let stop = Arc::new(AtomicBool::new(false));

    // Spawn the threads
    let mut handles = vec![];
    for _ in 0..n_threads {
        let m = Arc::clone(&mutex);
        let start = Arc::clone(&start_barrier);
        let done = Arc::clone(&done_barrier);
        let stop = Arc::clone(&stop);
        handles.push(std::thread::spawn(move || {
            loop {
                start.wait();
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                for _ in 0..n_iterations_per_thread {
                    let mut guard = m.lock().unwrap();
                    *guard += 1;
                    black_box(*guard);
                }
                done.wait();
            }
        }));
    }

    // Benchmark: each iteration of b.iter is one epoch (start and done barrier)
    c.bench_function("std_mutex_4_threads", |b| {
        b.iter(|| {
            start_barrier.wait();
            done_barrier.wait();
        })
    });

    // Signal the threads to stop
    stop.store(true, Ordering::Relaxed);
    start_barrier.wait(); // Wake up the threads so they can check the stop flag

    // Join the threads
    for h in handles {
        h.join().unwrap();
    }
}

fn bench_basic_mutex(c: &mut Criterion) {
    let mutex = Arc::new(BasicMutex::new(0u64));
    let n_threads = 4;
    let n_iterations_per_thread = 1000;

    let start_barrier = Arc::new(Barrier::new(n_threads + 1));
    let done_barrier = Arc::new(Barrier::new(n_threads + 1));
    let stop = Arc::new(AtomicBool::new(false));

    // Spawn the threads
    let mut handles = vec![];
    for _ in 0..n_threads {
        let m = Arc::clone(&mutex);
        let start = Arc::clone(&start_barrier);
        let done = Arc::clone(&done_barrier);
        let stop = Arc::clone(&stop);
        handles.push(std::thread::spawn(move || {
            loop {
                start.wait();
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                for _ in 0..n_iterations_per_thread {
                    let mut guard = m.lock();
                    *guard += 1;
                    black_box(*guard);
                }
                done.wait();
            }
        }));
    }

    // Benchmark: each iteration of b.iter is one epoch (start and done barrier)
    c.bench_function("basic_mutex_4_threads", |b| {
        b.iter(|| {
            start_barrier.wait();
            done_barrier.wait();
        })
    });

    // Signal the threads to stop
    stop.store(true, Ordering::Relaxed);
    start_barrier.wait(); // Wake up the threads so they can check the stop flag

    // Join the threads
    for h in handles {
        h.join().unwrap();
    }
}

criterion_group!(
    benches,
    bench_std_mutex,
    bench_basic_mutex,
    bench_parking_lot_mutex
);
criterion_main!(benches);
