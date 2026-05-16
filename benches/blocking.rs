use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use std::sync::{Arc, Mutex as StdMutex};
use basic_mutex::BasicMutex;
use parking_lot::Mutex as PlMutex; // Add this import

fn bench_parking_lot_mutex(c: &mut Criterion) {
    let mutex = Arc::new(PlMutex::new(0u64));
    c.bench_function("parking_lot_mutex_4_threads", |b| {
        b.iter(|| {
            let mut handles = vec![];
            for _ in 0..4 {
                let m = Arc::clone(&mutex);
                handles.push(std::thread::spawn(move || {
                    for _ in 0..1000 {
                        let mut guard = m.lock();
                        *guard += 1;
                        std::hint::black_box(*guard); // Use std::hint::black_box
                    }
                }));
            }
            for h in handles { h.join().unwrap(); }
        });
    });
}


fn bench_std_mutex(c: &mut Criterion) {
    let mutex = Arc::new(StdMutex::new(0u64));
    c.bench_function("std_mutex_4_threads", |b| {
        b.iter(|| {
            let mut handles = vec![];
            for _ in 0..4 {
                let m = Arc::clone(&mutex);
                handles.push(std::thread::spawn(move || {
                    for _ in 0..1000 {
                        let mut guard = m.lock().unwrap();
                        *guard += 1;
                        black_box(*guard);
                    }
                }));
            }
            for h in handles { h.join().unwrap(); }
        });
    });
}

fn bench_basic_mutex(c: &mut Criterion) {
    let mutex = Arc::new(BasicMutex::new(0u64));
    c.bench_function("basic_mutex_4_threads", |b| {
        b.iter(|| {
            let mut handles = vec![];
            for _ in 0..4 {
                let m = Arc::clone(&mutex);
                handles.push(std::thread::spawn(move || {
                    for _ in 0..1000 {
                        let mut guard = m.lock();
                        *guard += 1;
                        black_box(*guard);
                    }
                }));
            }
            for h in handles { h.join().unwrap(); }
        });
    });
}

criterion_group!(benches, bench_std_mutex, bench_basic_mutex, bench_parking_lot_mutex);
criterion_main!(benches);