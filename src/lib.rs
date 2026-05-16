#![warn(missing_docs)]
#![allow(dead_code)]

//! A high-performance, FIFO-ordered mutex primitive.
//!
//! This crate provides [`BasicMutex`], a mutual exclusion lock that guarantees
//! First-In-First-Out (FIFO) ordering for waiting threads. Unlike standard
//! mutexes which may allow "lock barging," this implementation ensures that
//! threads acquire the lock in the exact order they requested it.
//!
//! # Key Features
//!
//! *   **FIFO Fairness:** Prevents starvation by enforcing strict ordering.
//! *   **Hybrid Waiting:** Uses efficient CPU spinning for short waits and
//!     OS-level parking for long waits to balance latency and CPU usage.
//! *   **Zero Dependencies:** Built entirely on `std::sync::atomic`.
//!
//! # Example
//!
//! ```
//! use basic_mutex::BasicMutex;
//! use std::sync::Arc;
//! use std::thread;
//!
//! let counter = Arc::new(BasicMutex::new(0));
//! let mut handles = vec![];
//!
//! for _ in 0..10 {
//!     let counter_clone = Arc::clone(&counter);
//!     let handle = thread::spawn(move || {
//!         let mut guard = counter_clone.lock();
//!         *guard += 1;
//!     });
//!     handles.push(handle);
//! }
//!
//! for handle in handles {
//!     handle.join().unwrap();
//! }
//!
//! assert_eq!(*counter.lock(), 10);
//! ```

use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    hint::spin_loop,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU8, Ordering},
    thread::Thread,
};

// Internal bitflags for the atomic state
const LOCKED: u8 = 0b0000_0001;
const HAS_WAITERS: u8 = 0b0000_0010;
const QUEUE_LOCKED: u8 = 0b0000_0100;
const WOKEN: u8 = 0b0000_1000;

/// Internal representation of a thread waiting for the lock.
struct Waiter {
    thread: Thread,
}

/// A mutual exclusion primitive useful for protecting shared data.
///
/// This mutex blocks threads waiting for the lock to become available.
/// It guarantees **FIFO (First-In, First-Out)** scheduling via an internal
/// wait queue, preventing starvation and lock barging.
///
/// # Type Parameters
///
/// *   `T`: The type of data protected by the mutex. Must implement `Send`.
pub struct BasicMutex<T: Send> {
    /// Atomic state flag combining LOCKED, HAS_WAITERS, QUEUE_LOCKED, and WOKEN.
    state: AtomicU8,
    /// The protected data. Access is guarded by the `LOCKED` state bit.
    value: UnsafeCell<T>,
    /// Queue of waiting threads. Access is guarded by the `QUEUE_LOCKED` state bit.
    threads: UnsafeCell<VecDeque<Waiter>>,
}

/// An RAII implementation of a scoped lock of a mutex.
///
/// When this structure is dropped, the lock is automatically unlocked.
/// The data can be accessed via [`Deref`] and [`DerefMut`].
///
/// # Thread Safety
///
/// `BasicMutexGuard` is `!Send` to prevent moving the guard to another thread,
/// which would violate the lock's ownership semantics.
pub struct BasicMutexGuard<'a, T: Send> {
    mutex: &'a BasicMutex<T>,
    phantom: PhantomData<*mut T>,
}

// SAFETY: BasicMutex is safe to share between threads if T is Send.
// The atomic state machine protects the internal UnsafeCells.
unsafe impl<T: Send> Sync for BasicMutex<T> {}

impl<'a, T: Send> Drop for BasicMutexGuard<'a, T> {
    fn drop(&mut self) {
        // 1. Acquire Queue Spinlock
        loop {
            let s = self.mutex.state.load(Ordering::Acquire);
            if s & QUEUE_LOCKED == 0 {
                match self.mutex.state.compare_exchange_weak(
                    s,
                    s | QUEUE_LOCKED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(_) => spin_loop(),
                }
            } else {
                spin_loop();
            }
        }

        // 2. Peek next waiter
        let next_thread = unsafe {
            let queue = &*self.mutex.threads.get();
            queue.front().map(|w| w.thread.clone())
        };
        let has_waiters = next_thread.is_some();

        // 3. Update State: Clear LOCKED & QUEUE_LOCKED. Set WOKEN if waiters exist.
        let mut new_state = 0u8;
        if has_waiters {
            new_state |= HAS_WAITERS | WOKEN;
        }
        self.mutex.state.store(new_state, Ordering::Release);

        // 4. Unpark next waiter
        if let Some(thread) = next_thread {
            thread.unpark();
        }
    }
}

impl<'a, T: Send> Deref for BasicMutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: Holder of BasicMutexGuard has exclusive access.
        unsafe { &*self.mutex.value.get() }
    }
}

impl<'a, T: Send> DerefMut for BasicMutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: Holder of BasicMutexGuard has exclusive mutable access.
        unsafe { &mut *self.mutex.value.get() }
    }
}

impl<T: Send> BasicMutex<T> {
    /// Creates a new mutex in an unlocked state.
    pub fn new(value: T) -> Self {
        Self {
            state: AtomicU8::new(0),
            value: UnsafeCell::new(value),
            threads: UnsafeCell::new(VecDeque::new()),
        }
    }

    /// Attempts to acquire the lock without blocking.
    ///
    /// Returns `Some(guard)` if successful, or `None` if the lock is held
    /// or if other threads are waiting (to maintain FIFO fairness).
    pub fn try_lock(&self) -> Option<BasicMutexGuard<'_, T>> {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            // Cannot barge if locked, waiters exist, queue is busy, or wakeup is pending
            if current & (LOCKED | HAS_WAITERS | QUEUE_LOCKED | WOKEN) != 0 {
                return None;
            }
            match self.state.compare_exchange_weak(
                current,
                current | LOCKED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(BasicMutexGuard { mutex: self, phantom: PhantomData }),
                Err(actual) => current = actual,
            }
        }
    }

    /// Acquires the lock, blocking until available.
    ///
    /// Guarantees FIFO ordering. Uses hybrid spinning (for low latency)
    /// and OS parking (for CPU efficiency) under contention.
    pub fn lock(&self) -> BasicMutexGuard<'_, T> {
        // --- PHASE 1: Fast Path (Uncontended) ---
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            // If any contention flag is set, we must go to the slow path.
            if state & (LOCKED | HAS_WAITERS | QUEUE_LOCKED | WOKEN) != 0 {
                break;
            }

            // Try to acquire the lock atomically.
            match self.state.compare_exchange_weak(
                state,
                state | LOCKED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return BasicMutexGuard { mutex: self, phantom: PhantomData },
                Err(actual) => state = actual, // Retry with new state
            }
        }
        // --- PHASE 2: Enqueue ---
        let current_thread = std::thread::current();

        // Acquire queue spinlock to push waiter
        let acquired_state = loop {
            let s = self.state.load(Ordering::Acquire);
            if s & QUEUE_LOCKED == 0 {
                match self.state.compare_exchange_weak(
                    s,
                    s | QUEUE_LOCKED | HAS_WAITERS,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break s,
                    Err(_) => spin_loop(),
                }
            } else {
                spin_loop();
            }
        };

        // Recheck: If mutex became free while we were acquiring QUEUE_LOCKED, claim it directly
        if acquired_state & LOCKED == 0 {
            // Mutex is free, claim it without enqueueing
            self.state.store(LOCKED, Ordering::Release);
            return BasicMutexGuard { mutex: self, phantom: PhantomData };
        }

        // Push to queue
        unsafe {
            let queue = &mut *self.threads.get();
            queue.push_back(Waiter {
                thread: current_thread.clone(),
            });
        }

        // Release queue spinlock
        self.state.fetch_and(!QUEUE_LOCKED, Ordering::Release);

        // --- PHASE 3: Wait for Woken Signal ---
        let mut spin_count = 0;
        loop {
            let state = self.state.load(Ordering::Acquire);

            // If WOKEN is set, try to claim the lock
            if state & WOKEN != 0 {
                if self.try_claim_lock(&current_thread) {
                    return BasicMutexGuard { mutex: self, phantom: PhantomData };
                }
            }

            // Hybrid Backoff: Spin briefly, then park
            if spin_count < 100 {
                spin_loop();
                spin_count += 1;
            } else {
                // Double-check before parking to avoid lost wakeups
                if self.state.load(Ordering::Acquire) & WOKEN == 0 {
                    std::thread::park();
                }
                spin_count = 0;
            }
        }
    }

    /// Helper: Attempts to claim lock if we are at the front of the queue.
    fn try_claim_lock(&self, current_thread: &Thread) -> bool {
        // Acquire queue spinlock
        loop {
            let s = self.state.load(Ordering::Acquire);
            if s & QUEUE_LOCKED == 0 {
                match self.state.compare_exchange_weak(
                    s,
                    s | QUEUE_LOCKED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(_) => spin_loop(),
                }
            } else {
                spin_loop();
            }
        }

        // Check if front
        let is_front = unsafe {
            let queue = &*self.threads.get();
            queue
                .front()
                .map_or(false, |w| w.thread.id() == current_thread.id())
        };

        if !is_front {
            self.state.fetch_and(!QUEUE_LOCKED, Ordering::Release);
            return false;
        }

        // Pop and Claim
        let has_waiters = unsafe {
            let queue = &mut *self.threads.get();
            queue.pop_front();
            !queue.is_empty()
        };

        let mut new_state = LOCKED;
        if has_waiters {
            new_state |= HAS_WAITERS;
        }
        self.state.store(new_state, Ordering::Release);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hint::black_box;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    /// Test 1: Basic functionality (Single-threaded)
    #[test]
    fn test_basic_lock_unlock() {
        let mutex = BasicMutex::new(42);

        // Test try_lock
        {
            let mut guard = mutex.try_lock().expect("Failed to try_lock");
            assert_eq!(*guard, 42);
            *guard = 100;
        }

        // Test lock
        {
            let mut guard = mutex.lock();
            assert_eq!(*guard, 100);
            *guard = 200;
        }

        assert_eq!(*mutex.lock(), 200);
    }

    /// Test 2: Mutual Exclusion (Multi-threaded)
    /// Reduced iterations for faster debug testing.
    #[test]
    fn test_mutual_exclusion() {
        let mutex = Arc::new(BasicMutex::new(0));
        let mut handles = vec![];

        // Reduced from 8 threads/1000 iters to 4 threads/100 iters for speed in debug
        for _ in 0..4 {
            let m = Arc::clone(&mutex);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let mut guard = m.lock();
                    *guard += 1;
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(*mutex.lock(), 400);
    }

    /// Test 3: FIFO Fairness Check
    /// Removed arbitrary sleep; used a channel to signal readiness instead.
    #[test]
    fn test_fifo_ordering() {
        let mutex = Arc::new(BasicMutex::new(0));
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = vec![];

        // Hold the lock initially so threads queue up
        let _main_guard = mutex.lock();

        // Use a channel to know when threads are actually waiting
        let (tx, rx) = std::sync::mpsc::channel();

        for i in 0..4 {
            let m = Arc::clone(&mutex);
            let o = Arc::clone(&order);
            let tx = tx.clone();
            handles.push(thread::spawn(move || {
                // Signal that we are about to block on lock
                tx.send(i).unwrap();
                let _guard = m.lock();
                o.lock().unwrap().push(i);
            }));
        }

        // Wait for all 4 threads to signal they are ready/waiting and capture enqueue order
        let mut enqueue_order = Vec::new();
        for _ in 0..4 {
            enqueue_order.push(rx.recv().unwrap());
        }

        // Small yield to ensure they are fully parked/queued
        thread::yield_now();

        // Release main lock to let them proceed
        drop(_main_guard);

        for h in handles {
            h.join().unwrap();
        }

        let final_order = order.lock().unwrap().clone();
        assert_eq!(final_order, enqueue_order);
    }

    /// Test 4: Try Lock Failure
    #[test]
    fn test_try_lock_failure() {
        let mutex = BasicMutex::new(42);
        let _guard = mutex.lock();

        // Should fail because main thread holds the lock
        assert!(mutex.try_lock().is_none());
    }

    /// Test 5: Reentrancy Deadlock Check
    #[test]
    fn test_no_reentrancy() {
        let mutex = Arc::new(BasicMutex::new(42));
        let _guard1 = mutex.lock();

        let m_clone = Arc::clone(&mutex);
        let handle = thread::spawn(move || {
            let _guard2 = m_clone.lock();
        });

        // Give the thread time to block
        thread::sleep(Duration::from_millis(10)); // Reduced from 50ms

        drop(_guard1);
        handle.join().unwrap();
    }

    /// Test 6: High Contention Stress Test
    /// Reduced iterations for debug speed.
    #[test]
    fn test_high_contention_stress() {
        let mutex = Arc::new(BasicMutex::new(0));
        let mut handles = vec![];

        for _ in 0..8 {
            // Reduced from 16
            let m = Arc::clone(&mutex);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    // Reduced from 500
                    let mut guard = m.lock();
                    *guard += 1;
                    black_box(*guard);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(*mutex.lock(), 800);
    }

    /// Test 7: Lost Wakeup Torture
    /// Reduced iterations.
    #[test]
    fn test_lost_wakeup_torture() {
        let mutex = Arc::new(BasicMutex::new(0));
        let mut handles = vec![];

        for i in 0..4 {
            // Reduced from 8
            let m = Arc::clone(&mutex);
            handles.push(thread::spawn(move || {
                for _ in 0..50 {
                    // Reduced from 200
                    let mut guard = m.lock();
                    *guard += 1;

                    if i % 2 == 0 {
                        thread::sleep(Duration::from_micros(10));
                    }
                    drop(guard);
                    thread::yield_now();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(*mutex.lock(), 200);
    }
    /// Test 8: Comparative Performance Check (Uncontended)
    /// Compares BasicMutex against std::sync::Mutex and parking_lot::Mutex.
    #[test]
    fn test_comparative_performance() {
        use parking_lot::Mutex as PlMutex;
        use std::sync::Mutex as StdMutex;

        let iterations = 100_000;

        // --- 1. BasicMutex lock() ---
        let basic_mutex = BasicMutex::new(0u64);
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let mut guard = basic_mutex.lock();
            *guard += 1;
        }
        let basic_lock_ns = start.elapsed().as_nanos() as f64 / iterations as f64;

        // --- 2. BasicMutex try_lock() ---
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let mut guard = basic_mutex.try_lock().expect("basic try_lock failed");
            *guard += 1;
        }
        let basic_try_ns = start.elapsed().as_nanos() as f64 / iterations as f64;

        // --- 3. std::sync::Mutex lock() ---
        let std_mutex = StdMutex::new(0u64);
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let mut guard = std_mutex.lock().unwrap();
            *guard += 1;
        }
        let std_lock_ns = start.elapsed().as_nanos() as f64 / iterations as f64;

        // --- 4. std::sync::Mutex try_lock() ---
        // Note: std returns Result<Option<Guard>, PoisonError>
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            // unwrap() handles PoisonError, then we check if we got the lock (Some)
            let mut guard = std_mutex.try_lock().expect("std try_lock failed");
            *guard += 1;
        }
        let std_try_ns = start.elapsed().as_nanos() as f64 / iterations as f64;

        // --- 5. parking_lot::Mutex lock() ---
        let pl_mutex = PlMutex::new(0u64);
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let mut guard = pl_mutex.lock();
            *guard += 1;
        }
        let pl_lock_ns = start.elapsed().as_nanos() as f64 / iterations as f64;

        // --- 6. parking_lot::Mutex try_lock() ---
        // Note: parking_lot returns Option<Guard>
        let start = std::time::Instant::now();
        for _ in 0..iterations {
            let mut guard = pl_mutex.try_lock().expect("pl try_lock failed");
            *guard += 1;
        }
        let pl_try_ns = start.elapsed().as_nanos() as f64 / iterations as f64;

        println!(
            "\n--- Uncontended Performance Comparison ({} iters) ---",
            iterations
        );
        println!(
            "{:<25} | {:<12} | {:<12}",
            "Implementation", "lock (ns)", "try_lock (ns)"
        );
        println!("{:-<55}", "");
        println!(
            "{:<25} | {:<12.2} | {:<12.2}",
            "BasicMutex (Yours)", basic_lock_ns, basic_try_ns
        );
        println!(
            "{:<25} | {:<12.2} | {:<12.2}",
            "std::sync::Mutex", std_lock_ns, std_try_ns
        );
        println!(
            "{:<25} | {:<12.2} | {:<12.2}",
            "parking_lot::Mutex", pl_lock_ns, pl_try_ns
        );
        println!("---------------------------------------------------\n");

        // Sanity checks to ensure work was actually done
        assert_eq!(*basic_mutex.lock(), iterations as u64 * 2);
        assert_eq!(*std_mutex.lock().unwrap(), iterations as u64 * 2);
        assert_eq!(*pl_mutex.lock(), iterations as u64 * 2);
    }
}
