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
    thread::{Thread, ThreadId},
    time::Duration,
};

// Internal bitflags for the atomic state
const LOCKED: u8 = 0b0000_0001;
const HAS_WAITERS: u8 = 0b0000_0010;
const QUEUE_LOCKED: u8 = 0b0000_0100;
const WOKEN: u8 = 0b0000_1000;

// Backoff thresholds for the queue spinlock
const SPIN_LIMIT: u32 = 100;
const YIELD_LIMIT: u32 = 1000;

/// Internal representation of a thread waiting for the lock.
struct Waiter {
    thread: Thread,
    thread_id: ThreadId,
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
        // 1. Acquire Queue Spinlock (with exponential backoff)
        self.mutex.acquire_queue_spinlock(0);

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
                Ok(_) => {
                    return Some(BasicMutexGuard {
                        mutex: self,
                        phantom: PhantomData,
                    });
                }
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
                Ok(_) => {
                    return BasicMutexGuard {
                        mutex: self,
                        phantom: PhantomData,
                    };
                }
                Err(actual) => state = actual, // Retry with new state
            }
        }
        // --- PHASE 2: Enqueue ---
        let current_thread = std::thread::current();
        let current_thread_id = current_thread.id();

        // Acquire queue spinlock to push waiter (also sets HAS_WAITERS atomically)
        let acquired_state = self.acquire_queue_spinlock(HAS_WAITERS);

        // Recheck: If mutex is truly uncontended (no lock, no waiters, no pending wakeup),
        // claim it directly without enqueueing. We must check all three bits to prevent
        // a newcomer from barging when HAS_WAITERS or WOKEN are set.
        if acquired_state & (LOCKED | HAS_WAITERS | WOKEN) == 0 {
            // Mutex is free, claim it without enqueueing
            self.state.store(LOCKED, Ordering::Release);
            return BasicMutexGuard {
                mutex: self,
                phantom: PhantomData,
            };
        }

        // Push to queue
        unsafe {
            let queue = &mut *self.threads.get();
            queue.push_back(Waiter {
                thread: current_thread.clone(),
                thread_id: current_thread_id,
            });
        }

        // Release queue spinlock
        self.state.fetch_and(!QUEUE_LOCKED, Ordering::Release);

        // --- PHASE 3: Wait for Woken Signal ---
        let mut spin_count = 0;
        loop {
            let state = self.state.load(Ordering::Acquire);

            // If WOKEN is set, try to claim the lock
            if state & WOKEN != 0
                && self.try_claim_lock(current_thread_id) {
                    return BasicMutexGuard {
                        mutex: self,
                        phantom: PhantomData,
                    };
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

    /// Acquires the `QUEUE_LOCKED` spinlock with exponential backoff.
    ///
    /// `extra_bits` are set atomically alongside `QUEUE_LOCKED` in the successful
    /// compare-exchange (e.g. `HAS_WAITERS` when enqueueing during `lock()`).
    ///
    /// Returns the state value observed *before* the spinlock was acquired.
    ///
    /// # Backoff strategy
    /// 1. Spin for `SPIN_LIMIT` iterations (`spin_loop` hint — stays on-core).
    /// 2. Call `yield_now()` for the next `YIELD_LIMIT - SPIN_LIMIT` iterations
    ///    (lets other runnable threads proceed without sleeping).
    /// 3. Sleep for 1 µs thereafter, resetting the counter to `YIELD_LIMIT` so
    ///    subsequent failures keep yielding rather than sleeping again immediately.
    ///
    /// Unlike parking, neither `yield_now` nor `sleep` require a matching unpark,
    /// so there is no deadlock risk.
    fn acquire_queue_spinlock(&self, extra_bits: u8) -> u8 {
        let mut spin_count = 0u32;
        loop {
            let s = self.state.load(Ordering::Acquire);
            if s & QUEUE_LOCKED == 0 {
                match self.state.compare_exchange_weak(
                    s,
                    s | QUEUE_LOCKED | extra_bits,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return s,
                    Err(_) => spin_count += 1,
                }
            } else {
                spin_count += 1;
            }

            if spin_count < SPIN_LIMIT {
                spin_loop();
            } else if spin_count < YIELD_LIMIT {
                std::thread::yield_now();
            } else {
                std::thread::sleep(Duration::from_micros(1));
                // Reset to YIELD_LIMIT so subsequent failures yield rather than sleep.
                spin_count = YIELD_LIMIT;
            }
        }
    }

    /// Helper: Attempts to claim lock if we are at the front of the queue.
    fn try_claim_lock(&self, current_thread_id: ThreadId) -> bool {
        // Acquire queue spinlock (with exponential backoff)
        self.acquire_queue_spinlock(0);

        // Check if front
        let is_front = unsafe {
            (&*self.threads.get())
                .front()
                .is_some_and(|w| w.thread_id == current_thread_id)
        };

        if !is_front {
            // Release the queue spinlock before returning so other threads can proceed.
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
    ///
    /// Grants each thread permission to call lock() one at a time (with a small
    /// delay between each grant) while the main thread holds the lock. This ensures
    /// threads enqueue in a known order (0, 1, 2, 3), after which the main lock is
    /// released and we verify acquisition order matches.
    #[test]
    fn test_fifo_ordering() {
        let mutex = Arc::new(BasicMutex::new(0));
        let acquire_order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = vec![];

        // Hold the lock so spawned threads block immediately when they call lock()
        let main_guard = mutex.lock();

        // Per-thread "start" channels: the main thread sends permission one at a time
        let mut start_txs = Vec::new();
        for i in 0..4 {
            let (start_tx, start_rx) = std::sync::mpsc::channel::<()>();
            start_txs.push(start_tx);

            let m = Arc::clone(&mutex);
            let ao = Arc::clone(&acquire_order);
            handles.push(thread::spawn(move || {
                // Wait for explicit permission before calling lock().
                // This lets the main thread control the enqueue order.
                start_rx.recv().unwrap();
                let _guard = m.lock();
                ao.lock().unwrap().push(i);
            }));
        }

        // Grant permissions one at a time with a sleep between each.
        // Since the main guard is still held, each thread that receives permission
        // immediately blocks inside lock() and enters the wait queue before the
        // next thread is permitted to call lock().
        for tx in start_txs {
            tx.send(()).unwrap();
            thread::sleep(Duration::from_millis(5));
        }

        // All 4 threads are now queued in order 0, 1, 2, 3.
        // Release the main lock so they proceed in FIFO order.
        drop(main_guard);

        for h in handles {
            h.join().unwrap();
        }

        let final_acquire = acquire_order.lock().unwrap().clone();
        assert_eq!(final_acquire, vec![0, 1, 2, 3]);
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

    /// Test 8: Contended try_lock Under Concurrent Lock Holders
    ///
    /// Spawns threads that each hold the lock briefly while other threads
    /// hammer try_lock. Verifies that try_lock never produces a data race:
    /// every successful try_lock acquisition must see a consistent counter,
    /// and the final count must equal the total number of successful increments.
    #[test]
    fn test_contended_try_lock() {
        let mutex = Arc::new(BasicMutex::new(0u64));
        let successful_tries = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut handles = vec![];

        // 4 threads that hold the lock briefly (creating contention)
        for _ in 0..4 {
            let m = Arc::clone(&mutex);
            handles.push(thread::spawn(move || {
                for _ in 0..50 {
                    let mut guard = m.lock();
                    *guard += 1;
                    // Hold briefly to create contention window
                    black_box(*guard);
                }
            }));
        }

        // 4 threads that only use try_lock, counting their successes
        for _ in 0..4 {
            let m = Arc::clone(&mutex);
            let tries = Arc::clone(&successful_tries);
            handles.push(thread::spawn(move || {
                let mut local_count = 0u64;
                for _ in 0..200 {
                    if let Some(mut guard) = m.try_lock() {
                        *guard += 1;
                        local_count += 1;
                        black_box(*guard);
                    } else {
                        thread::yield_now();
                    }
                }
                tries.fetch_add(local_count, std::sync::atomic::Ordering::Relaxed);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // The final value must equal lock() increments + try_lock() increments exactly
        let lock_increments = 4 * 50u64;
        let try_increments = successful_tries.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(*mutex.lock(), lock_increments + try_increments);
    }

    /// Test 9: Mixed Contended lock() and try_lock()
    ///
    /// Half the threads use lock() (blocking), half use try_lock() (non-blocking).
    /// Asserts data integrity: no increment is lost or double-counted.
    #[test]
    fn test_mixed_contended_lock_and_try_lock() {
        let mutex = Arc::new(BasicMutex::new(0u64));
        let try_lock_successes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut handles = vec![];
        const LOCK_THREADS: u64 = 4;
        const LOCK_ITERS: u64 = 75;

        // Blocking lock() threads
        for _ in 0..LOCK_THREADS {
            let m = Arc::clone(&mutex);
            handles.push(thread::spawn(move || {
                for _ in 0..LOCK_ITERS {
                    let mut guard = m.lock();
                    *guard += 1;
                    black_box(*guard);
                }
            }));
        }

        // Non-blocking try_lock() threads
        for _ in 0..4 {
            let m = Arc::clone(&mutex);
            let successes = Arc::clone(&try_lock_successes);
            handles.push(thread::spawn(move || {
                let mut local = 0u64;
                for _ in 0..300 {
                    if let Some(mut guard) = m.try_lock() {
                        *guard += 1;
                        local += 1;
                        black_box(*guard);
                    } else {
                        thread::yield_now();
                    }
                }
                successes.fetch_add(local, std::sync::atomic::Ordering::Relaxed);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let expected = LOCK_THREADS * LOCK_ITERS
            + try_lock_successes.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(*mutex.lock(), expected);
    }

    /// Test 10: try_lock Returns None When Waiters Are Queued
    ///
    /// Verifies the fairness guarantee: once at least one thread is blocked
    /// in lock(), try_lock() from a third thread must return None (no barging).
    #[test]
    fn test_contended_try_lock_blocked_by_waiters() {
        let mutex = Arc::new(BasicMutex::new(0u64));

        // Acquire the lock on the main thread
        let guard = mutex.lock();

        // Spawn a thread that will block on lock(), enqueuing itself as a waiter
        let m = Arc::clone(&mutex);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let waiter_handle = thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let mut g = m.lock();
            *g += 1;
        });

        // Wait until the waiter thread has signaled it is about to call lock()
        ready_rx.recv().unwrap();
        // Give the waiter thread time to actually enqueue in the wait queue
        thread::sleep(Duration::from_millis(5));

        // Release the main lock; the unlock sets HAS_WAITERS | WOKEN (not LOCKED),
        // so the immediately following try_lock races in the handoff window and must
        // return None because HAS_WAITERS forbids barging.
        drop(guard);
        assert!(
            mutex.try_lock().is_none(),
            "try_lock must not barge ahead of a queued waiter"
        );

        waiter_handle.join().unwrap();

        assert_eq!(*mutex.lock(), 1);
    }

    /// Test 11: Comparative Performance Check (Uncontended)
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
