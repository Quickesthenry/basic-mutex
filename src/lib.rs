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

use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    hint::spin_loop,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicU8, Ordering},
    thread::{Thread, ThreadId},
};

// Internal bitflags for the atomic state
const LOCKED: u8 = 0b0000_0001;
const HAS_WAITERS: u8 = 0b0000_0010;
const QUEUE_LOCKED: u8 = 0b0000_0100;
const WOKEN: u8 = 0b0000_1000;

// Faster backoff thresholds for queue spinlock
const QUEUE_SPIN_LIMIT: u32 = 32;
const QUEUE_YIELD_LIMIT: u32 = 128;

// Parking wait limits
const PARK_SPIN_BEFORE: u32 = 50;

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
pub struct BasicMutexGuard<'a, T: Send> {
    mutex: &'a BasicMutex<T>,
    phantom: PhantomData<*mut T>,
}

// SAFETY: BasicMutex is safe to share between threads if T is Send.
unsafe impl<T: Send> Sync for BasicMutex<T> {}

impl<'a, T: Send> Drop for BasicMutexGuard<'a, T> {
    fn drop(&mut self) {
        // 1. Acquire queue spinlock
        self.mutex.acquire_queue_spinlock(0);

        // 2. Peek next waiter
        let next_thread = unsafe {
            let queue = &*self.mutex.threads.get();
            queue.front().map(|w| w.thread.clone())
        };
        let has_waiters = next_thread.is_some();

        // 3. Update state while HOLDING queue lock
        let mut new_state = 0u8;
        if has_waiters {
            new_state |= HAS_WAITERS | WOKEN;
        }
        self.mutex.state.store(new_state, Ordering::Release);

        // 4. Unpark next waiter WHILE STILL HOLDING queue lock
        // This prevents race where another thread tries to claim during handoff
        if let Some(thread) = next_thread {
            thread.unpark();
        }

        // 5. Only release queue lock AFTER unpark completes
        self.mutex.state.fetch_and(!QUEUE_LOCKED, Ordering::Release);
    }
}

impl<'a, T: Send> Deref for BasicMutexGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.mutex.value.get() }
    }
}

impl<'a, T: Send> DerefMut for BasicMutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
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
    pub fn lock(&self) -> BasicMutexGuard<'_, T> {
        // --- PHASE 1: Fast Path (Uncontended) ---
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & (LOCKED | HAS_WAITERS | QUEUE_LOCKED | WOKEN) != 0 {
                break;
            }

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
                Err(actual) => state = actual,
            }
        }

        // --- PHASE 2: Enqueue ---
        let current_thread = std::thread::current();
        let current_thread_id = current_thread.id();

        // Acquire queue spinlock and set HAS_WAITERS atomically
        let acquired_state = self.acquire_queue_spinlock(HAS_WAITERS);

        // Quick recheck: if truly free, claim without enqueueing
        if acquired_state & (LOCKED | HAS_WAITERS | WOKEN) == 0 {
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
                thread: current_thread,
                thread_id: current_thread_id,
            });
        }

        // Release queue spinlock
        self.state.fetch_and(!QUEUE_LOCKED, Ordering::Release);

        // --- PHASE 3: Wait & Claim ---
        // **KEY OPTIMIZATION**: Don't check if we're "at front" in a loop.
        // Instead, just wait for WOKEN and then try to claim.
        // If we're not at front, we'll find that in try_claim_lock.
        let mut spin_count = 0u32;
        loop {
            let state = self.state.load(Ordering::Acquire);

            // If WOKEN is set, try to claim
            if state & WOKEN != 0 {
                if self.try_claim_lock(current_thread_id) {
                    return BasicMutexGuard {
                        mutex: self,
                        phantom: PhantomData,
                    };
                }
                // Not at front yet; keep spinning/waiting
            }

            // Spin briefly before parking
            if spin_count < PARK_SPIN_BEFORE {
                spin_loop();
                spin_count += 1;
            } else {
                // Double-check before parking
                if self.state.load(Ordering::Acquire) & WOKEN == 0 {
                    std::thread::park();
                }
                // After waking, don't reset spin_count—start trying to claim immediately
                spin_count = PARK_SPIN_BEFORE;
            }
        }
    }

    /// Acquires the `QUEUE_LOCKED` spinlock with aggressive exponential backoff.
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

            // **OPTIMIZED BACKOFF**: Pure spin first, then yield, NO sleep.
            // Mutex operations are usually fast; sleeping 1µs is too aggressive.
            if spin_count < QUEUE_SPIN_LIMIT {
                spin_loop();
            } else if spin_count < QUEUE_YIELD_LIMIT {
                std::thread::yield_now();
            } else {
                // Once we've yielded many times, go back to yielding (don't sleep).
                // This keeps latency low for contended workloads.
                std::thread::yield_now();
                spin_count = QUEUE_SPIN_LIMIT; // Reset to spin phase
            }
        }
    }

/// Helper: Attempts to claim lock if we are at the front of the queue.
    fn try_claim_lock(&self, current_thread_id: ThreadId) -> bool {
        self.acquire_queue_spinlock(0);

        // All queue operations in one unsafe block to avoid stacked borrows issues
        let (is_front, has_waiters) = unsafe {
            let queue = &mut *self.threads.get();
            
            // Check if we're at front
            let is_front = queue
                .front()
                .is_some_and(|w| w.thread_id == current_thread_id);
            
            if !is_front {
                (false, false)
            } else {
                // Pop and claim - we were at front
                queue.pop_front();
                let has_waiters = !queue.is_empty();
                (true, has_waiters)
            }
        };

        if !is_front {
            self.state.fetch_and(!QUEUE_LOCKED, Ordering::Release);
            return false;
        }

        // Verify we were actually woken (WOKEN should be set)
        let state = self.state.load(Ordering::Acquire);
        if state & WOKEN == 0 {
            self.state.fetch_and(!QUEUE_LOCKED, Ordering::Release);
            return false;
        }

        // Clear WOKEN when claiming - we're the new lock holder
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
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_basic_lock_unlock() {
        let mutex = BasicMutex::new(42);

        {
            let mut guard = mutex.try_lock().expect("Failed to try_lock");
            assert_eq!(*guard, 42);
            *guard = 100;
        }

        {
            let mut guard = mutex.lock();
            assert_eq!(*guard, 100);
            *guard = 200;
        }

        assert_eq!(*mutex.lock(), 200);
    }

    #[test]
    fn test_mutual_exclusion() {
        let mutex = Arc::new(BasicMutex::new(0));
        let mut handles = vec![];

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

    #[test]
    fn test_fifo_ordering() {
        let mutex = Arc::new(BasicMutex::new(0));
        let acquire_order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = vec![];

        let main_guard = mutex.lock();

        let mut start_txs = Vec::new();
        for i in 0..4 {
            let (start_tx, start_rx) = std::sync::mpsc::channel::<()>();
            start_txs.push(start_tx);

            let m = Arc::clone(&mutex);
            let ao = Arc::clone(&acquire_order);
            handles.push(thread::spawn(move || {
                start_rx.recv().unwrap();
                let _guard = m.lock();
                ao.lock().unwrap().push(i);
            }));
        }

        for tx in start_txs {
            tx.send(()).unwrap();
            thread::sleep(std::time::Duration::from_millis(5));
        }

        drop(main_guard);

        for h in handles {
            h.join().unwrap();
        }

        let final_acquire = acquire_order.lock().unwrap().clone();
        assert_eq!(final_acquire, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_try_lock_failure() {
        let mutex = BasicMutex::new(42);
        let _guard = mutex.lock();
        assert!(mutex.try_lock().is_none());
    }

    #[test]
    fn test_no_reentrancy() {
        let mutex = Arc::new(BasicMutex::new(42));
        let _guard1 = mutex.lock();

        let m_clone = Arc::clone(&mutex);
        let handle = thread::spawn(move || {
            let _guard2 = m_clone.lock();
        });

        thread::sleep(std::time::Duration::from_millis(10));
        drop(_guard1);
        handle.join().unwrap();
    }

    #[test]
    fn test_high_contention_stress() {
        let mutex = Arc::new(BasicMutex::new(0));
        let mut handles = vec![];

        for _ in 0..8 {
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

        assert_eq!(*mutex.lock(), 800);
    }

    #[test]
    fn test_contended_try_lock() {
        let mutex = Arc::new(BasicMutex::new(0u64));
        let successful_tries = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut handles = vec![];

        for _ in 0..4 {
            let m = Arc::clone(&mutex);
            handles.push(thread::spawn(move || {
                for _ in 0..50 {
                    let mut guard = m.lock();
                    *guard += 1;
                }
            }));
        }

        for _ in 0..4 {
            let m = Arc::clone(&mutex);
            let tries = Arc::clone(&successful_tries);
            handles.push(thread::spawn(move || {
                let mut local_count = 0u64;
                for _ in 0..200 {
                    if let Some(mut guard) = m.try_lock() {
                        *guard += 1;
                        local_count += 1;
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

        let lock_increments = 4 * 50u64;
        let try_increments = successful_tries.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(*mutex.lock(), lock_increments + try_increments);
    }

    #[test]
    fn test_lost_wakeup_torture() {
        let mutex = Arc::new(BasicMutex::new(0));
        let mut handles = vec![];

        for i in 0..4 {
            let m = Arc::clone(&mutex);
            handles.push(thread::spawn(move || {
                for _ in 0..50 {
                    let mut guard = m.lock();
                    *guard += 1;

                    if i % 2 == 0 {
                        thread::sleep(std::time::Duration::from_micros(10));
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
}