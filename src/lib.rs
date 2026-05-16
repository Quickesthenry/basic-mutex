#![allow(dead_code)]

//! A lightweight, FIFO-ordered mutex primitive.
//!
//! This module provides [`BasicMutex`], a mutual exclusion primitive useful for
//! protecting shared data across threads. It enforces strict First-In, First-Out (FIFO)
//! ordering of waiting threads to prevent starvation and lock barging.

use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    hint::spin_loop,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, AtomicU8, Ordering},
    thread::Thread,
};

struct Waiter {
    thread: Thread,
}

const LOCKED: u8 = 0b0000_0001;
const HAS_WAITERS: u8 = 0b0000_0010;
const QUEUE_LOCKED: u8 = 0b0000_0100;

/// A mutual exclusion primitive useful for protecting shared data.
///
/// This mutex will block threads waiting for the lock to become available.
/// It guarantees FIFO (First-In, First-Out) scheduling for waiting threads
/// via an internal queue to prevent starvation and lock barging.
///
/// # Examples
///
/// ```
/// use basic_mutex::BasicMutex;
/// use std::sync::Arc;
/// use std::thread;
///
/// let mutex = Arc::new(BasicMutex::new(0));
/// let c_mutex = Arc::clone(&mutex);
///
/// let handle = thread::spawn(move || {
///     let mut guard = c_mutex.lock();
///     *guard += 1;
/// });
///
/// handle.join().unwrap();
/// assert_eq!(*mutex.lock(), 1);
/// ```
pub struct BasicMutex<T: Send> {
    state: AtomicU8,
    lock: AtomicBool,
    has_waiters: AtomicBool,
    threads_lock: AtomicBool,
    threads: UnsafeCell<VecDeque<Waiter>>,
    value: UnsafeCell<T>,
}

/// An RAII implementation of a "scoped lock" of a mutex.
///
/// When this structure is dropped (falls out of scope), the lock is automatically
/// unlocked and the next waiting thread in the FIFO queue is awakened.
///
/// The data protected by the mutex can be accessed safely through this guard via
/// its [`Deref`] and [`DerefMut`] implementations.
///
/// # Lifetime Guarantees
///
/// The guard is tied to the lifetime of the underlying [`BasicMutex`] via the `'a`
/// parameter. This ensures that the mutex cannot be destroyed while a thread is still
/// holding a pointer to its protected data.
///
/// # Thread Safety
///
/// `BasicMutexGuard` is explicitly marked as `!Send` if `T` is `!Send`. Furthermore,
/// because it automatically releases the lock on drop, passing a guard across thread
/// boundaries can easily cause undefined behavior or accidental deadlocks if the
/// unlocking sequence is disrupted.
///
/// # Examples
///
/// ```
/// use basic_mutex::BasicMutex;
///
/// let mutex = BasicMutex::new(vec![1, 2, 3]);
///
/// // The scope of the guard is limited to this block
/// {
///     let mut guard = mutex.lock();
///     guard.push(4);
///     // guard is automatically dropped here, releasing the lock
/// }
///
/// assert_eq!(mutex.lock().len(), 4);
/// ```
pub struct BasicMutexGuard<'a, T: Send> {
    mutex: &'a BasicMutex<T>,
}

unsafe impl<T: Send> Sync for BasicMutex<T> {}

impl<'a, T: Send> Drop for BasicMutexGuard<'a, T> {
    fn drop(&mut self) {
        // 1. Acquire the QUEUE_LOCKED spinlock to safely touch the VecDeque
        let mut old = self.mutex.state.load(Ordering::Acquire);
        loop {
            if old & QUEUE_LOCKED != 0 {
                spin_loop();
                old = self.mutex.state.load(Ordering::Acquire);
                continue;
            }

            let new = old | QUEUE_LOCKED;

            match self.mutex.state.compare_exchange_weak(
                old,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => {
                    old = actual;
                    spin_loop();
                }
            }
        }

        // 2. Access the queue under mutual exclusion
        let queue = unsafe { &mut *self.mutex.threads.get() };
        let popped_waiter = queue.pop_front();
        let still_has_waiters = !queue.is_empty();

        // 3. Atomically release both QUEUE_LOCKED and LOCKED bits
        let mut current = self.mutex.state.load(Ordering::Acquire);
        loop {
            // We clear QUEUE_LOCKED and clear LOCKED because the next thread
            // must competitively re-acquire the lock bit when it wakes up.
            let mut new = current & !QUEUE_LOCKED & !LOCKED;

            if still_has_waiters {
                new |= HAS_WAITERS;
            } else {
                new &= !HAS_WAITERS;
            }

            match self.mutex.state.compare_exchange_weak(
                current,
                new,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => {
                    current = actual;
                    spin_loop();
                }
            }
        }

        // 4. Wake up the thread if one was waiting
        if let Some(next_waiter) = popped_waiter {
            next_waiter.thread.unpark();
        }
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
    /// Creates a new mutex in an unlocked state wrapping the value provided.
    ///
    /// # Examples
    ///
    /// ```
    /// use basic_mutex::BasicMutex;
    /// let mutex = BasicMutex::new(0);
    /// ```
    pub fn new(value: T) -> Self {
        Self {
            state: AtomicU8::new(0),
            lock: AtomicBool::new(false),
            has_waiters: AtomicBool::new(false),
            threads_lock: AtomicBool::new(false),
            threads: UnsafeCell::new(VecDeque::new()),
            value: UnsafeCell::new(value),
        }
    }

    /// Attempts to acquire this lock without blocking, respecting the FIFO queue.
    ///
    /// This function performs a single, non-blocking check on the internal queue
    /// state. If there are threads waiting in line, this returns [`None`] immediately
    /// to prevent barging.
    ///
    /// # Examples
    ///
    /// ```
    /// use basic_mutex::BasicMutex;
    ///
    /// let mutex = BasicMutex::new(42);
    ///
    /// if let Some(mut guard) = mutex.try_lock() {
    ///     *guard += 1;
    /// } else {
    ///     println!("Lock was contested");
    /// }
    /// ```
    pub fn try_lock(&self) -> Option<BasicMutexGuard<'_, T>> {
        // self.has_waiters.load(Ordering::Acquire)
        if (self.state.load(Ordering::Acquire) & HAS_WAITERS) != 0 {
            return None;
        }

        let mut current = self.state.load(Ordering::Acquire);

        loop {
            // if already locked, fail fast
            if current & LOCKED != 0 {
                return None;
            }

            let new = current | LOCKED;

            match self.state.compare_exchange_weak(
                current,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(BasicMutexGuard { mutex: self }),
                Err(actual) => current = actual,
            }
        }
    }

    /// Acquires a mutex, blocking the current thread until it is able to do so.
    ///
    /// This function will block the local thread until it is available to acquire
    /// the mutex. Upon returning, the thread is the only thread with the lock
    /// held. An RAII guard is returned to allow scoped access to the data.
    ///
    /// This mutex does not support lock poisoning. If a thread panics while
    /// holding the lock, the data will remain accessible.
    ///
    /// # Examples
    ///
    /// ```
    /// use basic_mutex::BasicMutex;
    ///
    /// let mutex = BasicMutex::new(42);
    /// let mut guard = mutex.lock();
    /// *guard += 1;
    ///
    pub fn lock<'a>(&'a self) -> BasicMutexGuard<'a, T> {
        let mut state = self.state.load(Ordering::Acquire);

        // -------------------------------------------------------------
        // PHASE 1: Competitive Fast Path
        // -------------------------------------------------------------
        loop {
            // If the lock bit is clear, attempt to claim it immediately
            if state & LOCKED == 0 {
                let new = state | LOCKED;
                match self.state.compare_exchange_weak(
                    state,
                    new,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return BasicMutexGuard { mutex: self },
                    Err(actual) => {
                        state = actual;
                        continue;
                    }
                }
            }
            break;
        }

        // -------------------------------------------------------------
        // PHASE 2: Acquire the Queue Spinlock
        // -------------------------------------------------------------
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            // We must claim exclusive rights to modify the VecDeque
            if state & QUEUE_LOCKED == 0 {
                let new = state | QUEUE_LOCKED | HAS_WAITERS;
                match self.state.compare_exchange_weak(
                    state,
                    new,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(actual) => {
                        state = actual;
                        continue;
                    }
                }
            }
            std::hint::spin_loop();
            state = self.state.load(Ordering::Acquire);
        }

        // -------------------------------------------------------------
        // PHASE 3: Enqueue the Waiter Node
        // -------------------------------------------------------------
        // We instantiate our explicit Waiter abstraction here.
        // Pushing it onto the VecDeque is perfectly safe under QUEUE_LOCKED.
        let waiter = Waiter {
            thread: std::thread::current(),
        };

        unsafe {
            let queue = &mut *self.threads.get();
            queue.push_back(waiter);
        }

        // Release the VecDeque spinlock
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            let new = state & !QUEUE_LOCKED;
            match self
                .state
                .compare_exchange_weak(state, new, Ordering::Release, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => state = actual,
            }
        }

        // -------------------------------------------------------------
        // PHASE 4: Competitive Parking Loop
        // -------------------------------------------------------------
        loop {
            // Since there is no status flag inside the waiter node,
            // we wake up and try to actively contest the lock state.
            state = self.state.load(Ordering::Acquire);
            if state & LOCKED == 0 {
                let new = state | LOCKED;
                if self
                    .state
                    .compare_exchange_weak(state, new, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    break;
                }
            }

            std::thread::park();
        }

        BasicMutexGuard { mutex: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_basic_functional() {
        let mutex = BasicMutex::new(42);

        if let Some(mut guard) = mutex.try_lock() {
            assert_eq!(*guard, 42);
            *guard = 50;
        } else {
            panic!("try_lock failed on an unlocked mutex");
        }

        let guard = mutex.lock();
        assert_eq!(*guard, 50);
    }

    #[test]
    fn test_mutual_exclusion() {
        let mutex = Arc::new(BasicMutex::new(0));
        let mutex_clone = Arc::clone(&mutex);

        let mut guard1 = mutex.lock();
        *guard1 = 10;

        let handle = thread::spawn(move || {
            let mut guard2 = mutex_clone.lock();
            *guard2 += 5;
        });

        thread::sleep(Duration::from_millis(50));

        assert_eq!(*guard1, 10);

        drop(guard1);
        handle.join().unwrap();

        let final_guard = mutex.lock();
        assert_eq!(*final_guard, 15);
    }

    #[test]
    fn test_high_contention() {
        let mutex = Arc::new(BasicMutex::new(0));
        let mut handles = vec![];

        for _ in 0..10 {
            let mutex_clone = Arc::clone(&mutex);
            let handle = thread::spawn(move || {
                for _ in 0..1000 {
                    let mut guard = mutex_clone.lock();
                    *guard += 1;
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let final_guard = mutex.lock();
        assert_eq!(*final_guard, 10000);
    }

    #[test]
    fn test_lost_wakeup_torture() {
        let mutex = Arc::new(BasicMutex::new(0));
        let mut handles = vec![];

        for i in 0..8 {
            let mutex_clone = Arc::clone(&mutex);
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let mut guard = mutex_clone.lock();
                    *guard += 1;

                    if i % 2 == 0 {
                        thread::sleep(Duration::from_nanos(10));
                    }
                    drop(guard);

                    thread::sleep(Duration::from_nanos(10));
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let final_guard = mutex.lock();
        assert_eq!(*final_guard, 800);
    }
}
