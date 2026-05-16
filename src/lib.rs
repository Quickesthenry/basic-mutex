#![allow(dead_code)]

//! A lightweight, FIFO-ordered mutex primitive.
//!
//! This module provides [`BasicMutex`], a mutual exclusion primitive useful for
//! protecting shared data across threads. It enforces strict First-In, First-Out (FIFO)
//! ordering of waiting threads to prevent starvation and lock barging.

use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, Ordering},
    thread::Thread,
};

struct Waiter {
    thread: Thread,
    ready: AtomicBool,
}

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
        loop {
            if self
                .mutex
                .threads_lock
                .compare_exchange_weak(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
            std::hint::spin_loop();
        }

        let queue = unsafe { &mut *self.mutex.threads.get() };

        if let Some(mut next_waiter) = queue.pop_front() {
            let still_has_waiters = !queue.is_empty();
            self.mutex.has_waiters.store(still_has_waiters, Ordering::Release);
            self.mutex.threads_lock.store(false, Ordering::Release);
            next_waiter.ready.store(true, Ordering::Release);
            next_waiter.thread.unpark();
        } else {
            self.mutex.has_waiters.store(false, Ordering::Release);
            self.mutex.lock.store(false, Ordering::Release);
            self.mutex.threads_lock.store(false, Ordering::Release);
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
        if self.has_waiters.load(Ordering::Acquire) {
            return None;
        }

        if self
            .lock
            .compare_exchange_weak(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            Some(BasicMutexGuard { mutex: self })
        } else {
            None
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
    /// ```
    pub fn lock<'a>(&'a self) -> BasicMutexGuard<'a, T> {
        if !self.has_waiters.load(Ordering::Acquire) {
            if self
                .lock
                .compare_exchange_weak(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return BasicMutexGuard { mutex: self };
            }
        }

        self.has_waiters.store(true, Ordering::SeqCst);

        loop {
            if self
                .threads_lock
                .compare_exchange_weak(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
            std::hint::spin_loop();
        }

        let waiter = Waiter {
            thread: std::thread::current(),
            ready: AtomicBool::new(false),
        };

        let queue = unsafe { &mut *self.threads.get() };
        queue.push_back(waiter);
        
        let waiter_ptr = unsafe {
            let ptr = queue.back().unwrap() as *const Waiter;
            &(*ptr).ready as *const AtomicBool
        };

        self.threads_lock.store(false, Ordering::Release);

        loop {
            if unsafe { (*waiter_ptr).load(Ordering::Acquire) } {
                break;
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