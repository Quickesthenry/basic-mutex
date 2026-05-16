#![allow(dead_code)]

//! A lightweight, FIFO-ordered mutex primitive.
//!
//! This module provides [`BasicMutex`], a mutual exclusion primitive useful for
//! protecting shared data across threads. It uses a hybrid approach of spinning
//! with exponential backoff before parking threads using the OS scheduler.

use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, Ordering},
    thread::Thread,
};
/// A mutual exclusion primitive useful for protecting shared data.
///
/// This mutex will block threads waiting for the lock to become available.
/// It guarantees FIFO (First-In, First-Out) scheduling for waiting threads
/// via an internal queue to prevent starvation.
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
///
pub struct BasicMutex<T: Send> {
    lock: AtomicBool,
    threads_lock: AtomicBool,
    threads: UnsafeCell<VecDeque<Thread>>,
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
///
pub struct BasicMutexGuard<'a, T: Send> {
    mutex: &'a BasicMutex<T>,
}

unsafe impl<T: Send> Sync for BasicMutex<T> {}

/// # Thread Synchronization Lifecycle
///
/// The `Drop` implementation handles the critical transition of releasing the
/// primary lock boolean and waking up the next thread in the FIFO queue.
impl<'a, T: Send> Drop for BasicMutexGuard<'a, T> {
    /// Drops the `BasicMutexGuard`, unlocking the underlying mutex.
    ///
    /// This will release the primary lock and unpark the next thread waiting
    /// in the FIFO queue, if one exists.
    fn drop(&mut self) {
        // Get VecDeque to unpark
        let mut spin_count = 0;
        loop {
            if self
                .mutex
                .threads_lock
                .compare_exchange_weak(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }

            if spin_count < 10 {
                std::hint::spin_loop();
                spin_count += 1;
            } else if spin_count < 20 {
                let delay = 1 << (spin_count - 10);
                for _ in 0..delay {
                    std::hint::spin_loop();
                }
                spin_count += 1;
            } else {
                std::thread::yield_now();
                spin_count = 0;
            }
        }

        let queue = unsafe { &mut *self.mutex.threads.get() };
        if !queue.is_empty() {
            let thread = match queue.pop_front() {
                Some(thread) => thread,
                None => unreachable!("Impossible!"),
            };
            thread.unpark();
        }

        self.mutex.lock.store(false, Ordering::Release);
        self.mutex.threads_lock.store(false, Ordering::Release);
    }
}

/// # RAII Safety Integration
///
/// This implementation ensures that the protected data cannot outlive the lock
/// acquisition context, passing underlying immutable pointers directly to the caller.
impl<'a, T: Send> Deref for BasicMutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe {
            &*self.mutex.value.get()
        }
    }
}

/// # RAII Safety Integration
///
/// This implementation allows exclusive, mutable access to the underlying data
/// through the guard. The borrow checker ensures that only one mutable reference
impl<'a, T: Send> DerefMut for BasicMutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            &mut *self.mutex.value.get()
        }
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
    ///
    pub fn new(value: T) -> Self {
        Self {
            lock: AtomicBool::new(false),
            threads_lock: AtomicBool::new(false),
            threads: UnsafeCell::new(VecDeque::new()),
            value: UnsafeCell::new(value),
        }
    }
    /// Attempts to acquire this lock without blocking, respecting the FIFO queue.
    ///
    /// This function performs a single, non-blocking check on the internal queue
    /// state. If the metadata lock is currently held by another thread or if there
    /// are threads waiting in line, this returns [`None`] immediately to minimize
    /// blocking time.
    pub fn try_lock(&self) -> Option<BasicMutexGuard<'_, T>> {
        // 1. Attempt to acquire the spinlock exactly ONCE.
        // If another thread holds it, bail out immediately to guarantee minimal overhead.
        if self
            .threads_lock
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            // 2. Safely inspect the queue while holding the metadata spinlock
            let queue = unsafe { &mut *self.threads.get() };

            if !queue.is_empty() {
                // Threads are waiting in line; we must not barge.
                self.threads_lock.store(false, Ordering::Release);
                return None;
            }

            // 3. The queue is empty. Try to claim the actual main lock.
            let success = self
                .lock
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok();

            // 4. Always release the metadata spinlock before returning
            self.threads_lock.store(false, Ordering::Release);

            if success {
                Some(BasicMutexGuard { mutex: self })
            } else {
                None
            }
        } else {
            // Spinlock was contested; exit instantly
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
    pub fn lock<'a>(&'a self) -> BasicMutexGuard<'a, T> {
        if let Some(guard) = self.try_lock() {
            return guard;
        }
        loop {
            // Phase 1: Try to acquire the main mutex lock immediately.
            // If it's free, we win and can exit right away.
            if self
                .lock
                .compare_exchange_weak(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }

            // Phase 2: We failed to get the main lock.
            // We must now acquire the internal spinlock to safely touch the VecDeque.
            let mut spin_count = 0;
            loop {
                if self
                    .threads_lock
                    .compare_exchange_weak(false, true, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }

                if spin_count < 10 {
                    std::hint::spin_loop();
                    spin_count += 1;
                } else if spin_count < 20 {
                    let delay = 1 << (spin_count - 10);
                    for _ in 0..delay {
                        std::hint::spin_loop();
                    }
                    spin_count += 1;
                } else {
                    std::thread::yield_now();
                    spin_count = 0;
                }
            }

            // Phase 3: We are safely inside the spinlock.
            // CRITICAL DOUBLE-CHECK: Did the thread holding the main lock release it
            // while we were busy spinning for the threads_lock?
            if self
                .lock
                .compare_exchange_weak(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                // It became free! Release the spinlock and exit the loop.
                self.threads_lock.store(false, Ordering::Release);
                break;
            }

            // Phase 4: The main lock is definitely still held.
            // It is now safe to queue ourselves up for parking.
            let queue = unsafe { &mut *self.threads.get() };
            queue.push_back(std::thread::current());

            // Phase 5: Release the internal spinlock BEFORE we go to sleep.
            // If we forgot this, nobody could ever access the queue to wake us up!
            self.threads_lock.store(false, Ordering::Release);

            // Phase 6: Deep sleep. The thread will halt here until the
            // unlocking thread calls `unpark()` on our handle.
            std::thread::park();

            // When we wake up, we loop back to the top to try and claim the main lock again.
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

    // 1. Basic Sanity Test
    // Validates that the mutex initializes, locks, unlocks, and mutates data correctly
    // in a single-threaded environment.
    #[test]
    fn test_basic_functional() {
        let mutex = BasicMutex::new(42);

        // Test try_lock
        if let Some(mut guard) = mutex.try_lock() {
            assert_eq!(*guard, 42);
            *guard = 50;
        } else {
            panic!("try_lock failed on an unlocked mutex");
        }

        // Test normal lock and deref
        let guard = mutex.lock();
        assert_eq!(*guard, 50);
    }

    // 2. Mutual Exclusion Test
    // Ensures that two threads absolutely cannot hold the lock at the same time.
    #[test]
    fn test_mutual_exclusion() {
        let mutex = Arc::new(BasicMutex::new(0));
        let mutex_clone = Arc::clone(&mutex);

        let mut guard1 = mutex.lock();
        *guard1 = 10;

        let handle = thread::spawn(move || {
            // This should block until guard1 is dropped
            let mut guard2 = mutex_clone.lock();
            *guard2 += 5;
        });

        // Give the spawned thread a brief moment to attempt to grab the lock
        thread::sleep(Duration::from_millis(50));

        // The value should still be 10 because handle is blocked
        assert_eq!(*guard1, 10);

        // Dropping guard1 allows the spawned thread to make progress
        drop(guard1);
        handle.join().unwrap();

        // Final verify
        let final_guard = mutex.lock();
        assert_eq!(*final_guard, 15);
    }

    // 3. High-Contention Stress Test
    // Spawns 10 threads doing massive, rapid mutations. If there is a data race or
    // a lock-barging fault, the final sum will be completely incorrect.
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

    // 4. Concurrency Torture Test (Lost Wakeup Finder)
    // Introduces deliberate, microscopic, randomized delays right around the lock
    // boundaries. This shuffles the OS thread schedules violently, making it highly
    // effective at exposing lost wakeups or deadlocks.
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

                    // Micro-sleep while holding the lock to back up the queue
                    if i % 2 == 0 {
                        thread::sleep(Duration::from_nanos(10));
                    }
                    drop(guard);

                    // Micro-sleep after dropping to let other threads barge/interleave
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
