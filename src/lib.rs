//! In-process thread-based task queue with priority and concurrency control.
//!
//! This crate provides a simple task queue that runs closures on a pool of worker
//! threads. Tasks can be submitted with different priorities, and higher-priority
//! tasks are executed first.
//!
//! # Example
//!
//! ```
//! use philiprehberger_task_queue::{TaskQueue, Priority};
//!
//! let queue = TaskQueue::new(2);
//!
//! let handle = queue.submit(|| 1 + 1);
//! assert_eq!(handle.join().unwrap(), 2);
//!
//! let handle = queue.submit_with_priority(Priority::High, || "done");
//! assert_eq!(handle.join().unwrap(), "done");
//!
//! queue.shutdown();
//! ```

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

/// Task execution priority.
///
/// Higher-priority tasks are dequeued before lower-priority ones.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Priority {
    /// Lowest execution priority.
    Low,
    /// Default execution priority.
    Normal,
    /// Highest execution priority.
    High,
}

impl Priority {
    fn as_u8(self) -> u8 {
        match self {
            Priority::Low => 0,
            Priority::Normal => 1,
            Priority::High => 2,
        }
    }
}

impl Ord for Priority {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_u8().cmp(&other.as_u8())
    }
}

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Error returned when a task fails to produce a result.
#[derive(Debug)]
pub enum TaskError {
    /// The task panicked during execution.
    Panicked,
    /// The task was cancelled because the queue shut down before it could run.
    Cancelled,
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskError::Panicked => write!(f, "task panicked"),
            TaskError::Cancelled => write!(f, "task cancelled"),
        }
    }
}

impl std::error::Error for TaskError {}

/// A handle to a submitted task, used to retrieve the result.
///
/// # Example
///
/// ```
/// use philiprehberger_task_queue::TaskQueue;
///
/// let queue = TaskQueue::new(1);
/// let handle = queue.submit(|| 42);
/// assert_eq!(handle.join().unwrap(), 42);
/// queue.shutdown();
/// ```
pub struct TaskHandle<T> {
    inner: Arc<TaskResultSlot<T>>,
}

struct TaskResultSlot<T> {
    mutex: Mutex<Option<Result<T, TaskError>>>,
    condvar: Condvar,
}

impl<T> TaskResultSlot<T> {
    fn set(&self, value: Result<T, TaskError>) {
        let mut guard = self.mutex.lock().unwrap();
        *guard = Some(value);
        self.condvar.notify_one();
    }
}

impl<T> TaskHandle<T> {
    /// Block until the task completes and return its result.
    ///
    /// Returns `Ok(value)` if the task completed successfully, or a [`TaskError`]
    /// if the task panicked or was cancelled.
    pub fn join(self) -> Result<T, TaskError> {
        let mut guard = self.inner.mutex.lock().unwrap();
        while guard.is_none() {
            guard = self.inner.condvar.wait(guard).unwrap();
        }
        guard.take().unwrap()
    }

    /// Check whether the task has completed without blocking.
    pub fn is_done(&self) -> bool {
        self.inner.mutex.lock().unwrap().is_some()
    }
}

/// Guard that sets `TaskError::Cancelled` on the result slot when dropped,
/// unless the task has already completed. This ensures that `TaskHandle::join`
/// never blocks forever if the task is dropped without running.
struct CancelGuard<T> {
    slot: Arc<TaskResultSlot<T>>,
}

impl<T> Drop for CancelGuard<T> {
    fn drop(&mut self) {
        let mut guard = self.slot.mutex.lock().unwrap();
        if guard.is_none() {
            *guard = Some(Err(TaskError::Cancelled));
            self.slot.condvar.notify_one();
        }
    }
}

type BoxedTask = Box<dyn FnOnce() + Send>;

struct QueueEntry {
    priority: Priority,
    sequence: u64,
    task: BoxedTask,
}

impl Eq for QueueEntry {}

impl PartialEq for QueueEntry {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct SharedState {
    queue: BinaryHeap<QueueEntry>,
    shutdown: bool,
    next_sequence: u64,
}

/// A thread-based task queue with configurable concurrency and priority scheduling.
///
/// Workers continuously pull the highest-priority task from the queue and execute it.
/// When the queue is shut down, running tasks are allowed to complete but pending
/// tasks are dropped (their handles will receive `TaskError::Cancelled`).
///
/// # Example
///
/// ```
/// use philiprehberger_task_queue::{TaskQueue, Priority};
///
/// let queue = TaskQueue::new(2);
///
/// let h1 = queue.submit(|| 10);
/// let h2 = queue.submit_with_priority(Priority::High, || 20);
///
/// assert_eq!(h1.join().unwrap(), 10);
/// assert_eq!(h2.join().unwrap(), 20);
///
/// queue.shutdown();
/// ```
pub struct TaskQueue {
    shared: Arc<(Mutex<SharedState>, Condvar)>,
    workers: Option<Vec<thread::JoinHandle<()>>>,
}

impl TaskQueue {
    /// Create a new task queue with the given number of worker threads.
    ///
    /// # Panics
    ///
    /// Panics if `concurrency` is zero.
    pub fn new(concurrency: usize) -> Self {
        assert!(concurrency > 0, "concurrency must be at least 1");

        let shared = Arc::new((
            Mutex::new(SharedState {
                queue: BinaryHeap::new(),
                shutdown: false,
                next_sequence: 0,
            }),
            Condvar::new(),
        ));

        let mut workers = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let shared = Arc::clone(&shared);
            let handle = thread::spawn(move || {
                worker_loop(&shared);
            });
            workers.push(handle);
        }

        TaskQueue {
            shared,
            workers: Some(workers),
        }
    }

    /// Submit a task with `Normal` priority.
    ///
    /// Returns a [`TaskHandle`] that can be used to retrieve the result.
    pub fn submit<F, T>(&self, task: F) -> TaskHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        self.submit_with_priority(Priority::Normal, task)
    }

    /// Submit a task with the given priority.
    ///
    /// Higher-priority tasks are executed before lower-priority ones when
    /// multiple tasks are waiting in the queue.
    ///
    /// Returns a [`TaskHandle`] that can be used to retrieve the result.
    pub fn submit_with_priority<F, T>(&self, priority: Priority, task: F) -> TaskHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let slot = Arc::new(TaskResultSlot {
            mutex: Mutex::new(None),
            condvar: Condvar::new(),
        });

        let cancel_guard = CancelGuard {
            slot: Arc::clone(&slot),
        };

        let boxed: BoxedTask = Box::new(move || {
            // The cancel guard is moved into the closure. If the closure runs,
            // we explicitly set the result and then forget the guard so it
            // doesn't overwrite with Cancelled. If the closure is dropped without
            // running, the guard's Drop fires and sets Cancelled.
            let outcome = panic::catch_unwind(AssertUnwindSafe(task));
            let value = match outcome {
                Ok(v) => Ok(v),
                Err(_) => Err(TaskError::Panicked),
            };
            cancel_guard.slot.set(value);
            // Prevent the Drop impl from overwriting the result with Cancelled
            std::mem::forget(cancel_guard);
        });

        let (ref mutex, ref condvar) = *self.shared;
        let mut state = mutex.lock().unwrap();
        let sequence = state.next_sequence;
        state.next_sequence += 1;
        state.queue.push(QueueEntry {
            priority,
            sequence,
            task: boxed,
        });
        condvar.notify_one();

        TaskHandle { inner: slot }
    }

    /// Shut down the task queue.
    ///
    /// Signals all workers to stop, waits for currently running tasks to finish,
    /// and drops any pending tasks. Pending task handles will receive
    /// `TaskError::Cancelled` when joined.
    pub fn shutdown(mut self) {
        self.do_shutdown();
    }

    fn do_shutdown(&mut self) {
        let (ref mutex, ref condvar) = *self.shared;

        {
            let mut state = mutex.lock().unwrap();
            state.shutdown = true;
            condvar.notify_all();
            // Drain the queue — dropping each entry drops its closure, which
            // drops the CancelGuard, which sets TaskError::Cancelled on the slot.
            state.queue.clear();
        }

        if let Some(workers) = self.workers.take() {
            for w in workers {
                let _ = w.join();
            }
        }
    }
}

impl Drop for TaskQueue {
    fn drop(&mut self) {
        let (ref mutex, ref condvar) = *self.shared;
        {
            let mut state = mutex.lock().unwrap();
            if !state.shutdown {
                state.shutdown = true;
                state.queue.clear();
                condvar.notify_all();
            }
        }
        if let Some(workers) = self.workers.take() {
            for w in workers {
                let _ = w.join();
            }
        }
    }
}

fn worker_loop(shared: &(Mutex<SharedState>, Condvar)) {
    let (ref mutex, ref condvar) = *shared;
    loop {
        let task = {
            let mut state = mutex.lock().unwrap();
            loop {
                if let Some(entry) = state.queue.pop() {
                    break Some(entry.task);
                }
                if state.shutdown {
                    break None;
                }
                state = condvar.wait(state).unwrap();
            }
        };
        match task {
            Some(task) => task(),
            None => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::Barrier;
    use std::time::Duration;

    #[test]
    fn submit_and_join() {
        let queue = TaskQueue::new(1);
        let handle = queue.submit(|| 42);
        assert_eq!(handle.join().unwrap(), 42);
        queue.shutdown();
    }

    #[test]
    fn submit_multiple_tasks_all_complete() {
        let queue = TaskQueue::new(2);
        let handles: Vec<_> = (0..10).map(|i| queue.submit(move || i * 2)).collect();
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        for (i, r) in results.iter().enumerate() {
            assert_eq!(*r, i * 2);
        }
        queue.shutdown();
    }

    #[test]
    fn priority_ordering() {
        let queue = TaskQueue::new(1);
        let barrier = Arc::new(Barrier::new(2));
        let order = Arc::new(Mutex::new(Vec::new()));

        // Block the single worker
        let b = barrier.clone();
        queue.submit(move || {
            b.wait();
        });

        // Give the worker time to pick up the blocking task
        thread::sleep(Duration::from_millis(50));

        // Now submit tasks with different priorities — they'll queue up
        let o = order.clone();
        let h_low = queue.submit_with_priority(Priority::Low, move || {
            o.lock().unwrap().push("low");
        });

        let o = order.clone();
        let h_high = queue.submit_with_priority(Priority::High, move || {
            o.lock().unwrap().push("high");
        });

        let o = order.clone();
        let h_normal = queue.submit_with_priority(Priority::Normal, move || {
            o.lock().unwrap().push("normal");
        });

        // Unblock the worker
        barrier.wait();

        // Wait for all tasks
        h_low.join().unwrap();
        h_high.join().unwrap();
        h_normal.join().unwrap();

        let final_order = order.lock().unwrap();
        assert_eq!(*final_order, vec!["high", "normal", "low"]);

        queue.shutdown();
    }

    #[test]
    fn is_done_returns_false_then_true() {
        let queue = TaskQueue::new(1);
        let barrier = Arc::new(Barrier::new(2));

        let b = barrier.clone();
        let handle = queue.submit(move || {
            b.wait();
            99
        });

        // Task is blocked, so not done yet
        assert!(!handle.is_done());

        // Unblock the task
        barrier.wait();

        // Wait for completion
        let result = handle.join().unwrap();
        assert_eq!(result, 99);

        queue.shutdown();
    }

    #[test]
    fn shutdown_completes_running_tasks() {
        let queue = TaskQueue::new(1);
        let (tx, rx) = mpsc::channel();

        queue.submit(move || {
            thread::sleep(Duration::from_millis(50));
            tx.send(true).unwrap();
        });

        // Give the worker time to start the task
        thread::sleep(Duration::from_millis(10));

        // Shutdown should wait for the running task
        queue.shutdown();

        // The task should have completed
        assert!(rx.recv_timeout(Duration::from_millis(100)).unwrap());
    }

    #[test]
    fn panicking_task_returns_panicked_error() {
        let queue = TaskQueue::new(1);
        let handle = queue.submit(|| {
            panic!("intentional panic");
        });
        match handle.join() {
            Err(TaskError::Panicked) => {}
            other => panic!("expected TaskError::Panicked, got {:?}", other.err()),
        }

        // Queue should still work after a panic
        let handle = queue.submit(|| 123);
        assert_eq!(handle.join().unwrap(), 123);

        queue.shutdown();
    }

    #[test]
    fn concurrency_limit_is_respected() {
        let concurrency = 3;
        let queue = TaskQueue::new(concurrency);
        let running = Arc::new(AtomicUsize::new(0));
        let max_running = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..concurrency * 2 {
            let r = running.clone();
            let m = max_running.clone();
            handles.push(queue.submit(move || {
                let current = r.fetch_add(1, Ordering::SeqCst) + 1;
                // Update max using compare-and-swap loop
                loop {
                    let prev_max = m.load(Ordering::SeqCst);
                    if current <= prev_max {
                        break;
                    }
                    if m.compare_exchange(prev_max, current, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        break;
                    }
                }
                thread::sleep(Duration::from_millis(50));
                r.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let observed_max = max_running.load(Ordering::SeqCst);
        assert!(
            observed_max <= concurrency,
            "max concurrent tasks ({observed_max}) exceeded concurrency limit ({concurrency})"
        );

        queue.shutdown();
    }
}
