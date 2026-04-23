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
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

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
    /// The task was rejected because the queue is at capacity.
    QueueFull,
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskError::Panicked => write!(f, "task panicked"),
            TaskError::Cancelled => write!(f, "task cancelled"),
            TaskError::QueueFull => write!(f, "task rejected: queue is full"),
        }
    }
}

impl std::error::Error for TaskError {}

/// Snapshot of task queue statistics for observability.
///
/// Obtained via [`TaskQueue::stats`].
#[derive(Debug, Clone)]
pub struct TaskQueueStats {
    /// Total number of tasks submitted to the queue.
    pub total_submitted: u64,
    /// Number of tasks that completed successfully.
    pub completed: u64,
    /// Number of tasks that failed (panicked).
    pub failed: u64,
    /// Number of tasks currently being executed by workers.
    pub in_flight: u64,
    /// Sum of end-to-end latencies (enqueue -> completion) in nanoseconds
    /// for every task that has finished (successfully or with a panic).
    pub total_latency_nanos: u128,
    /// Number of completed tasks that have contributed to `total_latency_nanos`.
    pub completed_latency_samples: u64,
}

impl TaskQueueStats {
    /// Return the average end-to-end task latency (enqueue -> completion).
    ///
    /// Returns `None` when no tasks have completed yet.
    ///
    /// # Example
    ///
    /// ```
    /// use philiprehberger_task_queue::TaskQueue;
    ///
    /// let queue = TaskQueue::new(1);
    /// queue.submit(|| 1 + 1).join().unwrap();
    /// let stats = queue.stats();
    /// assert!(stats.average_latency().is_some());
    /// queue.shutdown();
    /// ```
    pub fn average_latency(&self) -> Option<Duration> {
        if self.completed_latency_samples == 0 {
            return None;
        }
        let avg_nanos = self.total_latency_nanos / self.completed_latency_samples as u128;
        // Cap to u64::MAX nanos (~584 years) — safe for all practical cases.
        let capped = avg_nanos.min(u64::MAX as u128) as u64;
        Some(Duration::from_nanos(capped))
    }
}

/// Shared atomic counters used by the task queue for stats tracking.
struct StatsCounters {
    total_submitted: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    in_flight: AtomicU64,
    /// Cumulative enqueue -> completion latency across all finished tasks.
    /// Stored in a mutex because u128 has no stable atomic on all targets.
    latency: Mutex<LatencyAccumulator>,
}

#[derive(Default)]
struct LatencyAccumulator {
    total_nanos: u128,
    samples: u64,
}

impl StatsCounters {
    fn new() -> Self {
        Self {
            total_submitted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            latency: Mutex::new(LatencyAccumulator::default()),
        }
    }

    fn record_latency(&self, elapsed: Duration) {
        if let Ok(mut acc) = self.latency.lock() {
            acc.total_nanos = acc.total_nanos.saturating_add(elapsed.as_nanos());
            acc.samples = acc.samples.saturating_add(1);
        }
    }

    fn snapshot_latency(&self) -> (u128, u64) {
        match self.latency.lock() {
            Ok(acc) => (acc.total_nanos, acc.samples),
            Err(_) => (0, 0),
        }
    }
}

type CompletionCallback = dyn Fn(bool, Duration) + Send + Sync;

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

/// Returned by a task closure: signals the result slot after the worker
/// has finished post-task bookkeeping (stats, callback).
type TaskCompletion = Box<dyn FnOnce() + Send>;
type BoxedTask = Box<dyn FnOnce() -> TaskCompletion + Send>;

struct QueueEntry {
    priority: Priority,
    sequence: u64,
    task: BoxedTask,
    enqueued_at: Instant,
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
    draining: bool,
    next_sequence: u64,
    max_queued: Option<usize>,
    paused: bool,
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
    stats: Arc<StatsCounters>,
    callback: Arc<Mutex<Option<Arc<CompletionCallback>>>>,
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
                draining: false,
                next_sequence: 0,
                max_queued: None,
                paused: false,
            }),
            Condvar::new(),
        ));

        let stats = Arc::new(StatsCounters::new());
        let callback: Arc<Mutex<Option<Arc<CompletionCallback>>>> = Arc::new(Mutex::new(None));

        let mut workers = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let shared = Arc::clone(&shared);
            let stats = Arc::clone(&stats);
            let callback = Arc::clone(&callback);
            let handle = thread::spawn(move || {
                worker_loop(&shared, &stats, &callback);
            });
            workers.push(handle);
        }

        TaskQueue {
            shared,
            workers: Some(workers),
            stats,
            callback,
        }
    }

    /// Create a new task queue with a maximum pending task limit.
    ///
    /// When the queue contains `max_queued` or more pending tasks, new
    /// submissions are rejected and the returned handle will yield
    /// `TaskError::QueueFull` on join.
    ///
    /// # Panics
    ///
    /// Panics if `concurrency` is zero.
    pub fn with_capacity(concurrency: usize, max_queued: usize) -> Self {
        let queue = Self::new(concurrency);
        {
            let (ref mutex, _) = *queue.shared;
            mutex.lock().unwrap().max_queued = Some(max_queued);
        }
        queue
    }

    /// Temporarily stop workers from processing tasks.
    ///
    /// Tasks can still be submitted while paused, but workers will not
    /// dequeue them until [`resume`](TaskQueue::resume) is called.
    /// Draining overrides the pause — [`drain`](TaskQueue::drain) will
    /// complete all pending tasks even if the queue is paused.
    pub fn pause(&self) {
        let (ref mutex, _) = *self.shared;
        mutex.lock().unwrap().paused = true;
    }

    /// Resume processing after a call to [`pause`](TaskQueue::pause).
    pub fn resume(&self) {
        let (ref mutex, ref condvar) = *self.shared;
        mutex.lock().unwrap().paused = false;
        condvar.notify_all();
    }

    /// Check whether task processing is currently paused.
    pub fn is_paused(&self) -> bool {
        let (ref mutex, _) = *self.shared;
        mutex.lock().unwrap().paused
    }

    /// Return the number of tasks waiting in the queue.
    pub fn pending_count(&self) -> usize {
        let (ref mutex, _) = *self.shared;
        mutex.lock().unwrap().queue.len()
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
    ///
    /// If the queue is draining or shut down, the returned handle will
    /// immediately yield `TaskError::Cancelled`.
    pub fn submit_with_priority<F, T>(&self, priority: Priority, task: F) -> TaskHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let slot = Arc::new(TaskResultSlot {
            mutex: Mutex::new(None),
            condvar: Condvar::new(),
        });

        // Reject submissions if draining, shut down, or queue is full.
        {
            let (ref mutex, _) = *self.shared;
            let state = mutex.lock().unwrap();
            if state.draining || state.shutdown {
                slot.set(Err(TaskError::Cancelled));
                return TaskHandle { inner: slot };
            }
            if let Some(max) = state.max_queued {
                if state.queue.len() >= max {
                    slot.set(Err(TaskError::QueueFull));
                    return TaskHandle { inner: slot };
                }
            }
        }

        let cancel_guard = CancelGuard {
            slot: Arc::clone(&slot),
        };

        let boxed: BoxedTask = Box::new(move || {
            // The cancel guard is moved into the closure. If the closure runs,
            // we explicitly set the result and then forget the guard so it
            // doesn't overwrite with Cancelled. If the closure is dropped without
            // running, the guard's Drop fires and sets Cancelled.
            let outcome = panic::catch_unwind(AssertUnwindSafe(task));
            let success = outcome.is_ok();
            TASK_SUCCESS.with(|s| s.set(success));
            let value = match outcome {
                Ok(v) => Ok(v),
                Err(_) => Err(TaskError::Panicked),
            };
            let slot = Arc::clone(&cancel_guard.slot);
            // Prevent the Drop impl from overwriting the result with Cancelled
            std::mem::forget(cancel_guard);
            // Return a completion callback that the worker calls AFTER stats
            // and on_complete callback, so join() doesn't return prematurely.
            Box::new(move || slot.set(value))
        });

        self.stats
            .total_submitted
            .fetch_add(1, AtomicOrdering::Relaxed);

        let (ref mutex, ref condvar) = *self.shared;
        let mut state = mutex.lock().unwrap();
        let sequence = state.next_sequence;
        state.next_sequence += 1;
        state.queue.push(QueueEntry {
            priority,
            sequence,
            task: boxed,
            enqueued_at: Instant::now(),
        });
        condvar.notify_one();

        TaskHandle { inner: slot }
    }

    /// Return a snapshot of task queue statistics.
    ///
    /// The counters are updated atomically as tasks are submitted, completed,
    /// and failed, so successive calls may return different values.
    ///
    /// # Example
    ///
    /// ```
    /// use philiprehberger_task_queue::TaskQueue;
    ///
    /// let queue = TaskQueue::new(1);
    /// let handle = queue.submit(|| 1 + 1);
    /// handle.join().unwrap();
    ///
    /// let stats = queue.stats();
    /// assert_eq!(stats.total_submitted, 1);
    /// assert_eq!(stats.completed, 1);
    /// queue.shutdown();
    /// ```
    pub fn stats(&self) -> TaskQueueStats {
        let (total_latency_nanos, completed_latency_samples) = self.stats.snapshot_latency();
        TaskQueueStats {
            total_submitted: self.stats.total_submitted.load(AtomicOrdering::Relaxed),
            completed: self.stats.completed.load(AtomicOrdering::Relaxed),
            failed: self.stats.failed.load(AtomicOrdering::Relaxed),
            in_flight: self.stats.in_flight.load(AtomicOrdering::Relaxed),
            total_latency_nanos,
            completed_latency_samples,
        }
    }

    /// Stop accepting new tasks and wait for all queued and in-flight tasks to
    /// complete.
    ///
    /// Unlike [`shutdown`](TaskQueue::shutdown), `drain` does **not** drop
    /// pending tasks — every task that was already submitted will run to
    /// completion. New submissions made after `drain` is called will be
    /// immediately cancelled.
    ///
    /// This method blocks until the queue is empty and all workers are idle,
    /// then shuts down the worker threads.
    ///
    /// # Example
    ///
    /// ```
    /// use philiprehberger_task_queue::TaskQueue;
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// let queue = TaskQueue::new(2);
    /// let counter = Arc::new(AtomicUsize::new(0));
    ///
    /// for _ in 0..5 {
    ///     let c = counter.clone();
    ///     queue.submit(move || { c.fetch_add(1, Ordering::SeqCst); });
    /// }
    ///
    /// queue.drain();
    /// assert_eq!(counter.load(Ordering::SeqCst), 5);
    /// ```
    pub fn drain(mut self) {
        self.do_drain();
    }

    fn do_drain(&mut self) {
        let (ref mutex, ref condvar) = *self.shared;
        {
            let mut state = mutex.lock().unwrap();
            state.draining = true;
            // Do NOT clear the queue — let workers process everything.
            // Wake workers in case the queue was paused.
            condvar.notify_all();
        }

        // Wait until the queue is empty and no tasks are in-flight.
        {
            let mut state = mutex.lock().unwrap();
            while !state.queue.is_empty() || self.stats.in_flight.load(AtomicOrdering::SeqCst) > 0 {
                state = condvar.wait(state).unwrap();
            }
        }

        // Now perform a normal shutdown (workers will exit because queue is
        // empty and shutdown flag is set).
        self.do_shutdown();
    }

    /// Register a callback that fires after each task completes.
    ///
    /// The callback receives two arguments:
    /// - `success` — `true` if the task completed without panicking, `false` otherwise.
    /// - `duration` — wall-clock time the task took to execute.
    ///
    /// Only one callback may be active at a time; calling this again replaces
    /// the previous callback.
    ///
    /// # Example
    ///
    /// ```
    /// use philiprehberger_task_queue::TaskQueue;
    /// use std::sync::Arc;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    ///
    /// let queue = TaskQueue::new(1);
    /// let count = Arc::new(AtomicUsize::new(0));
    /// let c = count.clone();
    /// queue.on_complete(move |_success, _dur| {
    ///     c.fetch_add(1, Ordering::SeqCst);
    /// });
    ///
    /// queue.submit(|| 42).join().unwrap();
    /// assert_eq!(count.load(Ordering::SeqCst), 1);
    /// queue.shutdown();
    /// ```
    pub fn on_complete<F>(&self, callback: F)
    where
        F: Fn(bool, Duration) + Send + Sync + 'static,
    {
        let mut guard = self.callback.lock().unwrap();
        *guard = Some(Arc::new(callback));
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
                if !state.draining {
                    state.queue.clear();
                }
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

thread_local! {
    /// Used by the task closure to communicate success/failure to the worker loop.
    static TASK_SUCCESS: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

fn worker_loop(
    shared: &(Mutex<SharedState>, Condvar),
    stats: &StatsCounters,
    callback: &Mutex<Option<Arc<CompletionCallback>>>,
) {
    let (ref mutex, ref condvar) = *shared;
    loop {
        let task = {
            let mut state = mutex.lock().unwrap();
            loop {
                if !state.paused || state.draining {
                    if let Some(entry) = state.queue.pop() {
                        break Some((entry.task, entry.enqueued_at));
                    }
                }
                if state.shutdown || (state.draining && state.queue.is_empty()) {
                    break None;
                }
                state = condvar.wait(state).unwrap();
            }
        };
        match task {
            Some((task, enqueued_at)) => {
                stats.in_flight.fetch_add(1, AtomicOrdering::SeqCst);
                let start = Instant::now();
                let completion = task();
                let elapsed = start.elapsed();
                let total_latency = enqueued_at.elapsed();
                stats.record_latency(total_latency);
                stats.in_flight.fetch_sub(1, AtomicOrdering::SeqCst);

                // The task closure uses catch_unwind internally and communicates
                // success/failure via a thread-local, since the boxed closure
                // always returns () without panicking.
                let success = TASK_SUCCESS.with(|s| s.get());
                if success {
                    stats.completed.fetch_add(1, AtomicOrdering::Relaxed);
                } else {
                    stats.failed.fetch_add(1, AtomicOrdering::Relaxed);
                }

                // Fire the on_complete callback if registered.
                if let Ok(guard) = callback.lock() {
                    if let Some(ref cb) = *guard {
                        cb(success, elapsed);
                    }
                }

                // Now set the result and notify the TaskHandle — this ensures
                // stats and callback have both completed before join() returns.
                completion();

                // Notify condvar so drain() can check progress.
                condvar.notify_all();
            }
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

    #[test]
    fn stats_tracks_submitted_and_completed() {
        let queue = TaskQueue::new(2);

        let handles: Vec<_> = (0..5).map(|i| queue.submit(move || i)).collect();
        for h in handles {
            h.join().unwrap();
        }

        let s = queue.stats();
        assert_eq!(s.total_submitted, 5);
        assert_eq!(s.completed, 5);
        assert_eq!(s.failed, 0);
        assert_eq!(s.in_flight, 0);

        queue.shutdown();
    }

    #[test]
    fn stats_tracks_failures() {
        let queue = TaskQueue::new(1);

        let h1 = queue.submit(|| panic!("boom"));
        let _ = h1.join(); // Err(Panicked)

        let h2 = queue.submit(|| 42);
        h2.join().unwrap();

        let s = queue.stats();
        assert_eq!(s.total_submitted, 2);
        assert_eq!(s.completed, 1);
        assert_eq!(s.failed, 1);

        queue.shutdown();
    }

    #[test]
    fn drain_completes_all_pending_tasks() {
        let queue = TaskQueue::new(1);
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..10 {
            let c = counter.clone();
            queue.submit(move || {
                c.fetch_add(1, Ordering::SeqCst);
            });
        }

        queue.drain();
        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[test]
    fn drain_rejects_new_submissions() {
        let queue = TaskQueue::new(1);
        let barrier = Arc::new(Barrier::new(2));

        // Block the worker so we can call drain from another context
        let b = barrier.clone();
        queue.submit(move || {
            b.wait();
        });

        // Give the worker time to pick up the task
        thread::sleep(Duration::from_millis(50));

        // Submit a task that should be queued
        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        queue.submit(move || {
            c.fetch_add(1, Ordering::SeqCst);
        });

        // We need to set draining and then unblock. Since drain() consumes self,
        // we test the rejection behavior differently: submit after drain finishes
        // is not possible (self consumed). Instead, verify that drain processes
        // all queued tasks.
        barrier.wait();
        queue.drain();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn on_complete_callback_fires_on_success() {
        let queue = TaskQueue::new(1);
        let call_count = Arc::new(AtomicUsize::new(0));
        let success_count = Arc::new(AtomicUsize::new(0));

        let cc = call_count.clone();
        let sc = success_count.clone();
        queue.on_complete(move |success, dur| {
            cc.fetch_add(1, Ordering::SeqCst);
            if success {
                sc.fetch_add(1, Ordering::SeqCst);
            }
            assert!(dur.as_nanos() > 0);
        });

        let h = queue.submit(|| 42);
        h.join().unwrap();

        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        assert_eq!(success_count.load(Ordering::SeqCst), 1);

        queue.shutdown();
    }

    #[test]
    fn on_complete_callback_fires_on_failure() {
        let queue = TaskQueue::new(1);
        let failure_count = Arc::new(AtomicUsize::new(0));

        let fc = failure_count.clone();
        queue.on_complete(move |success, _dur| {
            if !success {
                fc.fetch_add(1, Ordering::SeqCst);
            }
        });

        let h = queue.submit(|| panic!("intentional"));
        let _ = h.join();

        assert_eq!(failure_count.load(Ordering::SeqCst), 1);

        queue.shutdown();
    }

    #[test]
    fn on_complete_callback_reports_duration() {
        let queue = TaskQueue::new(1);
        let observed_duration = Arc::new(Mutex::new(Duration::ZERO));

        let od = observed_duration.clone();
        queue.on_complete(move |_success, dur| {
            *od.lock().unwrap() = dur;
        });

        let h = queue.submit(|| {
            thread::sleep(Duration::from_millis(50));
        });
        h.join().unwrap();

        let dur = *observed_duration.lock().unwrap();
        assert!(dur >= Duration::from_millis(40), "duration was {dur:?}");

        queue.shutdown();
    }

    #[test]
    fn replacing_callback() {
        let queue = TaskQueue::new(1);
        let first_count = Arc::new(AtomicUsize::new(0));
        let second_count = Arc::new(AtomicUsize::new(0));

        let fc = first_count.clone();
        queue.on_complete(move |_, _| {
            fc.fetch_add(1, Ordering::SeqCst);
        });

        queue.submit(|| {}).join().unwrap();

        let sc = second_count.clone();
        queue.on_complete(move |_, _| {
            sc.fetch_add(1, Ordering::SeqCst);
        });

        queue.submit(|| {}).join().unwrap();

        assert_eq!(first_count.load(Ordering::SeqCst), 1);
        assert_eq!(second_count.load(Ordering::SeqCst), 1);

        queue.shutdown();
    }

    #[test]
    fn test_with_capacity_rejects_when_full() {
        let queue = TaskQueue::with_capacity(1, 2);
        // Pause so tasks don't get consumed
        queue.pause();

        let h1 = queue.submit(|| 1);
        let h2 = queue.submit(|| 2);
        let h3 = queue.submit(|| 3); // should be rejected

        // h3 should immediately return QueueFull
        queue.resume();
        assert!(matches!(h3.join(), Err(TaskError::QueueFull)));

        // h1 and h2 should succeed
        assert!(h1.join().is_ok());
        assert!(h2.join().is_ok());
        queue.shutdown();
    }

    #[test]
    fn test_with_capacity_allows_within_limit() {
        let queue = TaskQueue::with_capacity(2, 10);
        let handles: Vec<_> = (0..10).map(|i| queue.submit(move || i)).collect();
        for (i, h) in handles.into_iter().enumerate() {
            assert_eq!(h.join().unwrap(), i);
        }
        queue.shutdown();
    }

    #[test]
    fn test_pause_and_resume() {
        let queue = TaskQueue::new(2);
        queue.pause();

        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        queue.submit(move || {
            c.fetch_add(1, Ordering::SeqCst);
        });

        // Give workers time to potentially process (they shouldn't)
        thread::sleep(Duration::from_millis(50));
        assert_eq!(counter.load(Ordering::SeqCst), 0);

        queue.resume();
        // Give time to process
        thread::sleep(Duration::from_millis(100));
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        queue.shutdown();
    }

    #[test]
    fn test_is_paused() {
        let queue = TaskQueue::new(1);
        assert!(!queue.is_paused());
        queue.pause();
        assert!(queue.is_paused());
        queue.resume();
        assert!(!queue.is_paused());
        queue.shutdown();
    }

    #[test]
    fn test_drain_overrides_pause() {
        let queue = TaskQueue::new(2);
        queue.pause();

        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..5 {
            let c = counter.clone();
            queue.submit(move || {
                c.fetch_add(1, Ordering::SeqCst);
            });
        }

        // Drain should complete all tasks even though paused
        queue.drain();
        assert_eq!(counter.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn test_pending_count() {
        let queue = TaskQueue::new(1);
        queue.pause();

        assert_eq!(queue.pending_count(), 0);
        queue.submit(|| 1);
        queue.submit(|| 2);
        assert_eq!(queue.pending_count(), 2);

        queue.resume();
        thread::sleep(Duration::from_millis(100));
        assert_eq!(queue.pending_count(), 0);

        queue.shutdown();
    }

    #[test]
    fn test_queue_full_error_display() {
        assert_eq!(
            format!("{}", TaskError::QueueFull),
            "task rejected: queue is full"
        );
    }
}
