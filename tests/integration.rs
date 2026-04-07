//! Integration tests for philiprehberger-task-queue.

use philiprehberger_task_queue::{TaskError, TaskQueue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn concurrent_enqueue_and_drain_completes_every_task() {
    let queue = TaskQueue::new(4);
    let counter = Arc::new(AtomicUsize::new(0));

    thread::scope(|s| {
        for _ in 0..8 {
            let q = &queue;
            let c = counter.clone();
            s.spawn(move || {
                let handles: Vec<_> = (0..25)
                    .map(|_| {
                        let cc = c.clone();
                        q.submit(move || {
                            cc.fetch_add(1, Ordering::SeqCst);
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().expect("task should succeed");
                }
            });
        }
    });

    // Drain to ensure no pending work remains.
    queue.drain();
    assert_eq!(counter.load(Ordering::SeqCst), 8 * 25);
}

#[test]
fn capacity_exhaustion_returns_queue_full() {
    // Single worker, tiny capacity — block the worker and overflow the queue.
    let queue = TaskQueue::with_capacity(1, 2);

    // Block the worker indefinitely until we release it.
    let release = Arc::new(AtomicUsize::new(0));
    let r = release.clone();
    let blocker = queue.submit(move || {
        while r.load(Ordering::SeqCst) == 0 {
            thread::sleep(Duration::from_millis(5));
        }
    });

    // Give the worker a moment to pick up the blocker.
    thread::sleep(Duration::from_millis(50));

    // Fill the queue to capacity (2 pending tasks).
    let _h1 = queue.submit(|| 1);
    let _h2 = queue.submit(|| 2);

    // Next submission should be rejected.
    let rejected = queue.submit(|| 3);
    match rejected.join() {
        Err(TaskError::QueueFull) => {}
        other => panic!("expected QueueFull, got {other:?}"),
    }

    // Release the blocker and let the queue drain normally.
    release.store(1, Ordering::SeqCst);
    blocker.join().unwrap();
    queue.drain();
}

#[test]
fn pause_blocks_processing_until_resume() {
    let queue = TaskQueue::new(2);
    queue.pause();
    assert!(queue.is_paused());

    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..5 {
        let c = counter.clone();
        handles.push(queue.submit(move || {
            c.fetch_add(1, Ordering::SeqCst);
        }));
    }

    // Workers are paused — tasks should not run yet.
    thread::sleep(Duration::from_millis(75));
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(queue.pending_count(), 5);

    // Resume and make sure everything drains.
    queue.resume();
    assert!(!queue.is_paused());
    for h in handles {
        h.join().expect("task should run after resume");
    }
    assert_eq!(counter.load(Ordering::SeqCst), 5);

    queue.shutdown();
}

#[test]
fn average_latency_is_some_after_work_completes() {
    let queue = TaskQueue::new(2);

    // Initially, no samples -> None.
    assert!(queue.stats().average_latency().is_none());

    let handles: Vec<_> = (0..6)
        .map(|i| {
            queue.submit(move || {
                thread::sleep(Duration::from_millis(5));
                i
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let stats = queue.stats();
    assert_eq!(stats.completed, 6);
    assert_eq!(stats.completed_latency_samples, 6);
    assert!(stats.total_latency_nanos > 0);
    let avg = stats.average_latency().expect("should have an average now");
    assert!(avg > Duration::from_nanos(0));

    queue.shutdown();
}
