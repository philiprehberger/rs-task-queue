# rs-task-queue

[![CI](https://github.com/philiprehberger/rs-task-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/philiprehberger/rs-task-queue/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/philiprehberger-task-queue.svg)](https://crates.io/crates/philiprehberger-task-queue)
[![License](https://img.shields.io/github/license/philiprehberger/rs-task-queue)](LICENSE)

In-process thread-based task queue with priority and concurrency control

## Installation

```toml
[dependencies]
philiprehberger-task-queue = "0.2.2"
```

## Usage

```rust
use philiprehberger_task_queue::{TaskQueue, Priority};

// Create a queue with 4 worker threads
let queue = TaskQueue::new(4);

// Submit a task (Normal priority by default)
let handle = queue.submit(|| {
    42
});

// Block until the task completes
let result = handle.join().unwrap();
assert_eq!(result, 42);

// Submit with explicit priority
let handle = queue.submit_with_priority(Priority::High, || {
    "urgent work done"
});
assert_eq!(handle.join().unwrap(), "urgent work done");

// Check completion without blocking
let handle = queue.submit(|| 100);
// ... do other work ...
if handle.is_done() {
    println!("Task finished!");
}

// Graceful shutdown: finishes running tasks, drops pending ones
queue.shutdown();
```

### Stats

Get a real-time snapshot of queue activity:

```rust
use philiprehberger_task_queue::TaskQueue;

let queue = TaskQueue::new(2);
queue.submit(|| 1 + 1).join().unwrap();

let stats = queue.stats();
println!("submitted={} completed={} failed={} in_flight={}",
    stats.total_submitted, stats.completed, stats.failed, stats.in_flight);

queue.shutdown();
```

### Drain

Graceful shutdown that completes all pending tasks instead of dropping them:

```rust
use philiprehberger_task_queue::TaskQueue;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

let queue = TaskQueue::new(2);
let counter = Arc::new(AtomicUsize::new(0));

for _ in 0..10 {
    let c = counter.clone();
    queue.submit(move || { c.fetch_add(1, Ordering::SeqCst); });
}

// Waits for all 10 tasks to finish, then shuts down
queue.drain();
assert_eq!(counter.load(Ordering::SeqCst), 10);
```

### Callbacks

Register a callback that fires after every task completes:

```rust
use philiprehberger_task_queue::TaskQueue;

let queue = TaskQueue::new(2);
queue.on_complete(|success, duration| {
    println!("task finished: success={success} took={duration:?}");
});

queue.submit(|| "work").join().unwrap();
queue.shutdown();
```

## API

| Item | Description |
|------|-------------|
| `TaskQueue::new(concurrency)` | Create a queue with N worker threads |
| `queue.submit(task)` | Submit a task at Normal priority; returns `TaskHandle<T>` |
| `queue.submit_with_priority(priority, task)` | Submit a task at the given priority; returns `TaskHandle<T>` |
| `queue.stats()` | Return a `TaskQueueStats` snapshot (submitted, completed, failed, in-flight) |
| `queue.drain()` | Stop accepting tasks, wait for all queued tasks to finish, then shut down |
| `queue.on_complete(callback)` | Register a `Fn(bool, Duration)` callback fired after each task |
| `queue.shutdown()` | Signal workers to stop, wait for running tasks, drop pending |
| `handle.join()` | Block until the task completes; returns `Result<T, TaskError>` |
| `handle.is_done()` | Check if the task has completed without blocking |
| `Priority::High` | Highest execution priority |
| `Priority::Normal` | Default execution priority |
| `Priority::Low` | Lowest execution priority |
| `TaskError::Panicked` | Task panicked during execution |
| `TaskError::Cancelled` | Task was dropped during shutdown before it ran |


## Development

```bash
cargo test
cargo clippy -- -D warnings
```

## License

MIT
