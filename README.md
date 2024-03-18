# rs-task-queue

[![CI](https://github.com/philiprehberger/rs-task-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/philiprehberger/rs-task-queue/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/philiprehberger-task-queue.svg)](https://crates.io/crates/philiprehberger-task-queue)
[![License](https://img.shields.io/github/license/philiprehberger/rs-task-queue)](LICENSE)

In-process thread-based task queue for Rust with priority scheduling and concurrency control. Zero external dependencies — uses only `std` threading primitives.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
philiprehberger-task-queue = "0.1"
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

## API

| Item | Description |
|------|-------------|
| `TaskQueue::new(concurrency)` | Create a queue with N worker threads |
| `queue.submit(task)` | Submit a task at Normal priority; returns `TaskHandle<T>` |
| `queue.submit_with_priority(priority, task)` | Submit a task at the given priority; returns `TaskHandle<T>` |
| `queue.shutdown()` | Signal workers to stop, wait for running tasks, drop pending |
| `handle.join()` | Block until the task completes; returns `Result<T, TaskError>` |
| `handle.is_done()` | Check if the task has completed without blocking |
| `Priority::High` | Highest execution priority |
| `Priority::Normal` | Default execution priority |
| `Priority::Low` | Lowest execution priority |
| `TaskError::Panicked` | Task panicked during execution |
| `TaskError::Cancelled` | Task was dropped during shutdown before it ran |

## License

MIT
