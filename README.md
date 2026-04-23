# rs-task-queue

[![CI](https://github.com/philiprehberger/rs-task-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/philiprehberger/rs-task-queue/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/philiprehberger-task-queue.svg)](https://crates.io/crates/philiprehberger-task-queue)
[![Last updated](https://img.shields.io/github/last-commit/philiprehberger/rs-task-queue)](https://github.com/philiprehberger/rs-task-queue/commits/main)

In-process thread-based task queue with priority and concurrency control

## Installation

```toml
[dependencies]
philiprehberger-task-queue = "0.4.0"
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

### Queue Capacity Limit

Limit the queue size to apply backpressure:

```rust
use philiprehberger_task_queue::{TaskQueue, TaskError};

// Allow at most 100 pending tasks
let queue = TaskQueue::with_capacity(4, 100);

let handle = queue.submit(|| 42);
assert_eq!(handle.join().unwrap(), 42);

queue.shutdown();
```

### Pause and Resume

Temporarily stop processing without shutting down:

```rust
use philiprehberger_task_queue::TaskQueue;

let queue = TaskQueue::new(4);
queue.pause();

queue.submit(|| println!("queued but not yet running"));

queue.resume(); // workers start processing again
queue.shutdown();
```

### Error Handling

`submit` and `submit_with_priority` always return a `TaskHandle` — errors surface
when you call `join()`. The two most common error cases are backpressure
(`TaskError::QueueFull`) and task panics (`TaskError::Panicked`).

**Retry with backoff on `QueueFull`:**

```rust
use philiprehberger_task_queue::{TaskQueue, TaskError};
use std::thread;
use std::time::Duration;

let queue = TaskQueue::with_capacity(2, 4);

let mut delay = Duration::from_millis(10);
let result = loop {
    let handle = queue.submit(|| "work");
    match handle.join() {
        Err(TaskError::QueueFull) => {
            thread::sleep(delay);
            delay = (delay * 2).min(Duration::from_secs(1));
            continue;
        }
        other => break other,
    }
};
let _ = result;
queue.shutdown();
```

**Graceful degradation on `Panicked`:**

```rust
use philiprehberger_task_queue::{TaskQueue, TaskError};

let queue = TaskQueue::new(2);
let handle = queue.submit(|| {
    if false { panic!("boom"); }
    "ok"
});

let value = match handle.join() {
    Ok(v) => v,
    Err(TaskError::Panicked) => {
        eprintln!("task panicked — falling back to default");
        "fallback"
    }
    Err(_) => "fallback",
};
assert_eq!(value, "ok");
queue.shutdown();
```

### Queue Depth

Check how many tasks are waiting:

```rust
use philiprehberger_task_queue::TaskQueue;

let queue = TaskQueue::new(2);
println!("pending: {}", queue.pending_count());
queue.shutdown();
```

## API

| Item | Description |
|------|-------------|
| `TaskQueue::new(concurrency)` | Create a queue with N worker threads |
| `TaskQueue::with_capacity(concurrency, max_queued)` | Create a queue with a maximum pending task limit |
| `queue.submit(task)` | Submit a task at Normal priority; returns `TaskHandle<T>` |
| `queue.submit_with_priority(priority, task)` | Submit a task at the given priority; returns `TaskHandle<T>` |
| `queue.stats()` | Return a `TaskQueueStats` snapshot (submitted, completed, failed, in-flight, latency) |
| `stats.average_latency()` | `Option<Duration>` average enqueue-to-completion latency, `None` if no tasks have finished |
| `queue.drain()` | Stop accepting tasks, wait for all queued tasks to finish, then shut down |
| `queue.on_complete(callback)` | Register a `Fn(bool, Duration)` callback fired after each task |
| `queue.pause()` | Temporarily stop processing tasks |
| `queue.resume()` | Resume processing after pause |
| `queue.is_paused()` | Check if processing is paused |
| `queue.pending_count()` | Get number of tasks waiting in the queue |
| `queue.shutdown()` | Signal workers to stop, wait for running tasks, drop pending |
| `handle.join()` | Block until the task completes; returns `Result<T, TaskError>` |
| `handle.is_done()` | Check if the task has completed without blocking |
| `Priority::High` | Highest execution priority |
| `Priority::Normal` | Default execution priority |
| `Priority::Low` | Lowest execution priority |
| `TaskError::Panicked` | Task panicked during execution |
| `TaskError::Cancelled` | Task was dropped during shutdown before it ran |
| `TaskError::QueueFull` | Task rejected because queue is at capacity |

## Development

```bash
cargo test
cargo clippy -- -D warnings
```

## Support

If you find this project useful:

⭐ [Star the repo](https://github.com/philiprehberger/rs-task-queue)

🐛 [Report issues](https://github.com/philiprehberger/rs-task-queue/issues?q=is%3Aissue+is%3Aopen+label%3Abug)

💡 [Suggest features](https://github.com/philiprehberger/rs-task-queue/issues?q=is%3Aissue+is%3Aopen+label%3Aenhancement)

❤️ [Sponsor development](https://github.com/sponsors/philiprehberger)

🌐 [All Open Source Projects](https://philiprehberger.com/open-source-packages)

💻 [GitHub Profile](https://github.com/philiprehberger)

🔗 [LinkedIn Profile](https://www.linkedin.com/in/philiprehberger)

## License

[MIT](LICENSE)
