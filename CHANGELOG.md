# Changelog

## 0.2.6 (2026-03-31)

- Standardize README to 3-badge format with emoji Support section
- Update CI checkout action to v5 for Node.js 24 compatibility

## 0.2.5 (2026-03-27)

- Add GitHub issue templates, PR template, and dependabot configuration
- Update README badges and add Support section

## 0.2.4 (2026-03-22)

- Fix CHANGELOG date ordering for v0.1.7

## 0.2.3 (2026-03-22)

- Fix CHANGELOG date ordering

## 0.2.2 (2026-03-22)

- Fix CHANGELOG compliance

## 0.2.1 (2026-03-17)

- Fix race condition where `join()` could return before `on_complete` callback and stats were updated
- Task result delivery is now deferred until after worker bookkeeping completes

## 0.2.0 (2026-03-16)

- Add `TaskQueueStats` and `stats()` method for observability (total submitted, completed, failed, in-flight)
- Add `drain()` for graceful shutdown that completes all pending tasks instead of dropping them
- Add `on_complete()` callback that fires after each task with `(success: bool, duration: Duration)`
- New submissions are rejected with `TaskError::Cancelled` when draining

## 0.1.7 (2026-03-16)

- Add readme, rust-version, documentation to Cargo.toml
- Add Development section to README

## 0.1.6 (2026-03-16)

- Update install snippet to use full version

## 0.1.5 (2026-03-16)

- Add README badges
- Synchronize version across Cargo.toml, README, and CHANGELOG

## 0.1.0 (2026-03-15)

- Initial release
- Thread-based task queue with configurable concurrency
- Priority support (High, Normal, Low)
- TaskHandle for joining results and checking completion
- Panic-safe task execution with TaskError reporting
- Graceful shutdown with in-flight task completion
