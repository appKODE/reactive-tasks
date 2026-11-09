# Changelog

## 1.1.0 - 2026-11-09

### Added

- `Scheduler.jobStates(handle, replayFinished = false)`: states of one task, never completes on job cancellation.
  Replays the state of a running job to new subscribers; with `replayFinished = true` it also replays the last
  finished (success or error) job when nothing is running, so late subscribers get the result.
- `Scheduler.successResults(handle, replayFinished = false): Flow<R>` and
  `Scheduler.errors(handle, replayFinished = false): Flow<Throwable>`.
- `JobState.resultOf(handle): R?`: the job result typed by the task's result type, no cast at the call site.
  `JobState` itself stays untyped (`result: Any?`), so 1.0.0 code compiles unchanged.
- `Scheduler.isRunning(handle)`: whether a job started with `start` / `startLatest` / `restart` is active.
- `observeStateChanges(handle, completeOnJobCancel)`: pass `false` to keep observing after a job is cancelled.
- README: remo 1.2.0 / 9.0.1 to reactive-tasks API map.

### Fixed

- `observeStateChanges(handle)` emits only states of `handle`'s jobs and completes only when a job of `handle`
  is cancelled (it used to emit all tasks' states and close on any cancelled job).
- Default `onUncaughtException` printed task id and name swapped.

## 1.0.0 - 2026-11-08

First public release, published to Maven Central as `ru.kode:reactive-tasks`.

- Kotlin Multiplatform targets: JVM, Android, iOS (`iosX64`, `iosArm64`, `iosSimulatorArm64`).
