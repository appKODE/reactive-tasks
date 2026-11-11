# Changelog

## 1.3.0 - 2026-11-11

### Added

- `Scheduler.cancelAll()`: cancels the jobs of all tasks and all other coroutines launched in `scheduler.scope`, the
  scheduler stays usable. Cancelling `scheduler.scope` itself makes the scheduler unusable for good (later starts and
  launches do nothing), so `cancelAll()` is the way to stop and start again, e.g. a remo 1.x model started again with
  a new scope.

## 1.2.0 - 2026-11-10

### Added

- `Scheduler.finishedStates(handle, replayLast = false): Flow<JobState>`: only `FinishedSuccess` and
  `FinishedError` states of the task, never completes on job cancellation. With `replayLast = true` the last
  finished job is replayed first, even while another job runs. The remo `results(replayLast)` equivalent.
- `Scheduler.runningStates(handle): Flow<Boolean>`: `isRunning(handle)` on subscription, then on every job start,
  finish and cancellation (including cancelling the scheduler scope). For progress indicators, replaces remo's
  `JobFlow.state`.

### Changed

- `successResults(handle, replayFinished = true)` and `errors(handle, replayFinished = true)` replay the last
  finished job even while another job of the task runs (they used to replay nothing until the running job finished).
  As in remo, the last finished state is replayed and then filtered: after an error `successResults` replays
  nothing and `errors` replays the error.

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
