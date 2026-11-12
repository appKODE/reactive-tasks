# Changelog

## 2.0.1 - 2026-11-12

### Fixed

- `launch`, `launchLatest`, `launchIfIdle`, `start`, `startIfIdle` threw `NullPointerException` on JVM/Android when
  the argument was `null`. A `null` argument now works everywhere, and `relaunch` re-runs a job with a `null` argument
  instead of returning `JobHandle.NIL`.
- Memory leak: a scheduler kept the argument and the state of every job ever started. The argument is now stored in
  the `JobHandle`, and `Scheduler.taskJobState` keeps only the last finished job of each task (plus jobs which are
  still running). States of older finished jobs and of cancelled jobs are removed. Read finished states with
  `finishedStates` / `results` instead of looking up old job ids in `taskJobState`.
- A job could get dropped and `start` could hang forever when an observer of the task (`results`, `finishedStates`,
  ...) started the next job from inside its collector. State updates to observers no longer suspend the job which
  publishes them.

## 2.0.0 - 2026-11-12

### Changed (breaking)

Suspending functions get the short names, non-suspending ones that return a handle are named `launch`, like
`CoroutineScope.launch`.

| 1.x | 2.0.0 |
|---|---|
| `start(handle, argument, tag)`: `JobHandle` | `launch(handle, argument, tag)` |
| `startLatest(handle, argument, tag)` | `launchLatest(handle, argument, tag)` |
| `startIfIdle(handle, argument, tag)`: `JobHandle?` | `launchIfIdle(handle, argument, tag)` |
| `restart(handle)` | `relaunch(handle)` |
| `startSuspended(handle, argument, tag)`: `JobState` | `start(handle, argument, tag)` |
| `startSuspendedIfIdle(handle, argument, tag)`: `JobState?` | `startIfIdle(handle, argument, tag)` |

Migrate in two passes: first rename `start`, `startLatest`, `startIfIdle` and `restart` to the `launch` names, then
`startSuspended*` to `start*`. In the other order, a `start` call inside a coroutine still compiles and starts
waiting for the job.

## 1.4.0 - 2026-11-11

### Added

- `Scheduler.startIfIdle(handle, argument, tag)` and `startIfIdle(handle)` for `Unit` tasks: starts a job unless one is
  running. Atomic with respect to other `startIfIdle` calls (two concurrent calls start one job). Returns the
  `JobHandle`, or null if dropped. Replaces remo `SkipNew` / `Disallow`.
- `Scheduler.startSuspendedIfIdle(handle, argument, tag)` and `startSuspendedIfIdle(handle)`: runs `startSuspended`
  unless a job is running, returns the `JobState`, or null (without waiting) if dropped.
- `Scheduler.bindTo(scope)`: calls `cancelAll()` when the job of `scope` completes.
- `Scheduler.results(handle, replayLast = true)`: `Flow<kotlin.Result<R>>` of finished jobs.

### Changed

- `kotlinx-atomicfu` is now an `implementation` dependency of all targets, it was iOS only.

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
