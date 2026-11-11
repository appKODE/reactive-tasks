# Reactive Tasks

A library for scheduling, executing and observing repeateble reactive tasks.

# Installation

Kotlin Multiplatform: JVM, Android, iOS (`iosX64`, `iosArm64`, `iosSimulatorArm64`).

```kotlin
implementation("ru.kode:reactive-tasks:1.3.0")
```

# How to use

```kotlin
//
// Create a scheduler
//
val scheduler = Scheduler(scope = CoroutineScope(Dispatchers.IO))

//
// Register tasks
//
val fetchTaskHandle = scheduler.registerTask("fetch", body = { sampleId: String ->
    val response = networkApi.fetchSampleData(sampleId) // suspending call
    val result = response.data.size
    result // return value
})

val commitChangesHandle = scheduler.registerTask("commit", body = { changes: Changes ->
    database.store(changes)
    networkApi.updateCommitDate(changes.date)
})

val searchTaskHandle = scheduler.registerTask("search", body = { query: String ->
    networkApi.searchBy(query)
})

//
// Start tasks in parallel
//
val jobHandle1 = scheduler.start(fetchTaskHandle,     "sample-id-001")
val jobHandle2 = scheduler.start(fetchTaskHandle,     "sample-id-002")
val jobHandle3 = scheduler.start(commitChangesHandle, Changes(date = LocalDate.now()))

//
// Start tasks where each new start cancels previous running (like Flow.flatMapLatest() does)
// 
scheduler.startLatest(searchTaskHandle, "Kaliningrad")
scheduler.startLatest(searchTaskHandle, "Moscow")
scheduler.startLatest(searchTaskHandle, "Saint-Petersburg")

//
// Start tasks as suspend-functions in some scope
//
val mainScope = CoroutineScope(Dispatchers.Main)
mainScope.launch {
    val resultState = scheduler.startSuspended(fetchTaskHandle, "sample-id-003")
    if (resultState.result == 3) {
        scheduler.startSuspended(fetchTaskHandle, "sample-id-004")
        println("all fetches done!")
    }
}

//
// Add listener to observe changes
//
scheduler.addTaskStateChangeListener(object : TaskStateChangeListener {
    override suspend fun onJobStateChanged(state: JobState) {
        when (state.runState) {
            RunState.NotStarted -> println("[${state.jobId}] not started")
            RunState.Running ->    println("[${state.jobId}] running")
            RunState.FinishedSuccess -> println("[${state.jobId}] success result=${state.result}")
            RunState.FinishedError-> { 
                println("[${state.jobId}] error")
                state.error.printStackTrace() 
            }
        }
    }
    
    override fun onJobCancelled(taskId: Uuid, jobId: Uuid) {
        println("task is cancelled: jobId = $jobId, taskId = $taskId")
    }
}

//
// Or observe one task as a Flow. Read a typed result with state.resultOf(handle): R?
//
val searchStates = scheduler.jobStates(searchTaskHandle)      // replays a running job's state, never completes on cancel
val lastResult = scheduler.successResults(searchTaskHandle, replayFinished = true) // also replays the last finished result
val searchResults = scheduler.successResults(searchTaskHandle) // Flow<R>, no casts
val searchErrors = scheduler.errors(searchTaskHandle)          // Flow<Throwable>
val searchFinished = scheduler.finishedStates(searchTaskHandle, replayLast = true) // finished states only
val searchProgress = scheduler.runningStates(searchTaskHandle) // Flow<Boolean>, false after a cancel
val fetchRunning = scheduler.isRunning(fetchTaskHandle)

//
// observeStateChanges completes on the first cancelled job of the task (pass completeOnJobCancel = false
// to keep observing). It is fairly simple implementation, if you need something more involved,
// check how it is implemented, use it as a reference
//
val taskStateChanges = scheduler.observeStateChanges(searchTaskHandle)
runBlocking {
    taskStateChanges
        .shareIn(CoroutineScope(Dispatchers.Main), SharingStarted.Lazily)
        .onSubscription {
            println("Subscribed!")
        }
        .collect {
            println("Received state $it")
        }
}

//
// Cancel all jobs of a certain task
//
scheduler.cancel(searchTaskHandle)
// or
runBlocking {
    scheduler.cancelAndJoin(searchTaskHandle)
}

// 
// Cancel a specific started job 
//
scheduler.cancel(jobHandle2)
// or
runBlocking {
    scheduler.cancelAndJoin(jobHandle2)
}

//
// Cancel the jobs of all tasks and all coroutines launched in scheduler.scope, the scheduler stays usable.
// Cancelling scheduler.scope itself makes the scheduler unusable for good
//
scheduler.cancelAll()
```

# Migrating from remo

[remo](https://github.com/appKODE/remo) (`ru.kode:remo`, `ru.kode.android:remo`, `ReactiveModel`) is deprecated,
reactive-tasks replaces it. Both run suspend "jobs" and let you observe their state, results and errors;
reactive-tasks drops the `ReactiveModel` base class and `kotlin-result`, and adds `startLatest`, `startSuspended`,
`restart` and iOS.

```kotlin
// before
implementation("ru.kode:remo:1.2.0") // or ru.kode:remo:9.0.1, ru.kode.android:remo:9.1.0
// after
implementation("ru.kode:reactive-tasks:1.3.0")
```

remo 1.2.0 and 9.x differ only in how a model gets its scope; tasks and `JobFlow` are the same.
Below `scheduler` is a `Scheduler`, `fetch` is a `TaskHandle<A, R>`.

## API map

| remo 1.2.0 | remo 9.0.1 | reactive-tasks 1.3.0 |
|---|---|---|
| `class M : ReactiveModel(dispatcher)` | `class M(scope) : ReactiveModel(scope)` | `class M(scheduler: Scheduler)`, no base class |
| `model.start(parentScope)`, `Job.cancel()` to stop, can be started again with a new scope | constructor `scope`, cancel it to stop | `Scheduler(scope)`, cancel `scope` to stop for good; to stop and start again: `parentScope.coroutineContext.job.invokeOnCompletion { scheduler.cancelAll() }` on each start |
| `onPostStart()` | `init {}` (`onPostStart` deprecated) | 1.x: a `start(parentScope)` function of the model, called where `model.start` was (runs on every start); 9.x: `init {}` |
| `scope.launch {}` inside the model | same | `scheduler.scope.launch {}` |
| `uncaughtExceptions: Flow<Throwable>` | same | `Scheduler(onUncaughtException = { taskId, taskName, e -> })` |
| `ReactiveModel(errorMapper = …)`, `task(errorMapper = …)`, `mapErrors {}` | same | map in the flow: `errors(fetch).map { … }`, or catch inside the task body |
| `val fetch = task { id: String -> api.fetch(id) }` | same | `val fetch = scheduler.registerTask("fetch") { id: String -> api.fetch(id) }` |
| `task { -> api.fetch() }` (`Task0`) | same | `registerTask("fetch") { _: Unit -> api.fetch() }`, `start(fetch, Unit)` |
| `task { a: A, b: B -> }` (`Task2` / `Task3`) | same | one data class argument: `registerTask("t") { (a, b): Args -> }` |
| `taskIn(watchContext) { }`, `WatchContext.executeInModelScope { }` | same | `registerTask` + `start` |
| one `WatchContext` shared by several tasks | same | `merge(jobStates(a), jobStates(b))`, or one task with a sealed argument |
| `fetch.start(id)`: `Job` | same | `scheduler.start(fetch, id)`: `JobHandle` |
| `job.cancel()` | same | `scheduler.cancel(jobHandle)`, `scheduler.cancel(fetch)` (all jobs of the task) |
| `start(id, queueingStrategy = Disallow)` (default, throws if a job runs) | same | no equivalent, `start` runs jobs in parallel |
| `start(id, queueingStrategy = SkipNew)` | same | `if (!scheduler.isRunning(fetch)) scheduler.start(fetch, id)` |
| restart a running job with a new argument | same | `scheduler.startLatest(fetch, id)`: cancels running jobs of the task |
| re-run with the last argument | same | `scheduler.restart(fetch)` |
| `start(id, StartScheduled.Lazily())` | same | `flow { emit(scheduler.startSuspended(fetch, id)) }`, or `shareIn(…).onSubscription { scheduler.start(fetch, id) }` |
| — | — | `startSuspended(fetch, id)`: suspends until done, returns `JobState` (`state.resultOf(fetch)`: `R?`) |
| `fetch.jobFlow` | same | `scheduler.jobStates(fetch)`: `Flow<JobState>` |
| `fetch.jobFlow.state`: `Idle` / `Running` (progress) | same | `scheduler.runningStates(fetch)`: `Flow<Boolean>`, current value on subscribe, `false` after a cancel |
| `fetch.jobFlow.successResults()` | same | `scheduler.successResults(fetch)`: `Flow<R>` |
| `fetch.jobFlow.errors()` | same | `scheduler.errors(fetch)`: `Flow<Throwable>` |
| `fetch.jobFlow.results()`: `Result<R, Throwable>` | same | `jobStates(fetch)`: `JobState` with `runState`, `error`, `resultOf(fetch)`: `R?` |
| `results(replayLast = true)` | same | `scheduler.finishedStates(fetch, replayLast = true)`: replays the last finished job even while a new one runs |
| `successResults(replayLast = true)`, `errors(replayLast = true)` | same | `successResults(fetch, replayFinished = true)`, `errors(fetch, replayFinished = true)` |
| `mapSuccessResults {}` | same | `successResults(fetch).map {}` |

## Example

```kotlin
// remo
class UserListModel : ReactiveModel() {
  val fetch = task { sort: Sort -> api.fetch(sort) }
}
model.fetch.jobFlow.successResults().collect { render(it) }
model.fetch.start(Sort.Ascending)

// reactive-tasks
class UserListModel(private val scheduler: Scheduler) {
  val fetch = scheduler.registerTask("fetch") { sort: Sort -> api.fetch(sort) }
  val users: Flow<List<User>> = scheduler.successResults(fetch)
  fun fetch(sort: Sort) = scheduler.start(fetch, sort)
}
```
