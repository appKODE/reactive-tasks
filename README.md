# Reactive Tasks

A library for scheduling, executing and observing repeateble reactive tasks.

# Installation

Kotlin Multiplatform: JVM, Android, iOS (`iosX64`, `iosArm64`, `iosSimulatorArm64`).

```kotlin
implementation("ru.kode:reactive-tasks:1.0.0")
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
// There's also a simple utility which wraps listener
// into Flow. It is fairly simple implementation, if you need
// something more involved, check how observeStateChanges is implemented, 
// use it as a reference
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
implementation("ru.kode:reactive-tasks:1.0.0")
```

## API map

| remo | reactive-tasks |
|---|---|
| `class UserModel : ReactiveModel()` | `class UserModel(scheduler: Scheduler)`, no base class |
| `model.start(parentScope)` (1.x) / constructor scope (9.x), `Job.cancel()` to stop | `Scheduler(scope)`, cancel `scope` to stop |
| `scope.launch { }` inside the model | `scheduler.scope.launch { }` |
| `uncaughtExceptions: Flow<Throwable>` | `Scheduler(onUncaughtException = { taskId, name, e -> })` |
| `val fetch = task { id: String -> api.fetch(id) }` | `val fetch = scheduler.registerTask("fetch") { id: String -> api.fetch(id) }` |
| `task { -> api.fetch() }` | `registerTask("fetch") { _: Unit -> api.fetch() }`, `start(fetch, Unit)` |
| `task { a: A, b: B -> }` (2–3 params) | one argument: `registerTask("t") { (a, b): Pair<A, B> -> }` or a data class |
| `fetch.start(id)`: `Job` | `scheduler.start(fetch, id)`: `JobHandle` |
| `job.cancel()` | `scheduler.cancel(jobHandle)` / `scheduler.cancel(fetch)` (all jobs of the task) |
| `QueueingStrategy.Disallow` (default: fails if a job runs) | no equivalent, `start` runs jobs in parallel |
| `QueueingStrategy.SkipNew` | check `isRunning(fetch)` (below) before `start` |
| — | `startLatest(fetch, id)`: cancels running jobs of the task (search-as-you-type) |
| — | `startSuspended(fetch, id)`: suspends until done, returns `JobState` |
| `WatchContext()` + `executeInModelScope { }` | `registerTask` + `start` |
| one `WatchContext` shared by several jobs (one loader) | several tasks, `merge(jobStates(a), jobStates(b))`, or one task with a sealed argument |
| `fetch.jobFlow.state`: `Idle` / `Running` | `jobStates(fetch).map { it.runState == RunState.Running }` |
| `fetch.jobFlow.successResults()` | `jobStates(fetch).filter { it.runState == RunState.FinishedSuccess }.map { it.result as R }` |
| `fetch.jobFlow.errors()` | `jobStates(fetch).filter { it.runState == RunState.FinishedError }.map { it.error!! }` |
| `fetch.jobFlow.results()`: `Result<R, Throwable>` | `JobState` with `runState`, `result`, `error` |
| `results(replayLast = true)` | no replay: `shareIn(scope, started, replay = 1)` or `stateIn`; last states are in `scheduler.taskJobState` |
| `StartScheduled.Lazily(...)` | subscribe first: `shareIn(...).onSubscription { scheduler.start(fetch, id) }` |
| `mapErrors {}` / `WatchContext(errorMapper = …)` | map in the flow, or catch inside the task body |

## Observing one task

`Scheduler.observeStateChanges(handle)` (1.0.0) emits states of **all** tasks of the scheduler and closes on the
first cancelled job of any task — `startLatest` cancels jobs, so the flow would end. Until it is fixed, observe with
this helper, which behaves like remo's `jobFlow`:

```kotlin
@OptIn(ExperimentalUuidApi::class)
fun Scheduler.jobStates(task: TaskHandle<*, *>): Flow<JobState> = callbackFlow {
  val listener = object : TaskStateChangeListener {
    override suspend fun onJobStateChanged(state: JobState) {
      if (state.taskId == task.id) send(state)
    }

    override fun onJobCancelled(taskId: Uuid, jobId: Uuid) = Unit
  }
  addTaskStateChangeListener(listener)
  awaitClose { removeTaskStateChangeListener(listener) }
}

fun Scheduler.isRunning(task: TaskHandle<*, *>): Boolean =
  taskJobState.values.any { it.taskId == task.id && it.runState == RunState.Running }
```

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
  val users: Flow<List<User>> = scheduler.jobStates(fetch)
    .filter { it.runState == RunState.FinishedSuccess }
    .map { it.result as List<User> }
  fun fetch(sort: Sort) = scheduler.start(fetch, sort)
}
```
