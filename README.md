# Reactive Tasks

A library for scheduling, executing and observing repeateble reactive tasks.

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
```
