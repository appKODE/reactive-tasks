package ru.kode.reactivetasks

import java.util.concurrent.CountDownLatch
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.onSubscription
import kotlinx.coroutines.flow.shareIn
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import ru.kode.reactivetasks.RunState
import ru.kode.reactivetasks.Scheduler
import ru.kode.reactivetasks.JobState
import ru.kode.reactivetasks.TaskStateChangeListener
import ru.kode.reactivetasks.observeStateChanges
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

@OptIn(ExperimentalUuidApi::class)
fun testBasicStart(scheduler: Scheduler) {
    val latch = CountDownLatch(5)

    val fetchHandle = scheduler.registerTask("fetch", body = { id: String ->
        println("fetch started, user id = $id")
        delay(3000)
        if (id.contains("500")) {
            error("hello")
        } else {
            println("fetch finished for arg=$id")
        }
        id + "_result"
    })

    scheduler.addTaskStateChangeListener(object : TaskStateChangeListener {
        override suspend fun onJobStateChanged(state: JobState) {
            println("[change-listener] state: $state")
            if (state.taskId == fetchHandle.id) {
                if (state.runState == RunState.FinishedError || state.runState == RunState.FinishedSuccess) {
                    latch.countDown()
                    if (state.argument == "user-id-2200") {
                        scheduler.restart(fetchHandle)
                    }
                }
            }
        }

        // TODO mention in docs that implementation should be fast: copy from invokeOnCompletion docs
        override fun onJobCancelled(taskId: Uuid, jobId: Uuid) {
            println("task is cancelled: jobId = $jobId, taskId = $taskId")
        }
    })

    scheduler.start(fetchHandle, "user-id-100", "my tag")
    scheduler.start(fetchHandle, "user-id-2200")
    scheduler.start(fetchHandle, "user-id-500")

    val scope = CoroutineScope(Dispatchers.IO)
    scope.launch {
        val result = scheduler.startSuspended(fetchHandle, "user-id-800")
        println("received suspended result $result")
        latch.countDown()
    }

    latch.await()
}

@OptIn(ExperimentalUuidApi::class)
fun testStartLatest(scheduler: Scheduler) {
    val latch = CountDownLatch(1)

    val queryHandle = scheduler.registerTask("query", body = { query: String ->
        println("query started, query = $query")
        delay(3000)
        println("query finished")
    })

    scheduler.addTaskStateChangeListener(object : TaskStateChangeListener {
        override suspend fun onJobStateChanged(state: JobState) {
            println("state: $state")
            if (state.taskId == queryHandle.id) {
                if (state.runState == RunState.FinishedError || state.runState == RunState.FinishedSuccess) {
                    if (state.argument == "final-query") {
                        latch.countDown()
                    }
                }
            }
        }

        // TODO mention in docs that implementation should be fast: copy from invokeOnCompletion docs
        override fun onJobCancelled(taskId: Uuid, jobId: Uuid) {
            println("task is cancelled: jobId = $jobId, taskId = $taskId")
        }
    })

    val scope = CoroutineScope(Dispatchers.IO)
    scope.launch {
        for (i in 0 until 5) {
            if (i != 0) {
                delay(30)
            }
            scheduler.startLatest(queryHandle, "query$i")
        }
        delay(300)
        scheduler.startLatest(queryHandle, "final-query")
    }
    latch.await()
}

@OptIn(ExperimentalUuidApi::class)
fun testFlowAdapters(scheduler: Scheduler, observeScope: CoroutineScope) {
    val handle = scheduler.registerTask("fetch", body = { query: String ->
        println("fetch started")
        delay(3000)
        println("fetch finished")
    })

    val taskStateChanges = scheduler.observeStateChanges(handle)
    runBlocking {
        taskStateChanges
            .shareIn(observeScope, SharingStarted.Lazily)
            .onSubscription {
                println("Subscribed!")
                scheduler.start(handle, "300")
            }
            .collect {
                println("Received state $it")
            }
    }
}

@OptIn(ExperimentalUuidApi::class)
fun main() {
    val schedulerScope = CoroutineScope(Dispatchers.Default)
    val mainScope = CoroutineScope(Dispatchers.IO)
    val scheduler = Scheduler(scope = schedulerScope)

    val mode = "flow"
    if (mode == "basic") {
        testBasicStart(scheduler)
    } else if (mode == "flow") {
        testFlowAdapters(scheduler, mainScope)
    } else {
        testStartLatest(scheduler)
    }
}
