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
        override fun onJobCancelled(taskId: Uuid, startId: Uuid) {
            println("task is cancelled: startId = $startId, taskId = $taskId")
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
        override fun onJobCancelled(taskId: Uuid, startId: Uuid) {
            println("task is cancelled: startId = $startId, taskId = $taskId")
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

    println("ready to start")
    val mode = "flow"
    if (mode == "basic") {
        testBasicStart(scheduler)
    } else if (mode == "flow") {
        testFlowAdapters(scheduler, mainScope)
    } else {
        testStartLatest(scheduler)
    }

    // val fetchJob = scheduler.observeAsState(fetchHandle)
    //     .onSubscription {
    //         scheduler.start(fetchHandle, "user-id-100")
    //         scheduler.start(fetchHandle, "user-id-2200")
    //         scheduler.start(fetchHandle, "user-id-500")
    //         scheduler.start(fetchHandle, "user-id-700")
    //         scheduler.start(fetchHandle, "user-id-800")
    //         scheduler.start(fetchHandle, "user-id-900")
    //     }
    //     .onEach { state ->
    //         println(state.toString())
    //     }
    //     .launchIn(mainScope)

    // val taskHandle1 = scheduler.registerSerialTask { id: String ->
    //     repositoryFetchUser(id)
    // }

    // val taskHandle2 = scheduler.registerParallelTask { id: String ->
    //     repositoryFetchUser(id)
    // }

    // val taskHandle3 = scheduler.registerSingleTask { id: String ->
    //     repositoryFetchUser(id)
    // }

    // scheduler.observe(taskHandle1)
    //     .onEach { state ->
    //     }
    //     .launchIn(scope)

    // val startHandle1 = scheduler.start(taskHandle2, "user-id-100")
    // val startHandle2 = scheduler.start(taskHandle2, "user-id-200")
    // val startHandle3 = scheduler.start(taskHandle3, "user-id-300")
    // scheduler.cancel(taskHandle1) // cancels all instances of running task
    // scheduler.cancel(startHandle1) // cancels a particular instance of task

    // scheduler.start(taskHandle2, "user-id-100")
    // scheduler.start(taskHandle2, "user-id-200")
    // scheduler.start(taskHandle2, "user-id-300")
    // scheduler.cancel(taskHandle2)

    // //
    // // Usecase:  with previous arguments
    // //
    // scheduler.restart(startHandle1)

    // //
    // // Usecase: restart with new argument... what does this mean? cancel if some instance is running?
    // //
    // scheduler.startLatest(taskHandle1, "user-id-200")

    // scheduler.startQueued(startHandle2, "user-id-200", condition = { result -> result.error == null })
    // // OR
    // val queuedHandle1 = scheduler.queue(taskHandle2).addFirst("user-id-200")
    // val queuedHandle2 = scheduler.queue(taskHandle2).addLast("user-id-200")
    // // (queue starts to be used right after all running tasks are finished (they may be parallel), queued handles will lack Jobs)

    // // any other schemes can be done using scheduler.cancelAndJoin(), scheduler.startSuspended()

    // //
    // // Usecase: start suspended
    // //
    // {
    //     // TODO
    // }

    // // Usecase: distinguish between starts when subscribing to state.
    // // 1. Screen1 starts TASK-X, subscribes, task finishes
    // // 2. Screen2 starts TASK-X, subscribes
    // //
    // // Screen2 will receive result which was received on step1, and this is unwanted, Screen2 wants to
    // // only receive results since it started TASK-X. Doing skip(1) will not work, because Screen2 still wants
    // // to have state-like behaviour of subscription
    // // Current suggestion: allow to additionally mark task start instances with some code/id, then it can be
    // // used when subscribing to filter only correspondingly marked results (filtering should be left to the user,
    // // not contained in lib, to avoid method bloat)
    // {
    //     // TODO
    // }

    // //
    // // Restart tasks
    // //
    // {
    //     // TODO
    // }

    // //
    // // Launch when subscribed to by [count] subscribers???
    // //
    // {
    //     // TODO
    // }

}
