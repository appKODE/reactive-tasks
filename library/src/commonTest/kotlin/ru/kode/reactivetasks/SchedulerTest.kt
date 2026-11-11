@file:OptIn(ExperimentalUuidApi::class, ExperimentalCoroutinesApi::class)

package ru.kode.reactivetasks

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withContext
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.coroutines.ContinuationInterceptor
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

internal sealed interface Event {
    data class Changed(val state: JobState) : Event
    data class Cancelled(val taskId: Uuid, val jobId: Uuid) : Event
}

internal class RecordingListener : TaskStateChangeListener {
    val events = mutableListOf<Event>()

    val states: List<JobState> get() = events.filterIsInstance<Event.Changed>().map { it.state }
    val cancelled: List<Event.Cancelled> get() = events.filterIsInstance<Event.Cancelled>()

    override suspend fun onJobStateChanged(state: JobState) {
        events += Event.Changed(state)
    }

    override fun onJobCancelled(taskId: Uuid, jobId: Uuid) {
        events += Event.Cancelled(taskId, jobId)
    }
}

internal class UncaughtRecorder {
    val errors = mutableListOf<Triple<Uuid, String, Throwable>>()
    val handler: (Uuid, String, Throwable) -> Unit = { id, name, e -> errors += Triple(id, name, e) }
}

internal fun TestScope.testScheduler(uncaught: UncaughtRecorder = UncaughtRecorder()) = Scheduler(
    scope = backgroundScope,
    defaultDispatcher = StandardTestDispatcher(testScheduler),
    onUncaughtException = uncaught.handler,
)

class SchedulerTest {

    // region registerTask / handles

    @Test
    fun registerTaskReturnsNamedHandleWithUniqueId() = runTest {
        val scheduler = testScheduler()
        val a = scheduler.registerTask("fetch") { _: String -> }
        val b = scheduler.registerTask("fetch") { _: String -> }

        assertEquals("fetch", a.name)
        assertNotEquals(a.id, b.id)
        assertNotEquals(Uuid.NIL, a.id)
        assertEquals("TaskHandle[name=fetch,id=${a.id}]", a.toString())

        val job = scheduler.start(a, "x")
        assertEquals("JobHandle[id=${job.id}]", job.toString())
        assertSame<Any>(a, job.taskHandle)
    }

    @Test
    fun nilToStrings() {
        assertEquals("TaskHandle.NIL", TaskHandle.NIL.toString())
        assertEquals("JobHandle.NIL", JobHandle.NIL.toString())
        assertEquals("JobState.NIL", JobState.NIL.toString())
    }

    @Test
    fun bodyRunsOnGivenDispatcherOrDefault() = runTest {
        val scheduler = testScheduler()
        val custom = StandardTestDispatcher(testScheduler, name = "custom")
        var seenCustom: ContinuationInterceptor? = null
        var seenDefault: ContinuationInterceptor? = null
        val onCustom = scheduler.registerTask("custom", custom) { _: Unit ->
            seenCustom = currentCoroutineContext()[ContinuationInterceptor]
        }
        val onDefault = scheduler.registerTask("default") { _: Unit ->
            seenDefault = currentCoroutineContext()[ContinuationInterceptor]
        }

        scheduler.start(onCustom, Unit)
        scheduler.start(onDefault, Unit)
        advanceUntilIdle()

        assertSame(custom, seenCustom)
        assertSame(scheduler.defaultDispatcher, seenDefault)
    }

    // endregion

    // region start

    @Test
    fun startReportsRunningThenSuccess() = runTest {
        val scheduler = testScheduler()
        val listener = RecordingListener().also(scheduler::addTaskStateChangeListener)
        val task = scheduler.registerTask("len") { s: String -> delay(100); s.length }

        val job = scheduler.start(task, "abc", tag = "t")
        advanceUntilIdle()

        val running = JobState(job.id, task.id, "abc", RunState.Running, tag = "t")
        val success = JobState(job.id, task.id, "abc", RunState.FinishedSuccess, result = 3, tag = "t")
        assertEquals(listOf<Event>(Event.Changed(running), Event.Changed(success)), listener.events)
        assertEquals(success, scheduler.taskJobState[job.id])
        assertEquals(job.id, listener.states.first().jobId)
    }

    @Test
    fun bodyExceptionBecomesFinishedError() = runTest {
        val uncaught = UncaughtRecorder()
        val scheduler = testScheduler(uncaught)
        val listener = RecordingListener().also(scheduler::addTaskStateChangeListener)
        val boom = IllegalStateException("boom")
        val task = scheduler.registerTask<Unit, Unit>("fail") { throw boom }

        val job = scheduler.start(task, Unit)
        advanceUntilIdle()

        assertEquals(listOf(RunState.Running, RunState.FinishedError), listener.states.map { it.runState })
        val final = scheduler.taskJobState.getValue(job.id)
        assertSame(boom, final.error)
        assertNull(final.result)
        assertTrue(uncaught.errors.isEmpty())
        assertTrue(listener.cancelled.isEmpty())
    }

    @Test
    fun twoStartsRunConcurrently() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("slow") { _: Int -> delay(100) }

        val first = scheduler.start(task, 1)
        val second = scheduler.start(task, 2)
        runCurrent()
        assertEquals(RunState.Running, scheduler.taskJobState.getValue(first.id).runState)
        assertEquals(RunState.Running, scheduler.taskJobState.getValue(second.id).runState)

        advanceUntilIdle()
        assertEquals(RunState.FinishedSuccess, scheduler.taskJobState.getValue(first.id).runState)
        assertEquals(RunState.FinishedSuccess, scheduler.taskJobState.getValue(second.id).runState)
    }

    @Test
    fun listenerExceptionGoesToOnUncaughtException() = runTest {
        val uncaught = UncaughtRecorder()
        val scheduler = testScheduler(uncaught)
        val listener = RecordingListener()
        scheduler.addTaskStateChangeListener(object : TaskStateChangeListener {
            override suspend fun onJobStateChanged(state: JobState) = throw IllegalStateException("listener")
            override fun onJobCancelled(taskId: Uuid, jobId: Uuid) = Unit
        })
        scheduler.addTaskStateChangeListener(listener)
        val task = scheduler.registerTask("named") { _: Unit -> }

        scheduler.start(task, Unit)
        advanceUntilIdle()

        val (id, name, error) = uncaught.errors.single()
        assertEquals(task.id, id)
        assertEquals("named", name)
        assertIs<IllegalStateException>(error)
        assertEquals("listener", error.message)
        // the throwing listener aborts the job before later listeners are notified
        assertTrue(listener.events.isEmpty())
    }

    // endregion

    // region startLatest

    @Test
    fun startLatestCancelsPreviousJob() = runTest {
        val scheduler = testScheduler()
        val listener = RecordingListener().also(scheduler::addTaskStateChangeListener)
        val task = scheduler.registerTask("search") { q: String -> delay(1000); q }

        val first = scheduler.start(task, "a")
        runCurrent()
        val second = scheduler.startLatest(task, "ab")
        advanceUntilIdle()

        assertEquals(
            listOf<Event>(
                Event.Changed(JobState(first.id, task.id, "a", RunState.Running)),
                Event.Cancelled(task.id, first.id),
                Event.Changed(JobState(second.id, task.id, "ab", RunState.Running)),
                Event.Changed(JobState(second.id, task.id, "ab", RunState.FinishedSuccess, result = "ab")),
            ),
            listener.events,
        )
        assertTrue(first.job.isCancelled)
        // quirk: a cancelled job's state stays Running forever
        assertEquals(RunState.Running, scheduler.taskJobState.getValue(first.id).runState)
    }

    // endregion

    // region startSuspended

    @Test
    fun startSuspendedReturnsFinalState() = runTest {
        val scheduler = testScheduler()
        val listener = RecordingListener().also(scheduler::addTaskStateChangeListener)
        val boom = IllegalArgumentException("bad")
        val task = scheduler.registerTask("parse") { s: String -> if (s == "bad") throw boom else s.toInt() }

        val ok = scheduler.startSuspended(task, "42", tag = "t")
        assertEquals(RunState.FinishedSuccess, ok.runState)
        assertEquals(42, ok.result)
        assertEquals("t", ok.tag)
        assertEquals(ok, scheduler.taskJobState[ok.jobId])

        val failed = scheduler.startSuspended(task, "bad")
        assertEquals(RunState.FinishedError, failed.runState)
        assertSame(boom, failed.error)

        assertEquals(
            listOf(RunState.Running, RunState.FinishedSuccess, RunState.Running, RunState.FinishedError),
            listener.states.map { it.runState },
        )
    }

    @Test
    fun startSuspendedCancellationThrowsWithoutOnJobCancelled() = runTest {
        val scheduler = testScheduler()
        val listener = RecordingListener().also(scheduler::addTaskStateChangeListener)
        val task = scheduler.registerTask("slow") { _: Unit -> delay(1000) }
        var thrown: Throwable? = null

        val caller = launch {
            try {
                scheduler.startSuspended(task, Unit)
            } catch (e: CancellationException) {
                thrown = e
                throw e
            }
        }
        runCurrent()
        caller.cancel()
        advanceUntilIdle()

        assertIs<CancellationException>(thrown)
        assertTrue(caller.isCancelled)
        assertEquals(listOf(RunState.Running), listener.states.map { it.runState })
        assertTrue(listener.cancelled.isEmpty())
    }

    // endregion

    // region restart

    @Test
    fun restartTaskReusesMostRecentArgumentAndDropsTag() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("echo") { s: String -> s }

        scheduler.start(task, "a", tag = "t")
        val r1 = scheduler.restart(task)
        advanceUntilIdle()
        assertEquals("a", scheduler.taskJobState.getValue(r1.id).result)
        assertNull(scheduler.taskJobState.getValue(r1.id).tag)

        scheduler.startLatest(task, "b", tag = "t")
        val r2 = scheduler.restart(task)
        advanceUntilIdle()
        assertEquals("b", scheduler.taskJobState.getValue(r2.id).argument)

        scheduler.startSuspended(task, "c", tag = "t")
        val r3 = scheduler.restart(task)
        advanceUntilIdle()
        assertEquals("c", scheduler.taskJobState.getValue(r3.id).argument)
        assertNull(scheduler.taskJobState.getValue(r3.id).tag)
    }

    @Test
    fun restartJobReusesThatJobsArgument() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("echo") { s: String -> s }

        val first = scheduler.start(task, "first")
        scheduler.start(task, "second")
        val restarted = scheduler.restart(first)
        advanceUntilIdle()

        assertNotEquals(first.id, restarted.id)
        assertSame<Any>(task, restarted.taskHandle)
        assertEquals("first", scheduler.taskJobState.getValue(restarted.id).result)
    }

    @Test
    fun restartWithoutPreviousStartReturnsNil() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("never") { _: String -> }

        assertSame(JobHandle.NIL, scheduler.restart(task))
        assertSame(JobHandle.NIL, scheduler.restart(JobHandle.NIL))
    }

    @Test
    fun restartAfterNullArgument() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("nullable") { _: String? -> }

        val started = runCatching { scheduler.start(task, null) }
        // quirk: JVM/Android argumentCache is a ConcurrentHashMap, which rejects null values, so start throws NPE.
        // iOS LockedMap accepts null, but restart treats a null argument as "never started" and returns NIL.
        val error = started.exceptionOrNull()
        if (error != null) {
            assertIs<NullPointerException>(error)
        } else {
            assertSame(JobHandle.NIL, scheduler.restart(task))
            assertSame(JobHandle.NIL, scheduler.restart(started.getOrThrow()))
        }
    }

    // endregion

    // region cancel / cancelAndJoin

    @Test
    fun cancelTaskCancelsAllItsJobsOnly() = runTest {
        val scheduler = testScheduler()
        val listener = RecordingListener().also(scheduler::addTaskStateChangeListener)
        val task = scheduler.registerTask("slow") { _: Int -> delay(1000) }
        val other = scheduler.registerTask("other") { _: Int -> delay(1000) }

        val a = scheduler.start(task, 1)
        val b = scheduler.start(task, 2)
        val c = scheduler.start(other, 3)
        runCurrent()
        scheduler.cancel(task)
        advanceUntilIdle()

        assertEquals(setOf(Event.Cancelled(task.id, a.id), Event.Cancelled(task.id, b.id)), listener.cancelled.toSet())
        assertEquals(2, listener.cancelled.size)
        assertTrue(a.job.isCancelled && b.job.isCancelled)
        assertEquals(RunState.FinishedSuccess, scheduler.taskJobState.getValue(c.id).runState)
    }

    @Test
    fun cancelJobCancelsOnlyThatJob() = runTest {
        val scheduler = testScheduler()
        val listener = RecordingListener().also(scheduler::addTaskStateChangeListener)
        val task = scheduler.registerTask("slow") { _: Int -> delay(1000) }

        val a = scheduler.start(task, 1)
        val b = scheduler.start(task, 2)
        runCurrent()
        scheduler.cancel(a)
        advanceUntilIdle()

        assertEquals(listOf(Event.Cancelled(task.id, a.id)), listener.cancelled)
        assertEquals(RunState.FinishedSuccess, scheduler.taskJobState.getValue(b.id).runState)
    }

    @Test
    fun cancelAndJoinTaskWaitsForCompletion() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("slow") { _: Int -> delay(1000) }

        val a = scheduler.start(task, 1)
        val b = scheduler.start(task, 2)
        runCurrent()
        scheduler.cancelAndJoin(task)

        assertTrue(a.job.isCancelled && a.job.isCompleted)
        assertTrue(b.job.isCancelled && b.job.isCompleted)
    }

    @Test
    fun cancelAndJoinJobWaitsForCompletion() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("slow") { _: Int -> delay(1000) }

        val a = scheduler.start(task, 1)
        val b = scheduler.start(task, 2)
        runCurrent()
        scheduler.cancelAndJoin(a)

        assertTrue(a.job.isCancelled && a.job.isCompleted)
        assertTrue(b.job.isActive)
    }

    @Test
    fun cancellingSchedulerScopeCancelsJobs() = runTest {
        val parent = Job()
        val scheduler = Scheduler(CoroutineScope(parent), StandardTestDispatcher(testScheduler), UncaughtRecorder().handler)
        val listener = RecordingListener().also(scheduler::addTaskStateChangeListener)
        val task = scheduler.registerTask("slow") { _: Int -> delay(1000) }

        val job = scheduler.start(task, 1)
        runCurrent()
        parent.cancel()
        advanceUntilIdle()

        assertTrue(job.job.isCancelled)
        assertEquals(listOf(Event.Cancelled(task.id, job.id)), listener.cancelled)
    }

    // endregion

    // region listeners

    @Test
    fun removedListenerGetsNoMoreEvents() = runTest {
        val scheduler = testScheduler()
        val listener = RecordingListener().also(scheduler::addTaskStateChangeListener)
        val task = scheduler.registerTask("t") { _: Int -> }

        scheduler.start(task, 1)
        advanceUntilIdle()
        scheduler.removeTaskStateChangeListener(listener)
        scheduler.start(task, 2)
        advanceUntilIdle()

        assertEquals(listOf(1, 1), listener.states.map { it.argument })
    }

    @Test
    fun mutatingListenersInsideCallbackIsSafe() = runTest {
        val scheduler = testScheduler()
        val late = RecordingListener()
        val selfRemoving = object : TaskStateChangeListener {
            var calls = 0
            override suspend fun onJobStateChanged(state: JobState) {
                calls++
                scheduler.removeTaskStateChangeListener(this)
                scheduler.addTaskStateChangeListener(late)
            }
            override fun onJobCancelled(taskId: Uuid, jobId: Uuid) = Unit
        }
        val bystander = RecordingListener()
        scheduler.addTaskStateChangeListener(selfRemoving)
        scheduler.addTaskStateChangeListener(bystander)
        val task = scheduler.registerTask("t") { _: Int -> }

        scheduler.start(task, 1)
        advanceUntilIdle()

        assertEquals(1, selfRemoving.calls)
        assertEquals(listOf(RunState.Running, RunState.FinishedSuccess), bystander.states.map { it.runState })
        // added during the Running dispatch: misses that snapshot, sees the next one
        assertEquals(listOf(RunState.FinishedSuccess), late.states.map { it.runState })
    }

    // endregion

    // region cancelAll

    @Test
    fun cancelAllCancelsJobsAndCoroutinesAndKeepsSchedulerUsable() = runTest {
        val scheduler = testScheduler()
        val listener = RecordingListener().also(scheduler::addTaskStateChangeListener)
        val task = scheduler.registerTask("t") { _: Int -> delay(1_000) }
        var ticks = 0
        fun launchTicker() = scheduler.scope.launch {
            while (true) {
                delay(100)
                ticks++
            }
        }

        val job = scheduler.start(task, 1)
        val ticker = launchTicker()
        runCurrent()
        assertTrue(scheduler.isRunning(task))

        scheduler.cancelAll()
        runCurrent()

        assertTrue(job.job.isCancelled)
        assertTrue(ticker.isCancelled)
        assertTrue(!scheduler.isRunning(task))
        assertEquals(listOf(job.id), listener.cancelled.map { it.jobId })

        // the scheduler is still usable: tasks start and coroutines launch again
        scheduler.start(task, 2)
        launchTicker()
        advanceTimeBy(1_050)
        runCurrent()

        assertEquals(RunState.FinishedSuccess, listener.states.last { it.argument == 2 }.runState)
        assertEquals(10, ticks)
    }

    // endregion
    // region startIfIdle, startSuspendedIfIdle, bindTo, results

    @Test
    fun startIfIdleDropsStartWhileRunning() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("t") { _: Int -> delay(100) }

        val first = scheduler.startIfIdle(task, 1)
        val second = scheduler.startIfIdle(task, 2)
        runCurrent()

        assertTrue(first != null)
        assertNull(second)
        advanceUntilIdle()
        assertTrue(scheduler.startIfIdle(task, 3) != null)
    }

    @Test
    fun startIfIdleUnitOverloadStartsWithUnit() = runTest {
        val scheduler = testScheduler()
        val listener = RecordingListener().also(scheduler::addTaskStateChangeListener)
        val task = scheduler.registerTask("t") { _: Unit -> }

        scheduler.startIfIdle(task)
        advanceUntilIdle()

        assertEquals(Unit, listener.states.last().argument)
        assertEquals(RunState.FinishedSuccess, listener.states.last().runState)
    }

    @Test
    fun concurrentStartIfIdleStartsExactlyOneJob() = runTest {
        val scheduler = Scheduler(CoroutineScope(SupervisorJob()), Dispatchers.Default)
        val gate = CompletableDeferred<Unit>()
        val task = scheduler.registerTask("t") { _: Int -> gate.await() }

        val started = withContext(Dispatchers.Default) {
            (1..200).map { async { scheduler.startIfIdle(task, it) } }.awaitAll()
        }

        assertEquals(1, started.count { it != null })
        gate.complete(Unit)
        scheduler.cancelAll()
    }

    @Test
    fun startSuspendedIfIdleRunsWhenIdleAndDropsWhileRunning() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("t") { x: Int -> x * 2 }
        val slow = scheduler.registerTask("slow") { _: Int -> delay(100) }

        val state = scheduler.startSuspendedIfIdle(task, 21)
        assertEquals(RunState.FinishedSuccess, state?.runState)
        assertEquals(42, state?.resultOf(task))

        scheduler.start(slow, 1)
        runCurrent()
        assertNull(scheduler.startSuspendedIfIdle(slow, 2))
    }

    @Test
    fun bindToCancelsAllWhenScopeCompletesAndSchedulerStaysUsable() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("t") { _: Int -> delay(1_000) }
        val bound = Job()
        scheduler.bindTo(CoroutineScope(bound))

        val job = scheduler.start(task, 1)
        runCurrent()
        assertTrue(scheduler.isRunning(task))

        bound.cancel()
        runCurrent()

        assertTrue(job.job.isCancelled)
        assertTrue(!scheduler.isRunning(task))
        assertTrue(scheduler.startIfIdle(task, 2) != null)
    }

    @Test
    fun resultsEmitsSuccessAndFailureAndReplaysLast() = runTest {
        val scheduler = testScheduler()
        val boom = IllegalStateException("boom")
        val task = scheduler.registerTask("t") { x: Int -> if (x < 0) throw boom else x }

        scheduler.startSuspended(task, 1)
        scheduler.startSuspended(task, -1)

        // replayLast is true by default: only the most recent finished job is replayed
        val replayed = scheduler.results(task).first()
        assertSame(boom, replayed.exceptionOrNull())

        val live = mutableListOf<Result<Int>>()
        val collector = launch { scheduler.results(task, replayLast = false).toList(live) }
        runCurrent()
        scheduler.startSuspended(task, 7)
        runCurrent()
        collector.cancel()

        assertEquals(listOf(7), live.map { it.getOrThrow() })
    }

    // endregion
}
