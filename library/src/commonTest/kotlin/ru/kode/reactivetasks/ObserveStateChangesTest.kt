@file:OptIn(ExperimentalUuidApi::class, ExperimentalCoroutinesApi::class)

package ru.kode.reactivetasks

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

class ObserveStateChangesTest {

    private class Collected {
        lateinit var job: Job
        val states = mutableListOf<JobState>()
        var error: Throwable? = null
    }

    private fun TestScope.collect(flow: Flow<JobState>): Collected {
        val collected = Collected()
        // unconfined: advanceUntilIdle() does not run backgroundScope work, so the collector must resume eagerly
        collected.job = backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) {
            try {
                flow.collect { collected.states += it }
            } catch (e: CancellationException) {
                collected.error = e
                throw e
            }
        }
        runCurrent() // subscribe before the test starts anything
        return collected
    }

    private fun <T> TestScope.values(flow: Flow<T>): List<T> {
        val values = mutableListOf<T>()
        backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) { flow.collect { values += it } }
        runCurrent()
        return values
    }

    @Test
    fun emitsRunningThenSuccess() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("t") { s: String -> s.uppercase() }
        val collected = collect(scheduler.observeStateChanges(task))

        val job = scheduler.launch(task, "a")
        advanceUntilIdle()

        assertEquals(listOf(RunState.Running, RunState.FinishedSuccess), collected.states.map { it.runState })
        assertTrue(collected.states.all { it.jobId == job.id })
        assertEquals("A", collected.states.last().result)
        assertNull(collected.error)
    }

    @Test
    fun cancellingCollectorRemovesListener() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("t") { _: Int -> }
        val collected = collect(scheduler.observeStateChanges(task))
        // added after the flow's listener: if the dead listener were still there, its send would throw
        // and abort the job before this one is notified
        val plain = RecordingListener().also(scheduler::addTaskStateChangeListener)

        collected.job.cancel()
        advanceUntilIdle()
        scheduler.launch(task, 1)
        advanceUntilIdle()

        assertTrue(collected.states.isEmpty())
        assertEquals(listOf(RunState.Running, RunState.FinishedSuccess), plain.states.map { it.runState })
        assertTrue(plain.cancelled.isEmpty())
    }

    @Test
    fun jobCancellationClosesFlow() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("slow") { _: Int -> delay(1000) }
        val collected = collect(scheduler.observeStateChanges(task))

        val job = scheduler.launch(task, 1)
        runCurrent()
        scheduler.cancel(job)
        advanceUntilIdle()

        assertEquals(listOf(RunState.Running), collected.states.map { it.runState })
        val error = assertIs<CancellationException>(collected.error)
        assertTrue(error.message.orEmpty().contains("jobId=${job.id} was cancelled"), error.message)
        assertTrue(collected.job.isCompleted)
    }

    @Test
    fun ignoresOtherTasks() = runTest {
        val scheduler = testScheduler()
        val observed = scheduler.registerTask("observed") { _: Int -> }
        val other = scheduler.registerTask("other") { _: Int -> delay(1000) }
        val collected = collect(scheduler.observeStateChanges(observed))

        val otherJob = scheduler.launch(other, 1)
        runCurrent()
        scheduler.cancel(otherJob)
        advanceUntilIdle()
        scheduler.launch(observed, 1)
        advanceUntilIdle()

        assertNull(collected.error)
        assertEquals(listOf(observed.id, observed.id), collected.states.map { it.taskId })
    }

    @Test
    fun completeOnJobCancelFalseKeepsObserving() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("slow") { s: String -> delay(1000); s }
        val collected = collect(scheduler.observeStateChanges(task, completeOnJobCancel = false))

        scheduler.launchLatest(task, "a")
        runCurrent()
        scheduler.launchLatest(task, "b")
        advanceUntilIdle()

        assertNull(collected.error)
        assertEquals(listOf("a", "b", "b"), collected.states.map { it.argument })
        assertEquals("b", collected.states.last().result)
    }

    @Test
    fun jobStatesReplaysRunningState() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("slow") { _: Int -> delay(1000) }
        val job = scheduler.launch(task, 1)
        runCurrent()

        val collected = collect(scheduler.jobStates(task))
        scheduler.cancel(job)
        advanceUntilIdle()

        assertEquals(listOf(job.id to RunState.Running), collected.states.map { it.jobId to it.runState })
        assertNull(collected.error) // jobStates never completes on job cancellation
    }

    @Test
    fun jobStatesDoesNotReplayFinishedOrCancelledJobs() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("t") { n: Int -> delay(1000); n }
        scheduler.launch(task, 1)
        advanceUntilIdle()
        val cancelled = scheduler.launch(task, 2)
        runCurrent()
        scheduler.cancel(cancelled)
        advanceUntilIdle()

        val collected = collect(scheduler.jobStates(task))

        assertTrue(collected.states.isEmpty())
    }

    @Test
    fun jobStatesReplayFinishedReplaysLastFinishedJob() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("t") { n: Int -> delay(1000); if (n < 0) error("negative") else n }
        scheduler.launch(task, 1)
        advanceUntilIdle()
        val cancelled = scheduler.launch(task, 2)
        runCurrent()
        scheduler.cancel(cancelled)
        advanceUntilIdle()

        val states = collect(scheduler.jobStates(task, replayFinished = true)).states
        assertEquals(listOf(RunState.FinishedSuccess), states.map { it.runState })
        assertEquals(1, states.single().resultOf(task))

        val results = mutableListOf<Int>()
        backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) {
            scheduler.successResults(task, replayFinished = true).collect { results += it }
        }
        runCurrent()
        assertEquals(listOf(1), results)

        scheduler.launch(task, -1)
        advanceUntilIdle()
        val errors = mutableListOf<Throwable>()
        backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) {
            scheduler.errors(task, replayFinished = true).collect { errors += it }
        }
        runCurrent()
        assertEquals(listOf("negative"), errors.map { it.message })
    }

    @Test
    fun typedResultsAndErrors() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("t") { n: Int -> if (n < 0) error("negative") else n * 2 }
        val results = mutableListOf<Int>()
        val errors = mutableListOf<Throwable>()
        backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) { scheduler.successResults(task).collect { results += it } }
        backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) { scheduler.errors(task).collect { errors += it } }
        runCurrent()

        scheduler.launch(task, 2)
        scheduler.launch(task, -1)
        advanceUntilIdle()
        val state = scheduler.start(task, 5)
        val doubled: Int? = state.resultOf(task)
        val other = scheduler.registerTask("other") { _: Unit -> }
        assertFailsWith<IllegalArgumentException> { state.resultOf(other) }

        assertEquals(listOf(4, 10), results)
        assertEquals(listOf("negative"), errors.map { it.message })
        assertEquals(10, doubled)
    }

    @Test
    fun defaultUncaughtExceptionMessageHasNameThenId() {
        val id = Uuid.random()
        assertEquals("warning: uncaught exception from task \"fetch[$id]\"", uncaughtExceptionMessage(id, "fetch"))
    }

    @Test
    fun isRunningTracksStartedJobs() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("slow") { _: Int -> delay(1000) }
        assertFalse(scheduler.isRunning(task))

        scheduler.launch(task, 1)
        runCurrent()
        assertTrue(scheduler.isRunning(task))

        advanceUntilIdle()
        assertFalse(scheduler.isRunning(task))
    }

    @Test
    fun successResultsReplayDuringReRunReturnsPreviousResult() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("t") { n: Int -> delay(1000); n }
        scheduler.launch(task, 1)
        advanceUntilIdle()
        scheduler.launch(task, 2)
        runCurrent()

        val results = values(scheduler.successResults(task, replayFinished = true))
        assertEquals(listOf(1), results)

        advanceUntilIdle()
        assertEquals(listOf(1, 2), results)
    }

    @Test
    fun replayAfterErrorReplaysErrorOnly() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("t") { n: Int -> if (n < 0) error("negative") else n }
        scheduler.launch(task, 1)
        scheduler.launch(task, -1)
        advanceUntilIdle()

        assertEquals(emptyList(), values(scheduler.successResults(task, replayFinished = true)))
        assertEquals(listOf("negative"), values(scheduler.errors(task, replayFinished = true)).map { it.message })
    }

    @Test
    fun finishedStatesReplaysLastFinishedDuringRunningJob() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("t") { n: Int -> delay(1000); n }
        scheduler.launch(task, 1)
        advanceUntilIdle()
        scheduler.launch(task, 2)
        runCurrent()

        val collected = collect(scheduler.finishedStates(task, replayLast = true))
        assertEquals(listOf(RunState.FinishedSuccess), collected.states.map { it.runState })
        assertEquals(1, collected.states.single().resultOf(task))

        val cancelled = scheduler.launch(task, 3)
        runCurrent()
        scheduler.cancel(cancelled)
        advanceUntilIdle()
        assertEquals(listOf(1, 2), collected.states.map { it.resultOf(task) })
        assertTrue(collected.job.isActive)
    }

    @Test
    fun noReplayWhenReplayIsOff() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("t") { n: Int -> if (n < 0) error("negative") else n }
        scheduler.launch(task, 1)
        advanceUntilIdle()

        assertTrue(collect(scheduler.finishedStates(task)).states.isEmpty())
        assertEquals(emptyList(), values(scheduler.successResults(task)))
        scheduler.launch(task, -1)
        advanceUntilIdle()
        assertEquals(emptyList(), values(scheduler.errors(task)))
    }

    @Test
    fun runningStatesGoesFalseAfterCancel() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("slow") { _: Int -> delay(1000) }
        val runningOnCancel = mutableListOf<Boolean>()
        scheduler.addTaskStateChangeListener(object : TaskStateChangeListener {
            override suspend fun onJobStateChanged(state: JobState) = Unit
            override fun onJobCancelled(taskId: Uuid, jobId: Uuid) {
                runningOnCancel += scheduler.isRunning(task)
            }
        })
        val running = values(scheduler.runningStates(task))
        assertEquals(listOf(false), running)

        scheduler.launch(task, 1)
        runCurrent()
        assertEquals(listOf(false, true), running)

        scheduler.cancel(task)
        runCurrent()
        // the cancelled job is no longer counted when onJobCancelled fires
        assertEquals(listOf(false), runningOnCancel)
        assertEquals(listOf(false, true, false), running)
    }

    @Test
    fun runningStatesStaysTrueWhenStartLatestReplacesJob() = runTest {
        val scheduler = testScheduler()
        val listener = RecordingListener().also(scheduler::addTaskStateChangeListener)
        val task = scheduler.registerTask("slow") { _: Int -> delay(1000) }
        val running = values(scheduler.runningStates(task))

        val first = scheduler.launch(task, 1)
        runCurrent()
        scheduler.launchLatest(task, 2)
        runCurrent()
        assertEquals(listOf(first.id), listener.cancelled.map { it.jobId })
        assertEquals(listOf(false, true), running)

        advanceUntilIdle()
        assertEquals(listOf(false, true, false), running)
    }

    @Test
    fun runningStatesGoesFalseWhenSchedulerScopeIsCancelled() = runTest {
        val parent = Job()
        val scheduler = Scheduler(CoroutineScope(parent), StandardTestDispatcher(testScheduler), UncaughtRecorder().handler)
        val task = scheduler.registerTask("slow") { _: Int -> delay(1000) }
        val running = values(scheduler.runningStates(task))

        scheduler.launch(task, 1)
        runCurrent()
        parent.cancel()
        advanceUntilIdle()

        assertEquals(listOf(false, true, false), running)
    }
}
