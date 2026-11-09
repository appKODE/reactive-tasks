@file:OptIn(ExperimentalUuidApi::class, ExperimentalCoroutinesApi::class)

package ru.kode.reactivetasks

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch
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

    @Test
    fun emitsRunningThenSuccess() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("t") { s: String -> s.uppercase() }
        val collected = collect(scheduler.observeStateChanges(task))

        val job = scheduler.start(task, "a")
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
        scheduler.start(task, 1)
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

        val job = scheduler.start(task, 1)
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

        val otherJob = scheduler.start(other, 1)
        runCurrent()
        scheduler.cancel(otherJob)
        advanceUntilIdle()
        scheduler.start(observed, 1)
        advanceUntilIdle()

        assertNull(collected.error)
        assertEquals(listOf(observed.id, observed.id), collected.states.map { it.taskId })
    }

    @Test
    fun completeOnJobCancelFalseKeepsObserving() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("slow") { s: String -> delay(1000); s }
        val collected = collect(scheduler.observeStateChanges(task, completeOnJobCancel = false))

        scheduler.startLatest(task, "a")
        runCurrent()
        scheduler.startLatest(task, "b")
        advanceUntilIdle()

        assertNull(collected.error)
        assertEquals(listOf("a", "b", "b"), collected.states.map { it.argument })
        assertEquals("b", collected.states.last().result)
    }

    @Test
    fun jobStatesReplaysRunningState() = runTest {
        val scheduler = testScheduler()
        val task = scheduler.registerTask("slow") { _: Int -> delay(1000) }
        val job = scheduler.start(task, 1)
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
        scheduler.start(task, 1)
        advanceUntilIdle()
        val cancelled = scheduler.start(task, 2)
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
        scheduler.start(task, 1)
        advanceUntilIdle()
        val cancelled = scheduler.start(task, 2)
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

        scheduler.start(task, -1)
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

        scheduler.start(task, 2)
        scheduler.start(task, -1)
        advanceUntilIdle()
        val state = scheduler.startSuspended(task, 5)
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

        scheduler.start(task, 1)
        runCurrent()
        assertTrue(scheduler.isRunning(task))

        advanceUntilIdle()
        assertFalse(scheduler.isRunning(task))
    }
}
