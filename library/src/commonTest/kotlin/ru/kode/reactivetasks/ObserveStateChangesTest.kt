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
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.uuid.ExperimentalUuidApi

class ObserveStateChangesTest {

    private class Collected(val job: Job) {
        val states = mutableListOf<JobState>()
        var error: Throwable? = null
    }

    private fun TestScope.collect(flow: Flow<JobState>): Collected {
        lateinit var collected: Collected
        // unconfined: advanceUntilIdle() does not run backgroundScope work, so the collector must resume eagerly
        val job = backgroundScope.launch(UnconfinedTestDispatcher(testScheduler)) {
            try {
                flow.collect { collected.states += it }
            } catch (e: CancellationException) {
                collected.error = e
                throw e
            }
        }
        collected = Collected(job)
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
    fun emitsStatesOfOtherTasks() = runTest {
        val scheduler = testScheduler()
        val observed = scheduler.registerTask("observed") { _: Int -> }
        val other = scheduler.registerTask("other") { _: Int -> delay(1000) }
        val collected = collect(scheduler.observeStateChanges(observed))

        // quirk: no filtering by handle, other tasks' states leak into the flow...
        val otherJob = scheduler.start(other, 1)
        runCurrent()
        assertEquals(listOf(other.id), collected.states.map { it.taskId })

        // ...and cancelling another task closes it
        scheduler.cancel(otherJob)
        advanceUntilIdle()
        assertIs<CancellationException>(collected.error)
    }
}
