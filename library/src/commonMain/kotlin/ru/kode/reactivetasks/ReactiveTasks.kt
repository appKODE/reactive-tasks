package ru.kode.reactivetasks

import kotlinx.atomicfu.locks.SynchronizedObject
import kotlinx.atomicfu.locks.synchronized
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

expect fun <K, V> createConcurrentMutableMap(): MutableMap<K, V>
expect fun <T> createConcurrentMutableList(): MutableList<T>

// NOTE: not using data class because do not want "copy" and leaking private members
/**
 * A reference handle representing a task registered with the [Scheduler].
 * Can be used to start, cancel, check status of the task.
 */
@OptIn(ExperimentalUuidApi::class)
class TaskHandle<A, R>(
    val name: String,
    val id: Uuid,
    internal val scope: CoroutineScope,
    internal val body: suspend (A) -> R,
) {
    companion object {
        val NIL = TaskHandle<Any?, Any?>("", Uuid.NIL, CoroutineScope(EmptyCoroutineContext)) {}
    }

    override fun toString(): String {
        return if (id == Uuid.NIL) "TaskHandle.NIL" else "TaskHandle[name=$name,id=$id]"
    }
}

/**
 * A reference handle representing a job for the [taskHandle].
 * Gets created when a certain task is started in the [Scheduler].
 */
// NOTE: not using data class because do not want "copy" and leaking internal members
@OptIn(ExperimentalUuidApi::class)
class JobHandle(
    val taskHandle: TaskHandle<Any?, Any?>,
    val id: Uuid,
    internal val job: Job,
) {
    companion object {
        /**
         * Represents a non-existing job handle
         */
        val NIL = JobHandle(TaskHandle.NIL, Uuid.NIL, Job())
    }

    override fun toString(): String {
        return if (id == Uuid.NIL) "JobHandle.NIL" else "JobHandle[id=$id]"
    }
}

/**
 * A state of a running task's job
 */
enum class RunState {
    /**
     * Job is not started
     */
    NotStarted,
    /**
     * Job is currently running
     */
    Running,
    /**
     * Job is succesfully finished.
     * Result can be found in [JobState#result].
     */
    FinishedSuccess,
    /**
     * Job has finished with error.
     * Error can be found in [JobState#error].
     */
    FinishedError
}

/**
 * A state of a job. Use [resultOf] to read [result] typed by the task's result type
 */
@OptIn(ExperimentalUuidApi::class)
data class JobState(
    /**
     * Unique identifier of the job
     */
    val jobId: Uuid,
    /**
     * Unique identifier of the registered task which is executed by this job
     */
    val taskId: Uuid,
    /**
     * Argument with which job was started
     */
    val argument: Any?,
    /**
     * Current run state
     */
    val runState: RunState = RunState.NotStarted,
    /**
     * In case job has finished with error, this field will contain the corresponding
     * throwable, otherwise it will be null
     */
    val error: Throwable? = null,
    /**
     * In case job has successfully finished, this field will contain the produced
     * result, otherwise it will  be null
     */
    val result: Any? = null,
    /**
     * An optional tag assigned when starting a job using [Scheduler#start]
     */
    val tag: Any? = null,
) {
    companion object {
        /**
         * Represents a non-existing job state
         */
        val NIL = JobState(Uuid.NIL, Uuid.NIL, null)
    }

    override fun toString(): String {
        return if (jobId == Uuid.NIL) "JobState.NIL" else "JobState[${runState.name}][jobId=$jobId,taskId=$taskId,argument=$argument, error=$error, result=$result, tag=$tag]"
    }
}

/**
 * A listener which can be used to observe task's job state changes
 */
@OptIn(ExperimentalUuidApi::class)
interface TaskStateChangeListener {
    /**
     * Will be called whenever a job state changes
     */
    suspend fun onJobStateChanged(state: JobState)
    /**
     * Will be called if job is cancelled
     */
    fun onJobCancelled(taskId: Uuid, jobId: Uuid)
}

/**
 * Task scheduler is the main entry point of the task system.
 *
 * Register tasks with [registerTask] which returns a task handle.
 * This handle can be passed to [start] or [startLatest] to start the task's body.
 * An instance of started task's body is called a "job" and is represented by the [JobHandle] which
 * is returned by [start] or [startLatest].
 *
 * Tasks can also be started in a "blocking" manner by calling [startSuspended] which will execute
 * tasks body, suspend and return the result. Note that you still can observe this job's state with a
 * registered [TaskStateChangeListener].
 *
 * Tasks and Jobs can also be restarted with corresponding [restart] functions. They will reuse the arguments
 * passed to [start], see [restart] documentation for more information.
 */
@OptIn(ExperimentalUuidApi::class)
class Scheduler(
    /**
     * Scope to be used as a parent for all tasks/jobs in this scheduler.
     * If this scope gets cancelled, all tasks/jobs will be cancelled too and the scheduler can't be used
     * anymore: later starts and coroutines launched in it do nothing. Use [cancelAll] to stop the work and keep
     * the scheduler usable.
     */
    val scope: CoroutineScope,
    /**
     * Default dispatcher to use
     */
    val defaultDispatcher: CoroutineDispatcher = Dispatchers.Default,
    /**
     * A callback to be called whenever an uncaught exception is thrown.
     * Note that these are usually exceptions which happen outside of task's "body"-function
     * (which is passed to the [registerTask]), because exceptions inside the "body" are caught
     * and stored in the [JobState#error] field
     */
    val onUncaughtException: (taskId: Uuid, taskName: String, error: Throwable) -> Unit = { taskId, taskName, e ->
        println(uncaughtExceptionMessage(taskId, taskName))
        e.printStackTrace()
    }
) {
    // TODO use configurable entry count?
    /**
     * A map from either JobHandle.id or TaskHandle.id to an argument object.
     */
    private val argumentCache: MutableMap<Uuid, Any?> = createConcurrentMutableMap()

    // TODO evict states after some time/entry-count?
    private val _taskJobState: MutableMap<Uuid, JobState> = createConcurrentMutableMap()

    /**
     * A map of jobId to task job states
     */
    val taskJobState: Map<Uuid, JobState> = _taskJobState

    /**
     * A map of taskId to the most recent state of a still running job of that task
     */
    internal val runningTaskState: MutableMap<Uuid, JobState> = createConcurrentMutableMap()

    /**
     * A map of taskId to the state of the most recently finished (success or error) job of that task
     */
    internal val finishedTaskState: MutableMap<Uuid, JobState> = createConcurrentMutableMap()

    private val startIfIdleLock = SynchronizedObject()

    private val stateChangeListeners: MutableList<TaskStateChangeListener> = createConcurrentMutableList()

    /**
     * Jobs of the task scopes created by [registerTask], they are kept active by [cancelAll]
     */
    private val taskScopeJobs: MutableList<Job> = createConcurrentMutableList()

    /**
     * Called with a taskId when a job of that task is launched or completes (normally or by cancellation)
     */
    internal val runningChangeListeners: MutableList<(taskId: Uuid) -> Unit> = createConcurrentMutableList()

    private fun createExceptionHandler(taskId: Uuid, taskName: String): CoroutineExceptionHandler {
        return CoroutineExceptionHandler { _, e ->
            onUncaughtException(taskId, taskName, e)
        }
    }

    fun addTaskStateChangeListener(listener: TaskStateChangeListener) {
        stateChangeListeners.add(listener)
    }

    fun removeTaskStateChangeListener(listener: TaskStateChangeListener) {
        stateChangeListeners.remove(listener)
    }

    /**
     * Registers a task in this scheduler. Returns a [TaskHandle] which can be used to start
     * this task with [start], [startLatest], [startSuspended].
     *
     * @param name a descriptive task name. Usually used for debug purposes.
     * @param dispatcher a dispatcher to use when running this task
     * @param body a function representing this task's work
     */
    fun <A, R> registerTask(
        name: String,
        dispatcher: CoroutineDispatcher = defaultDispatcher,
        body: suspend (A) -> R,
    ): TaskHandle<A, R> {
        val taskId = Uuid.random()
        val scope = CoroutineScope(SupervisorJob(scope.coroutineContext.job) + createExceptionHandler(taskId, name) + dispatcher)
        taskScopeJobs.add(scope.coroutineContext.job)
        return TaskHandle(
            name = name,
            id = taskId,
            body = body,
            scope = scope,
        )
    }

    /**
     * Starts a new job for task represented by [handle].
     *
     * The job instance will be created for this task execution which can be observed using [TaskStateChangeListener] added with [addTaskStateChangeListener].
     *
     * @param handle represents a registered task to start
     * @param argument an argument to pass to the task's body function
     * @param tag an arbitrary tag to assign to the resulting task job. It will be stored in [JobState] and can be used identify groups of task jobs when needed
     */
    fun <A> start(handle: TaskHandle<A, *>, argument: A, tag: Any? = null): JobHandle {
        return startInternal(handle, argument, tag, cancelPrevious = false)
    }

    /**
     * Starts a new job for task represented by [handle] while also cancelling any previous jobs started for this task.
     * In other words, this behaves much like `Flow.flatMapLatest` does.
     *
     * The job instance will be created for this task execution which can be observed using [TaskStateChangeListener] added with [addTaskStateChangeListener].
     *
     * @param handle represents a registered task to start
     * @param argument an argument to pass to the task's body function
     * @param tag an arbitrary tag to assign to the resulting task job. It will be stored in [JobState] and can be used identify groups of task jobs when needed
     */
    fun <A> startLatest(handle: TaskHandle<A, *>, argument: A, tag: Any? = null): JobHandle {
        return startInternal(handle, argument, tag, cancelPrevious = true)
    }

    /**
     * Starts a new job for task represented by [handle] immediately and suspends until the task is finished.
     *
     * Task result can be obtained from the returned [JobState] value.
     *
     * The job instance will be created for this task execution which can be observed using [TaskStateChangeListener] added with [addTaskStateChangeListener].
     *
     * Note that [TaskStateChangeListener#onJobCancelled] won't be called in this case and if you
     * need to detect task cancellation, you should wrap this suspending call in try/catch and watch
     * for a [CancellationException] to be thrown.
     *
     * @param handle represents a registered task to start
     * @param argument an argument to pass to the task's body function
     * @param tag an arbitrary tag to assign to the resulting task job. It will be stored in [JobState] and can be used identify groups of task jobs when needed
     */
    suspend fun <A, R> startSuspended(handle: TaskHandle<A, R>, argument: A, tag: Any? = null): JobState {
        val jobId = Uuid.random()
        argumentCache.put(jobId, argument as Any?)
        argumentCache.put(handle.id, argument as Any?)
        return executeWrappedBody(handle, jobId, argument, tag)
    }

    /**
     * Starts a new job for [handle] unless a job started with [start], [startLatest] or [restart] is still running
     * ([isRunning]), in which case the call is dropped. The check and the start are one atomic step with respect to
     * other [startIfIdle] calls, so two concurrent calls start exactly one job. Calls to [start], [startLatest] and
     * [restart] are not serialized with it.
     * Replaces remo's `SkipNew` / `Disallow`.
     *
     * @return the started job, or null if the call was dropped
     */
    fun <A> startIfIdle(handle: TaskHandle<A, *>, argument: A, tag: Any? = null): JobHandle? {
        return synchronized(startIfIdleLock) {
            if (isRunning(handle)) null else startInternal(handle, argument, tag, cancelPrevious = false)
        }
    }

    /**
     * Same as `startIfIdle(handle, Unit)`
     */
    fun startIfIdle(handle: TaskHandle<Unit, *>): JobHandle? = startIfIdle(handle, Unit)

    /**
     * Runs [startSuspended] unless a job started with [start], [startLatest] or [restart] is still running
     * ([isRunning]): then the call is dropped and returns null immediately, it does not wait for the running job.
     * Never throws for errors of the task body, they are returned in the [JobState].
     * Unlike [startIfIdle] this is not atomic: jobs started with [startSuspended] are not counted by [isRunning], so
     * concurrent calls can all run. Replaces remo's `start().join()` with `SkipNew` / `Disallow`.
     *
     * @return the state of the finished job, or null if the call was dropped
     */
    suspend fun <A, R> startSuspendedIfIdle(handle: TaskHandle<A, R>, argument: A, tag: Any? = null): JobState? {
        return if (isRunning(handle)) null else startSuspended(handle, argument, tag)
    }

    /**
     * Same as `startSuspendedIfIdle(handle, Unit)`
     */
    suspend fun startSuspendedIfIdle(handle: TaskHandle<Unit, *>): JobState? = startSuspendedIfIdle(handle, Unit)

    /**
     * Calls [cancelAll] when the job of [scope] completes (normally or by cancellation), or right away if it
     * already has. The scheduler stays usable afterwards. Not single-use: every call registers one more
     * [cancelAll] for the completion of the given scope, there is no unbinding.
     */
    fun bindTo(scope: CoroutineScope) {
        scope.coroutineContext.job.invokeOnCompletion { cancelAll() }
    }

    private fun <A> startInternal(handle: TaskHandle<A, *>, argument: A, tag: Any?, cancelPrevious: Boolean): JobHandle {
        val jobId = Uuid.random()
        argumentCache.put(jobId, argument as Any?)
        argumentCache.put(handle.id, argument as Any?)
        // NOTE @dz there can be a race if children will change between we get them here and
        // before new job gets scheduled
        val previousJobs = handle.scope.coroutineContext.job.children.toList()
        val job = handle.scope.launch {
            if (cancelPrevious) {
                previousJobs.forEach {
                    it.cancelAndJoin()
                }
            }
            executeWrappedBody(handle, jobId, argument, tag)
        }
        runningChangeListeners.forEach { it(handle.id) }
        // the job is already detached from the task scope here, so isRunning does not count it
        job.invokeOnCompletion { cause ->
            if (cause is CancellationException) {
                stateChangeListeners.forEach {
                    it.onJobCancelled(handle.id, jobId)
                }
            }
            runningChangeListeners.forEach { it(handle.id) }
        }
        return JobHandle(
            handle as TaskHandle<Any?, Any?>,
            jobId,
            job,
        )
    }

    private suspend fun <A, R> executeWrappedBody(handle: TaskHandle<A, R>, jobId: Uuid, argument: A, tag: Any?): JobState {
        suspend fun publish(state: JobState): JobState {
            _taskJobState[jobId] = state
            if (state.runState == RunState.Running) {
                runningTaskState[handle.id] = state
            } else {
                finishedTaskState[handle.id] = state
                forgetRunning(handle.id, jobId)
            }
            stateChangeListeners.forEach { it.onJobStateChanged(state) }
            return state
        }
        val running = JobState(jobId = jobId, taskId = handle.id, argument = argument, tag = tag, runState = RunState.Running)
        try {
            publish(running)
            return try {
                publish(running.copy(runState = RunState.FinishedSuccess, result = handle.body(argument)))
            } catch (e: Throwable) {
                if (e is CancellationException) throw e
                publish(running.copy(runState = RunState.FinishedError, error = e))
            }
        } catch (e: CancellationException) {
            forgetRunning(handle.id, jobId)
            throw e
        }
    }

    private fun forgetRunning(taskId: Uuid, jobId: Uuid) {
        if (runningTaskState[taskId]?.jobId == jobId) runningTaskState.remove(taskId)
    }

    /**
     * Returns true if a job started with [start], [startLatest] or [restart] for [handle] is still active.
     * Jobs started with [startSuspended] are not counted.
     */
    fun isRunning(handle: TaskHandle<*, *>): Boolean {
        return handle.scope.coroutineContext.job.children.any()
    }

    /**
     * Restarts the task referenced by [handle].
     * Reuses the argument of the most recent [start] call.
     * Note that there is an overload which accepts [JobHandle] instead and it will use the argument from the particular job start.
     */
    fun <A, R> restart(handle: TaskHandle<A, R>): JobHandle {
        @Suppress("UNCHECKED_CAST")
        val arg = argumentCache[handle.id] as? A?
        return if (arg != null) {
            start(handle, arg)
        } else {
            JobHandle.NIL
        }
    }

    /**
     * Restarts the task referenced by job [handle].
     * Reuses the argument of the specified job [handle].
     * Note that there is an overload which accepts [TaskHandle] instead and it will use the argument from the most recent task start.
     */
    fun restart(handle: JobHandle): JobHandle {
        val arg = argumentCache[handle.id]
        return if (arg != null) {
            start(handle.taskHandle, arg)
        } else {
            JobHandle.NIL
        }
    }

    /**
     * Cancels all jobs currently executing for [taskHandle] (if any).
     * Registered change listeners will get [TaskStateChangeListener#onJobCancelled] notification
     */
    fun cancel(taskHandle: TaskHandle<*, *>) {
        taskHandle.scope.coroutineContext.cancelChildren()
    }

    /**
     * Cancels all jobs currently executing for [taskHandle] (if any).
     */
    fun cancel(jobHandle: JobHandle) {
        jobHandle.job.cancel()
    }

    /**
     * Cancels all jobs currently executing for [taskHandle] (if any) and suspends until
     * all of them are cancelled
     */
    suspend fun cancelAndJoin(taskHandle: TaskHandle<*, *>) {
        taskHandle.scope.coroutineContext.job.children.forEach {
            it.cancelAndJoin()
        }
    }

    /**
     * Cancels all jobs currently executing for [jobHandle] and suspends until
     * it is cancelled
     */
    suspend fun cancelAndJoin(jobHandle: JobHandle) {
        jobHandle.job.cancelAndJoin()
    }

    /**
     * Cancels the jobs of all tasks and all other coroutines launched in [scope], unlike cancelling [scope] the
     * scheduler stays usable: tasks can be started and coroutines launched in [scope] again.
     * Registered change listeners will get [TaskStateChangeListener#onJobCancelled] notifications
     */
    fun cancelAll() {
        scope.coroutineContext.job.children.forEach { if (it in taskScopeJobs) it.cancelChildren() else it.cancel() }
    }
}

@OptIn(ExperimentalUuidApi::class)
internal fun uncaughtExceptionMessage(taskId: Uuid, taskName: String): String {
    return "warning: uncaught exception from task \"$taskName[$taskId]\""
}

/**
 * A simple converter from callback-based observation (using [TaskStateChangeListener]) to reactive-based
 * observation using [Flow]. Emits states of jobs of the task represented by [handle] only.
 *
 * If you wish to do some action (for example starting a task with [Scheduler#start]) only after this flow
 * subscription is established, you can use some combination of "shareIn and/or replay and/or custom implementation
 * of SharingStarted" to fine tune subscription options. After converting the returned [Flow] to a [SharedFlow],
 * you can also use "Flow.onSubscription {}" extension function.
 *
 * See https://kotlinlang.org/api/kotlinx.coroutines/kotlinx-coroutines-core/kotlinx.coroutines.flow/share-in.html
 * for examples and more details.
 *
 * Note that if anything more complex or specific is required, you can always implement the functionality you
 * need in a similar fashion.
 *
 * @param completeOnJobCancel when true (the default), the flow completes with a [CancellationException] as soon as
 * any job of this task is cancelled. Pass false to keep observing, e.g. when the task is started with
 * [Scheduler#startLatest] which cancels previous jobs. Jobs of other tasks never complete the flow.
 */
fun Scheduler.observeStateChanges(handle: TaskHandle<*, *>, completeOnJobCancel: Boolean): Flow<JobState> {
    return stateChanges(handle, completeOnJobCancel, replay = Replay.None)
}

/**
 * Same as `observeStateChanges(handle, completeOnJobCancel = true)`
 */
fun Scheduler.observeStateChanges(handle: TaskHandle<*, *>): Flow<JobState> {
    return observeStateChanges(handle, completeOnJobCancel = true)
}

/**
 * Emits states of jobs of the task represented by [handle], never completes on job cancellation.
 *
 * If the task has a running job at the moment of subscription, its [RunState.Running] state is emitted first,
 * so a late subscriber does not miss it.
 *
 * @param replayFinished when true and no job is running, the state of the most recently finished (success or
 * error) job is replayed, so a subscriber that comes after the job completed still receives its result.
 * Cancelled jobs are never replayed.
 */
fun Scheduler.jobStates(handle: TaskHandle<*, *>, replayFinished: Boolean = false): Flow<JobState> {
    return stateChanges(handle, completeOnJobCancel = false, replay = if (replayFinished) Replay.Last else Replay.Running)
}

private enum class Replay { None, Running, Finished, Last }

/**
 * Emits only [RunState.FinishedSuccess] and [RunState.FinishedError] states of jobs of the task represented by
 * [handle], never completes on job cancellation.
 *
 * @param replayLast when true, the state of the most recently finished job is replayed first, even while another
 * job of the task is running. Cancelled jobs are never replayed.
 */
fun Scheduler.finishedStates(handle: TaskHandle<*, *>, replayLast: Boolean = false): Flow<JobState> {
    return stateChanges(handle, completeOnJobCancel = false, replay = if (replayLast) Replay.Finished else Replay.None)
        .filter { it.runState == RunState.FinishedSuccess || it.runState == RunState.FinishedError }
}

/**
 * Emits [Scheduler.isRunning] for [handle] on subscription and then whenever it changes: a job is started,
 * finishes or is cancelled (including by cancelling the scheduler scope).
 * Jobs started with [Scheduler.startSuspended] are not counted.
 */
@OptIn(ExperimentalUuidApi::class)
fun Scheduler.runningStates(handle: TaskHandle<*, *>): Flow<Boolean> {
    return callbackFlow {
        val listener: (Uuid) -> Unit = { taskId -> if (taskId == handle.id) trySend(Unit) }
        runningChangeListeners.add(listener)
        send(Unit)
        awaitClose { runningChangeListeners.remove(listener) }
    }
        .buffer(Channel.CONFLATED)
        // read on collection, so concurrent changes can not be emitted out of order
        .map { isRunning(handle) }
        .distinctUntilChanged()
}

/**
 * Emits the outcome of each finished job of the task represented by [handle] as a [Result]: success holds the
 * job's result (see [resultOf]), failure holds its error. Cancelled jobs emit nothing. See [finishedStates]
 *
 * @param replayLast when true (the default), the most recently finished job is replayed first, even while another
 * job of the task is running
 */
fun <R> Scheduler.results(handle: TaskHandle<*, R>, replayLast: Boolean = true): Flow<Result<R>> {
    @Suppress("UNCHECKED_CAST")
    return finishedStates(handle, replayLast).map { state ->
        if (state.runState == RunState.FinishedSuccess) Result.success(state.resultOf(handle) as R)
        else Result.failure(checkNotNull(state.error))
    }
}

/**
 * Emits results of successfully finished jobs of the task represented by [handle].
 *
 * @param replayFinished when true, the last finished job is replayed first if it succeeded, even while another
 * job of the task is running. If the last finished job failed, nothing is replayed. See [finishedStates]
 */
fun <R> Scheduler.successResults(handle: TaskHandle<*, R>, replayFinished: Boolean = false): Flow<R> {
    @Suppress("UNCHECKED_CAST")
    return finishedStates(handle, replayFinished).filter { it.runState == RunState.FinishedSuccess }.map { it.resultOf(handle) as R }
}

/**
 * Returns [JobState.result] typed by the result type of [handle], the task this state belongs to.
 * Null if the job has not finished successfully.
 */
@OptIn(ExperimentalUuidApi::class)
fun <R> JobState.resultOf(handle: TaskHandle<*, R>): R? {
    require(taskId == handle.id) { "state of task $taskId does not belong to task ${handle.id}" }
    @Suppress("UNCHECKED_CAST")
    return result as R?
}

/**
 * Emits errors of failed jobs of the task represented by [handle].
 *
 * @param replayFinished when true, the last finished job is replayed first if it failed, even while another
 * job of the task is running. If the last finished job succeeded, nothing is replayed. See [finishedStates]
 */
fun Scheduler.errors(handle: TaskHandle<*, *>, replayFinished: Boolean = false): Flow<Throwable> {
    return finishedStates(handle, replayFinished).mapNotNull { it.error }
}

@OptIn(ExperimentalUuidApi::class)
private fun Scheduler.stateChanges(
    handle: TaskHandle<*, *>,
    completeOnJobCancel: Boolean,
    replay: Replay,
): Flow<JobState> {
    return callbackFlow {
        val listener = object : TaskStateChangeListener {
            override suspend fun onJobStateChanged(state: JobState) {
                if (state.taskId == handle.id) send(state)
            }

            override fun onJobCancelled(taskId: Uuid, jobId: Uuid) {
                if (completeOnJobCancel && taskId == handle.id) {
                    cancel(CancellationException("Task taskId=$taskId, jobId=$jobId was cancelled"))
                }
            }
        }
        this@stateChanges.addTaskStateChangeListener(listener)
        // read after subscribing: a state changing in between may arrive twice, but is never lost
        val replayed = when (replay) {
            Replay.None -> null
            Replay.Running -> runningTaskState[handle.id]
            Replay.Finished -> finishedTaskState[handle.id]
            Replay.Last -> runningTaskState[handle.id] ?: finishedTaskState[handle.id]
        }
        replayed?.let { listener.onJobStateChanged(it) }
        awaitClose { this@stateChanges.removeTaskStateChangeListener(listener) }
    }
}
