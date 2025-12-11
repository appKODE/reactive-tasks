package ru.kode.reactivetasks

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
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
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
 * A state of a job
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
     * If this scope gets cancelled, all tasks/jobs will be cancelled too.
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
    val onUncaughtException: (taskId: Uuid, taskName: String, error: Throwable) -> Unit = { name, id, e ->
        println("warning: uncaught exception from task \"$name[$id]\"")
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

    private val stateChangeListeners: MutableList<TaskStateChangeListener> = createConcurrentMutableList()

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
        job.invokeOnCompletion { cause ->
            if (cause is CancellationException) {
                stateChangeListeners.forEach {
                    it.onJobCancelled(handle.id, jobId)
                }
            }
        }
        return JobHandle(
            handle as TaskHandle<Any?, Any?>,
            jobId,
            job,
        )
    }

    private suspend fun <A, R> executeWrappedBody(handle: TaskHandle<A, R>, jobId: Uuid, argument: A, tag: Any?): JobState {
        val state = JobState(jobId = jobId, taskId = handle.id, argument = argument, tag = tag, runState = RunState.Running)
        _taskJobState[jobId] = state
        stateChangeListeners.forEach { it.onJobStateChanged(state) }
        try {
            val result = handle.body(argument)
            val state = JobState(jobId = jobId, taskId = handle.id, argument = argument, tag = tag, runState = RunState.FinishedSuccess, result = result)
            _taskJobState[jobId] = state
            stateChangeListeners.forEach { it.onJobStateChanged(state) }
            return state
        } catch (e: Throwable) {
            if (e is CancellationException) {
                throw e
            } else {
                val state = JobState(jobId = jobId, taskId = handle.id, argument = argument, tag = tag, runState = RunState.FinishedError, error = e)
                _taskJobState[jobId] = state
                stateChangeListeners.forEach { it.onJobStateChanged(state) }
                return state
            }
        }
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
}

/**
 * A simple converter from callback-based observation (using [TaskStateChangeListener]) to reactive-based
 * observation using [Flow].
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
 */
@OptIn(ExperimentalUuidApi::class)
fun Scheduler.observeStateChanges(handle: TaskHandle<*, *>): Flow<JobState> {
    return callbackFlow {
        val listener = object : TaskStateChangeListener {
            override suspend fun onJobStateChanged(state: JobState) {
                send(state)
            }

            override fun onJobCancelled(taskId: Uuid, jobId: Uuid) {
                cancel(CancellationException("Task taskId=$taskId, jobId=$jobId was cancelled"))
            }
        }
        this@observeStateChanges.addTaskStateChangeListener(listener)
        awaitClose { this@observeStateChanges.removeTaskStateChangeListener(listener) }
    }
}
