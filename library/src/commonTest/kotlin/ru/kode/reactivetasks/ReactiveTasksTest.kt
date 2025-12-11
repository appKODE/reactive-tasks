@file:OptIn(ExperimentalUuidApi::class)
package ru.kode.reactivetasks

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.uuid.ExperimentalUuidApi

class ReactiveTasksTest {

    @Test
    fun shouldCreateTaskHandle() {
        val scope = CoroutineScope(Dispatchers.Default)
        val scheduler = Scheduler(scope = scope)

        val taskHandle = scheduler.registerTask("fetch") { userId: String ->
            delay(1000)
        }

        assertNotNull(taskHandle)
    }

}
