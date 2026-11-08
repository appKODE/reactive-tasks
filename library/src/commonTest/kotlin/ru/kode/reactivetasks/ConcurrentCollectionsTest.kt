package ru.kode.reactivetasks

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ConcurrentCollectionsTest {

    @Test
    fun mapBasicOperations() {
        val map = createConcurrentMutableMap<String, Int>()

        assertNull(map.put("a", 1))
        assertEquals(1, map["a"])
        assertEquals(1, map.put("a", 2))
        assertEquals(2, map["a"])
        map["b"] = 3
        assertEquals(2, map.size)
        assertTrue(map.containsKey("b"))
        assertEquals(3, map.remove("b"))
        assertFalse(map.containsKey("b"))
        assertNull(map["b"])
        assertEquals(1, map.size)
    }

    @Test
    fun mapEntriesIterationToleratesPuts() {
        val map = createConcurrentMutableMap<Int, Int>()
        repeat(10) { map[it] = it }

        for ((key, _) in map.entries) {
            map[key + 100] = key
        }

        assertTrue(map.size >= 10)
    }

    @Test
    fun listBasicOperations() {
        val list = createConcurrentMutableList<String>()

        list.add("a")
        list.add("b")
        list.add("c")
        assertEquals(3, list.size)
        assertEquals("b", list[1])
        assertEquals("b", list.set(1, "B"))
        assertEquals(listOf("a", "B", "c"), list.toList())
        assertTrue(list.remove("a"))
        assertFalse(list.remove("missing"))
        assertEquals("c", list.removeAt(1))
        assertEquals(listOf("B"), list.toList())
    }

    @Test
    fun listIterationSeesSnapshot() {
        val list = createConcurrentMutableList<Int>()
        list.addAll(listOf(1, 2, 3))
        val seen = mutableListOf<Int>()

        list.forEach {
            seen += it
            list.add(it * 10)
            list.remove(it)
        }

        assertEquals(listOf(1, 2, 3), seen)
        assertEquals(listOf(10, 20, 30), list.toList())
    }

    @Test
    fun concurrentWritesAreNotLost() = runTest {
        val map = createConcurrentMutableMap<Int, Int>()
        val list = createConcurrentMutableList<Int>()

        withContext(Dispatchers.Default) {
            coroutineScope {
                repeat(8) { worker ->
                    launch {
                        repeat(1000) { i ->
                            map[worker * 1000 + i] = i
                            list.add(i)
                        }
                    }
                }
            }
        }

        assertEquals(8000, map.size)
        assertEquals(8000, list.size)
    }
}
