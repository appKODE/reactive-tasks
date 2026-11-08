package ru.kode.reactivetasks

import kotlinx.atomicfu.locks.SynchronizedObject
import kotlinx.atomicfu.locks.synchronized
import kotlin.concurrent.Volatile

actual fun <K, V> createConcurrentMutableMap(): MutableMap<K, V> {
    return LockedMap()
}

actual fun <T> createConcurrentMutableList(): MutableList<T> {
    return CopyOnWriteList()
}

/**
 * HashMap guarded by a single lock. [entries], [keys] and [values] return snapshot copies,
 * so iterating them never observes a concurrent modification.
 */
// ponytail: one lock for the whole map, fine for the handful of tasks a ReactiveTasks instance holds
private class LockedMap<K, V> : MutableMap<K, V> {
    private val lock = SynchronizedObject()
    private val map = HashMap<K, V>()

    override val size: Int get() = synchronized(lock) { map.size }
    override fun isEmpty(): Boolean = synchronized(lock) { map.isEmpty() }
    override fun containsKey(key: K): Boolean = synchronized(lock) { map.containsKey(key) }
    override fun containsValue(value: V): Boolean = synchronized(lock) { map.containsValue(value) }
    override fun get(key: K): V? = synchronized(lock) { map[key] }
    override fun put(key: K, value: V): V? = synchronized(lock) { map.put(key, value) }
    override fun remove(key: K): V? = synchronized(lock) { map.remove(key) }
    override fun putAll(from: Map<out K, V>) = synchronized(lock) { map.putAll(from) }
    override fun clear() = synchronized(lock) { map.clear() }

    override val entries: MutableSet<MutableMap.MutableEntry<K, V>>
        get() = synchronized(lock) { HashMap(map).entries }
    override val keys: MutableSet<K>
        get() = synchronized(lock) { map.keys.toMutableSet() }
    override val values: MutableCollection<V>
        get() = synchronized(lock) { map.values.toMutableList() }

    override fun equals(other: Any?): Boolean = synchronized(lock) { map == other }
    override fun hashCode(): Int = synchronized(lock) { map.hashCode() }
    override fun toString(): String = synchronized(lock) { map.toString() }
}

/** Same semantics as java.util.concurrent.CopyOnWriteArrayList: iterators work on a snapshot. */
private class CopyOnWriteList<T> : AbstractMutableList<T>() {
    private val lock = SynchronizedObject()

    @Volatile
    private var items: List<T> = emptyList()

    override val size: Int get() = items.size
    override fun get(index: Int): T = items[index]
    override fun iterator(): MutableIterator<T> = SnapshotIterator(items.iterator())

    override fun add(index: Int, element: T) = synchronized(lock) {
        items = items.toMutableList().apply { add(index, element) }
    }

    override fun set(index: Int, element: T): T = synchronized(lock) {
        val copy = items.toMutableList()
        copy.set(index, element).also { items = copy }
    }

    override fun removeAt(index: Int): T = synchronized(lock) {
        val copy = items.toMutableList()
        copy.removeAt(index).also { items = copy }
    }

    // base class would remove through iterator(), which is a read-only snapshot here
    override fun remove(element: T): Boolean = synchronized(lock) {
        val copy = items.toMutableList()
        copy.remove(element).also { if (it) items = copy }
    }

    private class SnapshotIterator<T>(private val delegate: Iterator<T>) : MutableIterator<T>, Iterator<T> by delegate {
        override fun remove() = throw UnsupportedOperationException()
    }
}
