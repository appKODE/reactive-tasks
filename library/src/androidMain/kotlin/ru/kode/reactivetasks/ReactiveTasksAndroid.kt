package ru.kode.reactivetasks

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

actual fun <K, V> createConcurrentMutableMap(): MutableMap<K, V> {
    return ConcurrentHashMap()
}

actual fun <T> createConcurrentMutableList(): MutableList<T> {
    return CopyOnWriteArrayList()
}
