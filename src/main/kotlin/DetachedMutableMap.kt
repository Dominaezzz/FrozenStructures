import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.cinterop.StableRef
import kotlin.native.concurrent.*
import kotlin.native.internal.GC

class DetachedMutableMap<K : Any, V : Any> : MutableMap<K, V> {
    // Underlying map is kept in a detached graph, but keys and values are kept indirectly.
    private val realMap: AtomicRef<DetachedObjectGraph<MutableMap<Wrapper<K>, Wrapper<V>>>?>

//    constructor() : this(HashMap.INITIAL_CAPACITY)
//    constructor(initialCapacity: Int) : this(0)
//    constructor(original: Map<out K, V>) : this(original.size) { putAll(original) }

    init {
        val objectGraph = DetachedObjectGraph(TransferMode.SAFE) {
            mutableMapOf<Wrapper<K>, Wrapper<V>>()
        }
        realMap = atomic(objectGraph)

        freeze() // Might optimize this away later.
    }

    override val size: Int get() = realMap.unsafeAccess { it.size }  // Cache this?

    override fun isEmpty(): Boolean = realMap.unsafeAccess { it.isEmpty() }

    override fun containsKey(key: K): Boolean {
        return realMap.unsafeAccess { map ->
            withWrapper(key) { wKey ->
                map.containsKey(wKey)
            }
        }
    }

    override fun containsValue(value: V): Boolean {
        return realMap.unsafeAccess { map ->
            withWrapper(value) { wValue ->
                map.containsValue(wValue)
            }
        }
    }

    override fun get(key: K): V? {
        return realMap.unsafeAccess { map ->
            withWrapper(key) { wKey ->
                val valueWrapper = map[wKey]
                // Can be returned since it's being pulled out of a StableRef
                valueWrapper?.get()
            }
        }
    }

    override fun remove(key: K): V? {
        return realMap.unsafeAccess { map ->
            withWrapper(key) { wKey ->
                // FIXME: Old key is leaked here.
                val valueWrapper = map.remove(wKey)
                // Can be returned since it's being pulled out of a StableRef
                valueWrapper?.consume()
            }
        }
    }

    override fun put(key: K, value: V): V? {
        key.freeze() // I think the key should only be frozen if it's new to the map.
        value.freeze()
        return realMap.unsafeAccess { map ->
            val wKey = Wrapper(key)
            val wValue = Wrapper(value)

            // FIXME: Old xor new key is leaked here.
            val valueWrapper = map.put(wKey, wValue)
            // Can be returned since it's being pulled out of a StableRef
            valueWrapper?.consume()
        }
    }

    override fun putAll(from: Map<out K, V>) {
        // Optimize for when `from` is an instance of this class.
        realMap.unsafeAccess { map ->
            for (entry in from) {
                val wKey = Wrapper(entry.key)
                val wValue = Wrapper(entry.value)

                // FIXME: Old xor new key is leaked here.
                val valueWrapper = map.put(wKey, wValue)
                valueWrapper?.dispose()
            }
        }
    }

    override val entries: MutableSet<MutableMap.MutableEntry<K, V>> // TODO("This complicates locking.")
        get() {
            fun copyEntries(map: MutableMap<Wrapper<K>, Wrapper<V>>): MutableSet<MutableMap.MutableEntry<K, V>> {
                val entriesSet = LinkedHashSet<MutableMap.MutableEntry<K, V>>(map.size)
                map.entries.mapTo(entriesSet) { Entry(it.key.get(), it.value.get()) }
                return entriesSet
            }

            return realMap.unsafeAccess { map ->
                val entriesSet = copyEntries(map)
                GC.collect() // Collect iterators
                entriesSet
            }
        }
    override val keys: MutableSet<K> // TODO("This complicates locking.")
        get() {
            fun copyKeys(map: MutableMap<Wrapper<K>, Wrapper<V>>): MutableSet<K> {
                val keySet = LinkedHashSet<K>(map.size)
                map.keys.mapTo(keySet) { it.get() }
                return keySet
            }
            return realMap.unsafeAccess { map ->
                val keySet = copyKeys(map)
                GC.collect() // To collect iterator `map.keys`.
                keySet
            }
        }
    override val values: MutableCollection<V> // TODO("This complicates locking.")
        get() {
            fun copyValues(map: MutableMap<Wrapper<K>, Wrapper<V>>): MutableCollection<V> {
                val valuesList = ArrayList<V>(map.size)
                map.values.mapTo(valuesList) { it.get() }
                return valuesList
            }
            return realMap.unsafeAccess { map ->
                val valuesList = copyValues(map)
                GC.collect() // To collect iterator `map.values`.
                valuesList
            }
        }

    override fun clear() {
        fun clear(map: MutableMap<Wrapper<K>, Wrapper<V>>) {
            // Release StableRef to entries.
            for ((key, value) in map.entries) {
                key.dispose()
                value.dispose()
            }
            // Actually clear map.
            map.clear()
        }
        realMap.unsafeAccess { map ->
            clear(map)
            GC.collect() // Collect iterators
        }
    }

    // Could be inline.
    private class Wrapper<T: Any>(obj: T) {
        private val ref: StableRef<T>

        init {
            obj.freeze()
            ref = StableRef.create(obj)
            freeze()
        }

        fun get(): T = ref.get()
        fun consume(): T {
            val value = ref.get()
            ref.dispose()
            return value
        }
        fun dispose() {
            ref.dispose()
        }

        override fun equals(other: Any?): Boolean {
            return other is Wrapper<*> && get() == other.get()
        }

        override fun hashCode(): Int = get().hashCode()
    }
    private inline fun <T, TObj : Any> withWrapper(obj: TObj, block: (Wrapper<TObj>) -> T): T {
        val temp = Wrapper(obj)
        return try {
            block(temp)
        } finally {
            temp.consume()
        }
    }

    class Entry<K, V>(override val key: K, override val value: V) : MutableMap.MutableEntry<K, V> {
        override fun setValue(newValue: V): V {
            TODO("not implemented")
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other == null) return false
            if (this::class != other::class) return false

            other as Entry<*, *>

            if (key != other.key) return false
            if (value != other.value) return false

            return true
        }

        override fun hashCode(): Int {
            var result = key?.hashCode() ?: 0
            result = 31 * result + (value?.hashCode() ?: 0)
            return result
        }
    }
}
