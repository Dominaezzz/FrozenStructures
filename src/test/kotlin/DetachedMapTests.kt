import platform.posix.sleep
import kotlin.native.concurrent.*
import kotlin.random.Random
import kotlin.test.*
import kotlin.time.ExperimentalTime
import kotlin.time.MonoClock

// Tests copied from Stately https://github.com/touchlab/Stately/blob/de3752d7ed2fd1fe3a9792428ce5bb4717e2ea7f/stately-collections/src/commonTest/kotlin/co/touchlab/stately/collections/SharedHashMapTest.kt
class DetachedMapTest {
    @Test
    fun put() {
        val m = DetachedMutableMap<String, MapData>()
        m.put("asdf", MapData("qwert"))

        assertEquals(MapData("qwert"), m.get("asdf"))

        assertEquals(MapData("qwert"), m.put("asdf", MapData("rrr")))

        assertEquals(MapData("rrr"), m.get("asdf"))
        assertEquals(1, m.size)
        m.put("qqq", MapData("asdf"))
        assertEquals(2, m.size)
    }

    @Test
    fun remove() {
        val m = DetachedMutableMap<String, MapData>()
        addTen(m)
        assertEquals(10, m.size)
        assertEquals(MapData("Value: 8"), m.remove("Key: 8"))
        assertNull(m.remove("asdf"))
        assertNull(m.remove("Key: 8"))
        assertEquals(9, m.size)
    }

    @Test
    fun putAll() {
        val m = DetachedMutableMap<String, MapData>()
        addTen(m)

        val madd = DetachedMutableMap<String, MapData>()
        madd.put("Key: 5", MapData("Value: 5b"))
        madd.put("Key: 10", MapData("Value: 10"))

        m.putAll(madd)

        assertEquals(11, m.size)
        assertEquals(m.get("Key: 5"), MapData("Value: 5b"))
    }

    @Test
    fun isEmpty() {
        val m = DetachedMutableMap<String, MapData>()
        addTen(m)

        assertFalse(m.isEmpty())
        m.clear()
        assertTrue(m.isEmpty())
        addTen(m)
        m.entries.toList().forEach {
            m.remove(it.key)
        }
        assertTrue(m.isEmpty())
    }

    @Test
    fun containsKey() {
        val m = DetachedMutableMap<String, MapData>()
        addTen(m)

        assertTrue(m.containsKey("Key: 3"))
        assertFalse(m.containsKey("Key: 10"))
    }

    @Test
    fun containsValue() {
        val m = DetachedMutableMap<String, MapData>()
        addTen(m)

        assertTrue(m.containsValue(MapData("Value: 4")))
        assertFalse(m.containsValue(MapData("Value: 10")))
    }

    @Test
    fun keys() {
        val m = makeCount(3)
        assertTrue(checkCollection(m.keys, "Key: 0", "Key: 1", "Key: 2"))
    }

    @Test
    fun values() {
        val m = makeCount(3)
        assertTrue(checkCollection(m.values, MapData("Value: 0"), MapData("Value: 1"), MapData("Value: 2")))
    }

    @Test
    fun entries() {
        val m = makeCount(3)
        assertTrue(
            checkCollection(
                m.entries,
                DetachedMutableMap.Entry("Key: 0", MapData("Value: 0")),
                DetachedMutableMap.Entry("Key: 1", MapData("Value: 1")),
                DetachedMutableMap.Entry("Key: 2", MapData("Value: 2"))
            )
        )
    }

//    @Test
//    fun rehash() {
//        val m = DetachedMutableMap<String, MapData>()
//
//        val random = Random
//        for (i in 0 until 100_000) {
//            val rand = random.nextInt()
//            assertEquals(m.rehash(rand), m.rehash(rand))
//        }
//    }

    @Test
    fun testInitFrozen() {
        assertTrue(DetachedMutableMap<String, MapData>().isFrozen)
    }

//    @Test
//    fun mtAddRemove() {
//        val LOOPS = 10_000
//        val ops = ThreadOperations { DetachedMutableMap<String, MapData>() }
//        val removeOps = ThreadOperations { DetachedMutableMap<String, MapData>() }
//        val m = DetachedMutableMap<String, MapData>()
//        for (i in 0 until LOOPS) {
//            val key = "key $i"
//            val value = "val $i"
//            ops.exe { m.put(key, MapData(value)) }
//            ops.test { assertTrue { m.containsKey(key) } }
//            removeOps.exe { m.remove(key) }
//            removeOps.test { assertFalse { m.containsKey(key) } }
//        }
//
//        ops.run(threads = 8, randomize = true)
//        removeOps.run(threads = 8, randomize = true)
//        assertEquals(0, m.size)
//    }

//    /**
//     * Verify that bad hash generally works. Will be bad performance, but should function.
//     */
//    @Test
//    fun badHash() {
//        val map = DetachedMutableMap<BadHashKey, MapData>()
//        val ops = ThreadOperations { }
//        val removeOps = ThreadOperations { }
//
//        val LOOPS = 2_000
//        for (i in 0 until LOOPS) {
//            val key = "key $i"
//            ops.exe { map.put(BadHashKey(key), MapData("val $i")) }
//            ops.test { assertTrue { map.containsKey(BadHashKey(key)) } }
//            removeOps.exe { map.remove(BadHashKey(key)) }
//            removeOps.test { assertFalse { map.containsKey(BadHashKey(key)) } }
//        }
//
//        ops.run(threads = 8, randomize = true)
//        assertEquals(LOOPS, map.size)
//        removeOps.run(threads = 8, randomize = true)
//        assertEquals(0, map.size)
//    }
//
//    class BadHashKey(private val s: String) {
//        override fun hashCode(): Int = 2
//        override fun equals(other: Any?): Boolean = other != null && other is BadHashKey && other.s == s
//    }

    @ExperimentalTime
    @Test
    fun testBasicThreads() {
        val WORKERS = 10
        val LOOP_INSERT = 200
        val LOOP_REMOVE = 15

        val workers = Array(WORKERS) {
            Worker.start()
        }

        val m = DetachedMutableMap<String, MapData>().freeze()

        val count = AtomicInt(0).freeze()

        val start = MonoClock.markNow()

        workers.forEach {
            val mycount = count.value

            it.execute(
                TransferMode.SAFE,
                {
                    {
                        for (i in 0 until LOOP_INSERT) {
                            val key = "W:${mycount} I:$i"
                            m.put(key, MapData("W2:${mycount} I2:$i"))
                        }
                        for (i in 0 until LOOP_REMOVE) {
                            m.remove("W:${mycount} I:${i * 10}")
                        }
                    }
                }) { it() }

            count.addAndGet(1)
        }

        for (worker in workers) {
            val future = worker.requestTermination()
            future.result
        }

        println("basic threads time: ${start.elapsedNow()}")
        val size = (LOOP_INSERT - LOOP_REMOVE) * WORKERS
        assertEquals(m.size, size)
        assertEquals(m.entries.size, size)
        assertEquals(m.keys.size, size)
        m.clear()
        assertEquals(m.size, 0)
        assertEquals(m.entries.size, 0)
        assertEquals(m.keys.size, 0)
    }

//    @Test
//    fun testResize() {
//        val m = makeCount(10) as DetachedMutableMap<String, MapData>
//        assertEquals(m.currentBucketSize(), 16)
//        addCount(m, 14)
//        assertEquals(m.currentBucketSize(), 32)
//        addCount(m, 28)
//        assertEquals(m.currentBucketSize(), 64)
//        addCount(m, 60)
//        assertEquals(m.currentBucketSize(), 128)
//        addCount(m, 118)
//        assertEquals(m.currentBucketSize(), 256)
//        addCount(m, 230)
//        assertEquals(m.currentBucketSize(), 512)
//
//        for (i in 0 until 230) {
//            assertEquals(m.get("Key: $i"), MapData("Value: $i"))
//        }
//    }

//    @Test
//    fun testNotMutableSets() {
//        val m = DetachedMutableMap<String, MapData>()
//        for (i in 0 until 15) {
//            val thediv = i / 3
//            m.put("Key: $i", MapData("Value: $thediv"))
//        }
//
//        checkImmutableSet(m.keys, 15, 15, "Asdf")
//        checkImmutableSet(m.values, 15, 5, MapData("Qwert"))
//        checkImmutableSet(m.entries, 15, 15, DetachedMutableMap.Entry("Foo", MapData("Bar")))
//    }

    @Test
    fun testStableSets() {
        val m = DetachedMutableMap<String, MapData>()
        for (i in 0 until 15) {
            val thediv = i / 3
            m.put("Key: $i", MapData("Value: $thediv"))
        }

        val keys = m.keys
        val values = m.values
        val entries = m.entries

        assertEquals(15, keys.size)
        assertEquals(15, values.size)
        assertEquals(15, entries.size)

        for (i in 15 until 20) {
            val thediv = i / 3
            m.put("Key: $i", MapData("Value: $thediv"))
        }

        assertEquals(15, keys.size)
        assertEquals(15, values.size)
        assertEquals(15, entries.size)
        assertEquals(20, m.size)
    }

    /*@Test
    fun testBasicTimes(){
        val start = currentTimeMillis()
        val size = 150_000
        val l = Array(size){ i->
                Pair("Key: $i", MapData("Value: $i")).freeze()
        }
        val keys = Array(size){"Key: $it".freeze()}
        println("TIMETRACE Time setup: ${currentTimeMillis() - start}")
        println("TIMETRACE Time shared: "+ runMutableMapTimes(l, DetachedMutableMap(keys.size), keys))
        println("TIMETRACE Time reg: "+ runMutableMapTimes(l, HashMap(keys.size), keys))
    }
    fun runMutableMapTimes(l:Array<Pair<String, MapData>>, m:MutableMap<String, MapData>, keys:Array<String>):Long{
        val start = currentTimeMillis()
        l.forEach {
            m.put(it.first, it.second)
        }
        keys.forEach {
            m.get(it)
        }
        return currentTimeMillis() - start
    }*/

    private fun <T> checkImmutableSet(c: MutableCollection<T>, total: Int, uniques: Int, sample: T) {
        assertEquals(c.size, total)
        val checkSet = HashSet<T>()
        c.forEach { checkSet.add(it) }
        assertEquals(checkSet.size, uniques)
        assertFails { c.add(sample) }
        assertFails { c.addAll(listOf(sample)) }
        assertFails { c.clear() }
        assertFails { c.remove(sample) }
        assertFails { c.removeAll(listOf(sample)) }
        assertFails { c.retainAll(listOf(sample)) }
    }

    private fun <T> checkCollection(c: Collection<T>, vararg checks: T): Boolean {
        for (check in checks) {
            if (!c.contains(check))
                return false
        }

        return c.size == checks.size
    }

    private fun makeCount(count: Int): MutableMap<String, MapData> {
        val m = DetachedMutableMap<String, MapData>()
        addCount(m, count)
        return m
    }

    private fun addTen(m: MutableMap<String, MapData>) = addCount(m, 10)

    private fun addCount(m: MutableMap<String, MapData>, count: Int) {
        for (i in 0 until count) {
            m.put("Key: $i", MapData("Value: $i"))
        }
    }
}

data class MapData(val s: String)
