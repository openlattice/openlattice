package com.openlattice.hazelcast

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import org.junit.Assert
import org.junit.Test
import org.mockito.Mockito

class HazelcastMapTest {
    @Test
    fun testGetMap() {
        val uniqueId = "ea85d075-e627-4608-add7-d3ee43705761"
        val instance = Mockito.mock(HazelcastInstance::class.java)
        val map = Mockito.mock<IMap<*, *>>(IMap::class.java) as IMap<Any, Any>
        Mockito.`when`(instance.getMap<Any, Any>(uniqueId)).thenReturn(map)

        val first = HazelcastMap<Any, Any>(uniqueId)
        val value = first.getMap(instance)
        Assert.assertEquals(map, value)
    }

    @Test
    fun testConstructorNoThrow() {
        val uniqueId = "2d3347e3-d039-419a-9632-fb979b82cdfb"

        val first = HazelcastMap<Any, Any>(uniqueId)
        Assert.assertNotNull(first)
        val second = HazelcastMap<Any, Any>(uniqueId)
        Assert.assertNotNull(second)
    }


    @Test(expected = IllegalStateException::class)
    fun testDuplicateRegistration() {
        val uniqueId = "46587ede-b14a-46de-b8a2-1e00b6fe2ae3"

        val instance = Mockito.mock(HazelcastInstance::class.java)
        val map = Mockito.mock<IMap<*, *>>(IMap::class.java) as IMap<Any, Any>
        Mockito.`when`(instance.getMap<Any, Any>(uniqueId)).thenReturn(map)

        val first = HazelcastMap<Any, Any>(uniqueId)
        Assert.assertNotNull(first)
        val second = HazelcastMap<Any, Any>(uniqueId)
        second.getMap(instance)
    }

    @Test
    fun testValues() {
        val uniqueId = "abaf6918-04b1-4cef-b1e5-16a5f660d63b"

        val mapHandle = HazelcastMap<Any, Any>(uniqueId)
        Assert.assertEquals(1, HazelcastMap.values().filter { it.name == uniqueId }.size)
        Assert.assertEquals(mapHandle, HazelcastMap.valueOf(uniqueId))
    }
}
