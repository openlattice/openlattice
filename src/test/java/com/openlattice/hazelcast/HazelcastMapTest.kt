package com.openlattice.hazelcast

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import org.junit.Assert
import org.junit.Test
import org.mockito.Mockito

class HazelcastMapTest {
    @Test(expected = IllegalStateException::class)
    fun testDuplicateRegistration() {
        val uniqueId = "46587ede-b14a-46de-b8a2-1e00b6fe2ae3"

        val instance = Mockito.mock(HazelcastInstance::class.java)
        val map = Mockito.mock<IMap<*, *>>(IMap::class.java) as IMap<Any, Any>
        Mockito.`when`(instance.getMap<Any, Any>(uniqueId)).thenReturn(map)

        val first = HazelcastMap<Any, Any>(uniqueId)
        val value = first.getMap(instance)
        Assert.assertEquals(map, value)
        val second = HazelcastMap<Any, Any>(uniqueId)
        second.getMap(instance)
    }
}
