package com.openlattice.linking

import com.openlattice.data.EntityDataKey
import org.junit.Assert
import org.junit.Test
import java.util.UUID

class EntityKeyPairTest {

    @Test
    fun testOrder() {
        val entityDataKey1 = EntityDataKey(UUID.randomUUID(), UUID.randomUUID())
        val entityDataKey2 = EntityDataKey(UUID.randomUUID(), UUID.randomUUID())

        val pair1 = EntityKeyPair(entityDataKey1, entityDataKey2)
        val pair2 = EntityKeyPair(entityDataKey2, entityDataKey1)

        Assert.assertEquals(pair1.first, pair2.first)
        Assert.assertEquals(pair1.second, pair2.second)
        Assert.assertEquals(pair1, pair2)
    }

    @Test
    fun testSameElement() {
        val entityDataKey = EntityDataKey(UUID.randomUUID(), UUID.randomUUID())

        val pair = EntityKeyPair(entityDataKey, entityDataKey)

        Assert.assertEquals(entityDataKey, pair.first)
        Assert.assertEquals(entityDataKey, pair.second)
    }
}