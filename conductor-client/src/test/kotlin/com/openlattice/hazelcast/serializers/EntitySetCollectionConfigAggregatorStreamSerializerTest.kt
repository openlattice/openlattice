package com.openlattice.hazelcast.serializers

import com.google.common.collect.Maps
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.collections.CollectionTemplates
import com.openlattice.collections.aggregators.EntitySetCollectionConfigAggregator
import org.apache.commons.lang3.RandomUtils
import java.util.*
import java.util.concurrent.ConcurrentMap

class EntitySetCollectionConfigAggregatorStreamSerializerTest : AbstractStreamSerializerTest<EntitySetCollectionConfigAggregatorStreamSerializer, EntitySetCollectionConfigAggregator>() {

    override fun createSerializer(): EntitySetCollectionConfigAggregatorStreamSerializer {
        return EntitySetCollectionConfigAggregatorStreamSerializer()
    }

    override fun createInput(): EntitySetCollectionConfigAggregator {
        val templates = mutableMapOf<UUID, MutableMap<UUID, UUID>>()

        for (i in 0..4) {

            val size = RandomUtils.nextInt(1, 5)
            val map = mutableMapOf<UUID, UUID>()

            for (j in 0 until size) {
                map[UUID.randomUUID()] = UUID.randomUUID()
            }

            templates[UUID.randomUUID()] = map
        }

        return EntitySetCollectionConfigAggregator(CollectionTemplates(templates))
    }
}