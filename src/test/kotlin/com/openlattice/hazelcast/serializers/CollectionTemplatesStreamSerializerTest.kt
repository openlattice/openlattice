package com.openlattice.hazelcast.serializers

import com.google.common.collect.Maps
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.collections.CollectionTemplates
import org.apache.commons.lang3.RandomUtils
import java.util.*
import java.util.concurrent.ConcurrentMap

class CollectionTemplatesStreamSerializerTest : AbstractStreamSerializerTest<CollectionTemplatesStreamSerializer, CollectionTemplates>() {

    override fun createSerializer(): CollectionTemplatesStreamSerializer {
        return CollectionTemplatesStreamSerializer()
    }

    override fun createInput(): CollectionTemplates {
        val templates = Maps.newConcurrentMap<UUID, ConcurrentMap<UUID, UUID>>()

        for (i in 0..4) {

            val size = RandomUtils.nextInt(1, 5)
            val map = Maps.newConcurrentMap<UUID, UUID>()

            for (j in 0 until size) {
                map[UUID.randomUUID()] = UUID.randomUUID()
            }

            templates[UUID.randomUUID()] = map
        }

        return CollectionTemplates(templates)
    }
}