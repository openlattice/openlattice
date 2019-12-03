package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.assembler.processors.EntitySetContainsFlagEntryProcessor
import com.openlattice.mapstores.TestDataFactory

class EntitySetContainsFlagEntryProcessorStreamSerializerTest : AbstractStreamSerializerTest<EntitySetContainsFlagEntryProcessorStreamSerializer, EntitySetContainsFlagEntryProcessor>() {
    override fun createSerializer(): EntitySetContainsFlagEntryProcessorStreamSerializer {
        return EntitySetContainsFlagEntryProcessorStreamSerializer()
    }

    override fun createInput(): EntitySetContainsFlagEntryProcessor {
        return EntitySetContainsFlagEntryProcessor(TestDataFactory.entitySetFlag())
    }
}