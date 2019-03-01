package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.processors.UpdateProductionForeignTableOfEntitySetProcessor
import com.openlattice.mapstores.TestDataFactory
import org.mockito.Mockito
import java.util.UUID

class UpdateProductionForeignTableOfEntitySetProcessorStreamSerializerTest : AbstractStreamSerializerTest
<UpdateProductionForeignTableOfEntitySetProcessorStreamSerializer, UpdateProductionForeignTableOfEntitySetProcessor>() {

    override fun createSerializer(): UpdateProductionForeignTableOfEntitySetProcessorStreamSerializer {
        val processorSerializer = UpdateProductionForeignTableOfEntitySetProcessorStreamSerializer()
        processorSerializer.init(Mockito.mock(AssemblerConnectionManager::class.java))
        return processorSerializer
    }

    override fun createInput(): UpdateProductionForeignTableOfEntitySetProcessor {
        return UpdateProductionForeignTableOfEntitySetProcessor(
                UUID.randomUUID(),
                listOf(TestDataFactory.propertyType(), TestDataFactory.propertyType()))
    }
}