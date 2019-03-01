package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.processors.CreateProductionForeignTableOfEntitySetProcessor
import org.mockito.Mockito
import java.util.UUID

class CreateProductionForeignTableOfEntitySetProcessorStreamSerializerTest : AbstractStreamSerializerTest
<CreateProductionForeignTableOfEntitySetProcessorStreamSerializer, CreateProductionForeignTableOfEntitySetProcessor>() {

    override fun createSerializer(): CreateProductionForeignTableOfEntitySetProcessorStreamSerializer {
        val processorSerializer = CreateProductionForeignTableOfEntitySetProcessorStreamSerializer()
        processorSerializer.init( Mockito.mock( AssemblerConnectionManager::class.java ) )
        return processorSerializer
    }

    override fun createInput(): CreateProductionForeignTableOfEntitySetProcessor {
        return CreateProductionForeignTableOfEntitySetProcessor(UUID.randomUUID())
    }
}