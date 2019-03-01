package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.processors.CreateProductionForeignTableOfEntitySetProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds

class CreateProductionForeignTableOfEntitySetProcessorStreamSerializer
    : SelfRegisteringStreamSerializer<CreateProductionForeignTableOfEntitySetProcessor>,
        AssemblerConnectionManagerDependent {
    private lateinit var acm: AssemblerConnectionManager

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.CREATE_PRODUCTION_FOREIGN_TABLE_OF_ENTITY_SET_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out CreateProductionForeignTableOfEntitySetProcessor> {
        return CreateProductionForeignTableOfEntitySetProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, obj: CreateProductionForeignTableOfEntitySetProcessor) {
        UUIDStreamSerializer.serialize(out, obj.entitySetId)
    }

    override fun read(input: ObjectDataInput): CreateProductionForeignTableOfEntitySetProcessor {
        return CreateProductionForeignTableOfEntitySetProcessor(UUIDStreamSerializer.deserialize(input))
    }

    override fun init(assemblerConnectonManager: AssemblerConnectionManager) {
        this.acm = assemblerConnectonManager
    }
}