package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.processors.UpdateProductionForeignTableOfEntitySetProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds

class UpdateProductionForeignTableOfEntitySetProcessorStreamSerializer
    : SelfRegisteringStreamSerializer<UpdateProductionForeignTableOfEntitySetProcessor>,
        AssemblerConnectionManagerDependent {
    private lateinit var acm: AssemblerConnectionManager

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UPDATE_PRODUCTION_FOREIGN_TABLE_OF_ENTITY_SET_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out UpdateProductionForeignTableOfEntitySetProcessor> {
        return UpdateProductionForeignTableOfEntitySetProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, obj: UpdateProductionForeignTableOfEntitySetProcessor) {
        UUIDStreamSerializer.serialize(out, obj.entitySetId)
        out.writeInt(obj.newPropertyTypes.size)
        obj.newPropertyTypes.forEach {
            PropertyTypeStreamSerializer.serialize(out, it)
        }

    }

    override fun read(input: ObjectDataInput): UpdateProductionForeignTableOfEntitySetProcessor {
        val entitySetId = UUIDStreamSerializer.deserialize(input)
        val newPropertyTypesSize = input.readInt()
        val newPropertyTypes = (0 until newPropertyTypesSize).map {
            PropertyTypeStreamSerializer.deserialize(input)
        }

        return UpdateProductionForeignTableOfEntitySetProcessor(entitySetId, newPropertyTypes)
    }

    override fun init(assemblerConnectonManager: AssemblerConnectionManager) {
        this.acm = assemblerConnectonManager
    }
}