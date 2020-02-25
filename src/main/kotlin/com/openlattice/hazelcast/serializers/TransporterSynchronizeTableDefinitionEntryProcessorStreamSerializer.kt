package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.IndexType
import com.openlattice.transporter.processors.TransporterSynchronizeTableDefinitionEntryProcessor
import org.springframework.stereotype.Component

@Component
class TransporterSynchronizeTableDefinitionEntryProcessorStreamSerializer:
        TestableSelfRegisteringStreamSerializer<TransporterSynchronizeTableDefinitionEntryProcessor>,
        AssemblerConnectionManagerDependent<Void?>
{
    @Transient
    private lateinit var acm: AssemblerConnectionManager

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.TRANSPORTER_SYNCHRONIZE_TABLE_DEFINITION_ENTRY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out TransporterSynchronizeTableDefinitionEntryProcessor> {
        return TransporterSynchronizeTableDefinitionEntryProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: TransporterSynchronizeTableDefinitionEntryProcessor) {
        `out`.writeInt(`object`.newProperties.size)
        `object`.newProperties.forEach {
            PropertyTypeStreamSerializer.serialize(`out`, it)
        }
    }

    override fun read(`in`: ObjectDataInput): TransporterSynchronizeTableDefinitionEntryProcessor {
        val size = `in`.readInt()

        val newProperties = (1..size).map { _ ->
            PropertyTypeStreamSerializer.deserialize(`in`)
        }
        return TransporterSynchronizeTableDefinitionEntryProcessor(newProperties).init(acm)
    }

    override fun generateTestValue(): TransporterSynchronizeTableDefinitionEntryProcessor {
        return TransporterSynchronizeTableDefinitionEntryProcessor(listOf(
                TestDataFactory.propertyType(IndexType.BTREE, false),
                TestDataFactory.propertyType(IndexType.NONE, true)
        ))
    }

    override fun init(acm: AssemblerConnectionManager): Void? {
        this.acm = acm
        return null
    }
}