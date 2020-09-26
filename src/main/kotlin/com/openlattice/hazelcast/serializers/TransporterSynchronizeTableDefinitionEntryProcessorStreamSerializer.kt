package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.transporter.processors.TransporterSynchronizeTableDefinitionEntryProcessor
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import org.springframework.stereotype.Component

@Component
class TransporterSynchronizeTableDefinitionEntryProcessorStreamSerializer:
        SelfRegisteringStreamSerializer<TransporterSynchronizeTableDefinitionEntryProcessor>,
        TransporterDependent<Void?>
{
    @Transient
    private lateinit var data: TransporterDatastore

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
        return TransporterSynchronizeTableDefinitionEntryProcessor(newProperties).init(data)
    }

    override fun init(data: TransporterDatastore): Void? {
        this.data = data
        return null
    }
}