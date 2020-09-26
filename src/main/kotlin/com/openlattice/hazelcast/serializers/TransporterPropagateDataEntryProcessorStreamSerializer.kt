package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.transporter.processors.TransporterPropagateDataEntryProcessor
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import org.springframework.stereotype.Component

@Component
class TransporterPropagateDataEntryProcessorStreamSerializer :
        SelfRegisteringStreamSerializer<TransporterPropagateDataEntryProcessor>,
        TransporterDependent<Void?>
{
    @Transient
    private lateinit var data: TransporterDatastore

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.TRANSPORTER_PROPAGATE_DATA_ENTRY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out TransporterPropagateDataEntryProcessor> {
        return TransporterPropagateDataEntryProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: TransporterPropagateDataEntryProcessor) {
        SetStreamSerializers.serialize(out, `object`.entitySets) { es ->
            EntitySetStreamSerializer.serialize(out, es)
        }
        val partitions = `object`.entitySetPartitions.toIntArray()
        out.writeIntArray(partitions)
    }

    override fun read(`in`: ObjectDataInput): TransporterPropagateDataEntryProcessor {
        val entitySets = SetStreamSerializers.deserialize(`in`) {
            EntitySetStreamSerializer.deserialize(`in`)
        }
        val partitions = `in`.readIntArray().toList()
        check(::data.isInitialized) { TransporterDependent.NOT_INITIALIZED }
        return TransporterPropagateDataEntryProcessor(entitySets, partitions).init(data)
    }

    override fun init(data: TransporterDatastore): Void? {
        this.data = data
        return null
    }
}