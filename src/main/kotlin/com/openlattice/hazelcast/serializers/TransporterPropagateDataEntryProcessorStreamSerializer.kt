package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.AssemblerConnectionManagerDependent
import com.openlattice.edm.EntitySet
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.transporter.processors.TransporterPropagateDataEntryProcessor
import org.springframework.stereotype.Component

@Component
class TransporterPropagateDataEntryProcessorStreamSerializer : SelfRegisteringStreamSerializer<TransporterPropagateDataEntryProcessor>,
        AssemblerConnectionManagerDependent<Void?> {
    @Transient
    private lateinit var acm: AssemblerConnectionManager

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.TRANSPORTER_PROPAGATE_DATA_ENTRY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out TransporterPropagateDataEntryProcessor> {
        return TransporterPropagateDataEntryProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: TransporterPropagateDataEntryProcessor) {
        SetStreamSerializers.serialize(out, `object`.entitySets) { es -> out.writeObject(es) }
        val partitions = `object`.partitions.toIntArray()
        out.writeIntArray(partitions)
    }

    override fun read(`in`: ObjectDataInput): TransporterPropagateDataEntryProcessor {
        val entitySets = SetStreamSerializers.deserialize(`in`) { `in`.readObject<EntitySet>() }
        val partitions = `in`.readIntArray().toList()
        return TransporterPropagateDataEntryProcessor(entitySets, partitions).init(acm)
    }

    override fun init(acm: AssemblerConnectionManager): Void? {
        this.acm = acm
        return null
    }
}