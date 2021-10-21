package com.openlattice.hazelcast.serializers

class TransporterPropagateDataEntryProcessorStreamSerializer

/*
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
        check(::data.isInitialized) { TransporterDependent.NOT_INITIALIZED }
        SetStreamSerializers.serialize(out, `object`.entitySets) { es ->
            EntitySetStreamSerializer.serialize(out, es)
        }
        val partitions = `object`.entitySetPartitions.toIntArray()
        out.writeIntArray(partitions)
    }

    override fun read(`in`: ObjectDataInput): TransporterPropagateDataEntryProcessor {
        check(::data.isInitialized) { TransporterDependent.NOT_INITIALIZED }
        val entitySets = SetStreamSerializers.deserialize(`in`) {
            EntitySetStreamSerializer.deserialize(`in`)
        }
        val partitions = `in`.readIntArray()!!.toList()
        return TransporterPropagateDataEntryProcessor(entitySets, partitions).init(data)
    }

    override fun init(data: TransporterDatastore): Void? {
        this.data = data
        return null
    }
}
*/
