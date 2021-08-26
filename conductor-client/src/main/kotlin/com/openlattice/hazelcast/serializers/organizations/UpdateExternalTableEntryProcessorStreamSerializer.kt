package com.openlattice.hazelcast.serializers.organizations

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.processors.organizations.UpdateExternalTableEntryProcessor
import com.openlattice.hazelcast.serializers.MetadataUpdateStreamSerializer
import com.openlattice.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component

@Component
class UpdateExternalTableEntryProcessorStreamSerializer
    : TestableSelfRegisteringStreamSerializer<UpdateExternalTableEntryProcessor> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UPDATE_EXTERNAL_TABLE_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out UpdateExternalTableEntryProcessor> {
        return UpdateExternalTableEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput): UpdateExternalTableEntryProcessor {
        return UpdateExternalTableEntryProcessor(MetadataUpdateStreamSerializer.deserialize(`in`))
    }

    override fun write(out: ObjectDataOutput, `object`: UpdateExternalTableEntryProcessor) {
        MetadataUpdateStreamSerializer.serialize(out, `object`.update)
    }

    override fun generateTestValue(): UpdateExternalTableEntryProcessor {
        return UpdateExternalTableEntryProcessor(TestDataFactory.metadataUpdate())
    }
}