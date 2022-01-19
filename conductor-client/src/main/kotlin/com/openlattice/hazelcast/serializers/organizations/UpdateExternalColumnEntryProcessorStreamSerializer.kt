package com.openlattice.hazelcast.serializers.organizations

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.processors.organizations.UpdateExternalColumnEntryProcessor
import com.openlattice.hazelcast.serializers.MetadataUpdateStreamSerializer
import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component

@Component
class UpdateExternalColumnEntryProcessorStreamSerializer
    : TestableSelfRegisteringStreamSerializer<UpdateExternalColumnEntryProcessor> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UPDATE_EXTERNAL_COLUMN_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out UpdateExternalColumnEntryProcessor> {
        return UpdateExternalColumnEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput): UpdateExternalColumnEntryProcessor {
        return UpdateExternalColumnEntryProcessor(MetadataUpdateStreamSerializer.deserialize(`in`))
    }

    override fun write(out: ObjectDataOutput, `object`: UpdateExternalColumnEntryProcessor) {
        MetadataUpdateStreamSerializer.serialize(out, `object`.update)
    }

    override fun generateTestValue(): UpdateExternalColumnEntryProcessor {
        return UpdateExternalColumnEntryProcessor(TestDataFactory.metadataUpdate())
    }
}