package com.openlattice.hazelcast.serializers

import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.datasets.SecurableObjectMetadataUpdateEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component

@Component
class SecurableObjectMetadataUpdateEntryProcessorStreamSerializer :
    TestableSelfRegisteringStreamSerializer<SecurableObjectMetadataUpdateEntryProcessor> {

    override fun generateTestValue(): SecurableObjectMetadataUpdateEntryProcessor {
        return SecurableObjectMetadataUpdateEntryProcessor(TestDataFactory.securableObjectMetadataUpdate())
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SECURABLE_OBJECT_METADATA_UPDATE_ENTRY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out SecurableObjectMetadataUpdateEntryProcessor> {
        return SecurableObjectMetadataUpdateEntryProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: SecurableObjectMetadataUpdateEntryProcessor) {
        SecurableObjectMetadataUpdateStreamSerializer.serialize(out, `object`.update)
    }

    override fun read(`in`: ObjectDataInput): SecurableObjectMetadataUpdateEntryProcessor {
        return SecurableObjectMetadataUpdateEntryProcessor(SecurableObjectMetadataUpdateStreamSerializer.deserialize(`in`))
    }
}