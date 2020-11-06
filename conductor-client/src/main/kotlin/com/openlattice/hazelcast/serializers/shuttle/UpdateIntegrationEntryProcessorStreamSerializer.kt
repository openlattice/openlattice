package com.openlattice.hazelcast.serializers.shuttle

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.processors.shuttle.UpdateIntegrationEntryProcessor
import org.springframework.stereotype.Component

@Component
class UpdateIntegrationEntryProcessorStreamSerializer : SelfRegisteringStreamSerializer<UpdateIntegrationEntryProcessor> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UPDATE_INTEGRATION_EP.ordinal
    }

    override fun getClazz(): Class<out UpdateIntegrationEntryProcessor> {
        return UpdateIntegrationEntryProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: UpdateIntegrationEntryProcessor) {
        IntegrationUpdateStreamSerializer.serialize(out, `object`.update)
    }

    override fun read(`in`: ObjectDataInput): UpdateIntegrationEntryProcessor {
        return UpdateIntegrationEntryProcessor(IntegrationUpdateStreamSerializer.deserialize(`in`))
    }
}