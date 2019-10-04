package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.edm.processors.GetEntityTypeFromEntitySetEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class GetEntityTypeFromEntitySetEntryProcessorStreamSerializer: NoOpSelfRegisteringStreamSerializer<GetEntityTypeFromEntitySetEntryProcessor>() {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.GET_ET_FROM_ES_EP.ordinal
    }

    override fun getClazz(): Class<out GetEntityTypeFromEntitySetEntryProcessor> {
        return GetEntityTypeFromEntitySetEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput?): GetEntityTypeFromEntitySetEntryProcessor {
        return GetEntityTypeFromEntitySetEntryProcessor()
    }
}
