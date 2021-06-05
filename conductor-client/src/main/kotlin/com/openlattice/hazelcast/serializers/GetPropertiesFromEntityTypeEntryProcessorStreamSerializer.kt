package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.edm.processors.GetPropertiesFromEntityTypeEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class GetPropertiesFromEntityTypeEntryProcessorStreamSerializer: NoOpSelfRegisteringStreamSerializer<GetPropertiesFromEntityTypeEntryProcessor>() {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.GET_PROPERTIES_FROM_ET_EP.ordinal
    }

    override fun getClazz(): Class<out GetPropertiesFromEntityTypeEntryProcessor> {
        return GetPropertiesFromEntityTypeEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput?): GetPropertiesFromEntityTypeEntryProcessor {
        return GetPropertiesFromEntityTypeEntryProcessor()
    }
}