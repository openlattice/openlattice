package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.edm.processors.GetPropertiesFromEntityTypeEntryProcessor
import org.springframework.stereotype.Component

@Component
class GetPropertiesFromEntityTypeEntryProcessorStreamSerializer: NoOpSelfRegisteringStreamSerializer<GetPropertiesFromEntityTypeEntryProcessor>() {
    override fun getClazz(): Class<out GetPropertiesFromEntityTypeEntryProcessor> {
        return GetPropertiesFromEntityTypeEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput?): GetPropertiesFromEntityTypeEntryProcessor {
        return GetPropertiesFromEntityTypeEntryProcessor()
    }
}