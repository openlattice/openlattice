package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.edm.processors.GetEntityTypeFromEntitySetEntryProcessor
import org.springframework.stereotype.Component

@Component
class GetEntityTypeFromEntitySetEntryProcessorStreamSerializer: NoOpSelfRegisteringStreamSerializer<GetEntityTypeFromEntitySetEntryProcessor>() {
    override fun getClazz(): Class<out GetEntityTypeFromEntitySetEntryProcessor> {
        return GetEntityTypeFromEntitySetEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput?): GetEntityTypeFromEntitySetEntryProcessor {
        return GetEntityTypeFromEntitySetEntryProcessor()
    }
}
