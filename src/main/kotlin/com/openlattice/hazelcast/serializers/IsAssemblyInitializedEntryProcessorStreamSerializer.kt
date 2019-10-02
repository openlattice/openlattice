package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.edm.processors.IsAssemblyInitializedEntryProcessor
import org.springframework.stereotype.Component

@Component
class IsAssemblyInitializedEntryProcessorStreamSerializer : NoOpSelfRegisteringStreamSerializer<IsAssemblyInitializedEntryProcessor>() {
    override fun getClazz(): Class<out IsAssemblyInitializedEntryProcessor> {
        return IsAssemblyInitializedEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput?): IsAssemblyInitializedEntryProcessor {
        return IsAssemblyInitializedEntryProcessor()
    }
}