package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.assembler.processors.IsAssemblyInitializedEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class IsAssemblyInitializedEntryProcessorStreamSerializer : NoOpSelfRegisteringStreamSerializer<IsAssemblyInitializedEntryProcessor>() {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ASSEMBLY_INITIALIZED_EP.ordinal
    }

    override fun getClazz(): Class<out IsAssemblyInitializedEntryProcessor> {
        return IsAssemblyInitializedEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput?): IsAssemblyInitializedEntryProcessor {
        return IsAssemblyInitializedEntryProcessor()
    }
}