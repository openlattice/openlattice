package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.edm.processors.GetPartitionsFromEntitySetEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class GetPartitionsFromEntitySetEntryProcessorStreamSerializer : NoOpSelfRegisteringStreamSerializer<GetPartitionsFromEntitySetEntryProcessor>() {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.GET_PARTITIONS_FROM_ES_EP.ordinal
    }

    override fun getClazz(): Class<out GetPartitionsFromEntitySetEntryProcessor> {
        return GetPartitionsFromEntitySetEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput?): GetPartitionsFromEntitySetEntryProcessor {
        return GetPartitionsFromEntitySetEntryProcessor()
    }

}