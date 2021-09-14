package com.openlattice.hazelcast.serializers

import com.openlattice.edm.processors.GetNormalEntitySetIdsEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component


@Component
class GetNormalEntitySetIdsEntryProcessorStreamSerializer : NoOpSelfRegisteringStreamSerializer<GetNormalEntitySetIdsEntryProcessor>() {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.GET_NORMAL_ENTITY_SET_IDS_ENTRY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out GetNormalEntitySetIdsEntryProcessor> {
        return GetNormalEntitySetIdsEntryProcessor::class.java
    }
}
