package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.edm.processors.GetNormalEntitySetIdsEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component


@Component
class GetNormalEntitySetIdsEntryProcessorStreamSerializer : TestableSelfRegisteringStreamSerializer<GetNormalEntitySetIdsEntryProcessor> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.GET_NORMAL_ENTITY_SET_IDS_ENTRY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out GetNormalEntitySetIdsEntryProcessor> {
        return GetNormalEntitySetIdsEntryProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: GetNormalEntitySetIdsEntryProcessor) {
    }

    override fun read(`in`: ObjectDataInput): GetNormalEntitySetIdsEntryProcessor {
        return GetNormalEntitySetIdsEntryProcessor()
    }

    override fun generateTestValue(): GetNormalEntitySetIdsEntryProcessor {
        return GetNormalEntitySetIdsEntryProcessor()
    }
}
