package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.edm.processors.GetOrganizationIdFromEntitySetEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
@Component
class GetOrganizationIdFromEntitySetEntryProcessorStreamSerializer:
        NoOpSelfRegisteringStreamSerializer<GetOrganizationIdFromEntitySetEntryProcessor>()
{
    override fun read(`in`: ObjectDataInput?): GetOrganizationIdFromEntitySetEntryProcessor{
        return GetOrganizationIdFromEntitySetEntryProcessor()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.GET_ORG_ID_FROM_ENTITY_SET_EP.ordinal
    }

    override fun getClazz(): Class<out GetOrganizationIdFromEntitySetEntryProcessor> {
        return GetOrganizationIdFromEntitySetEntryProcessor::class.java
    }
}