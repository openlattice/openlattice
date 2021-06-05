package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.processors.GetMembersOfOrganizationEntryProcessor
import org.springframework.stereotype.Component

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@Component
class GetMembersOfOrganizationEntryProcessorStreamSerializer : NoOpSelfRegisteringStreamSerializer<GetMembersOfOrganizationEntryProcessor>() {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.GET_MEMBERS_FROM_ORG_EP.ordinal
    }

    override fun getClazz(): Class<out GetMembersOfOrganizationEntryProcessor> {
        return GetMembersOfOrganizationEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput?): GetMembersOfOrganizationEntryProcessor {
        return GetMembersOfOrganizationEntryProcessor()
    }
}