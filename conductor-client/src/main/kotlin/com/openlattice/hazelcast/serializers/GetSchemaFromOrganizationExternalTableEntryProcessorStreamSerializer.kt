package com.openlattice.hazelcast.serializers

import com.openlattice.edm.processors.GetSchemaFromOrganizationExternalTableEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
@Component
class GetSchemaFromOrganizationExternalTableEntryProcessorStreamSerializer: NoOpSelfRegisteringStreamSerializer<GetSchemaFromOrganizationExternalTableEntryProcessor>() {
    override fun getClazz(): Class<out GetSchemaFromOrganizationExternalTableEntryProcessor> {
        return GetSchemaFromOrganizationExternalTableEntryProcessor::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.GET_SCHEMA_FROM_ORG_EXT_TABLE_EP.ordinal
    }
}