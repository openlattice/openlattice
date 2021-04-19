package com.openlattice.hazelcast.serializers

import com.openlattice.edm.processors.GetSchemaFromExternalTableEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
@Component
class GetSchemaFromExternalTableEntryProcessorStreamSerializer : NoOpSelfRegisteringStreamSerializer<GetSchemaFromExternalTableEntryProcessor>() {
    override fun getClazz(): Class<out GetSchemaFromExternalTableEntryProcessor> {
        return GetSchemaFromExternalTableEntryProcessor::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.GET_SCHEMA_FROM_EXT_TABLE_EP.ordinal
    }
}