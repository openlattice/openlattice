package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.transporter.processors.GetPropertyTypesFromTransporterColumnSetEntryProcessor

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
class GetPropertyTypesFromTransporterColumnSetEntryProcessorStreamSerializer:
        NoOpSelfRegisteringStreamSerializer<GetPropertyTypesFromTransporterColumnSetEntryProcessor>()
{
    override fun read(`in`: ObjectDataInput?): GetPropertyTypesFromTransporterColumnSetEntryProcessor{
        return GetPropertyTypesFromTransporterColumnSetEntryProcessor()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.GET_PROPERTY_TYPES_TRANSPORTER_COL_SET_EP.ordinal
    }

    override fun getClazz(): Class<out GetPropertyTypesFromTransporterColumnSetEntryProcessor> {
        return GetPropertyTypesFromTransporterColumnSetEntryProcessor::class.java
    }
}