package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.edm.processors.GetFqnFromPropertyTypeEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class GetFqnFromPropertyTypeEntryProcessorStreamSerializer: NoOpSelfRegisteringStreamSerializer<GetFqnFromPropertyTypeEntryProcessor>() {
    override fun read(`in`: ObjectDataInput?): GetFqnFromPropertyTypeEntryProcessor {
        return GetFqnFromPropertyTypeEntryProcessor()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.INITIALIZE_ORGANIZATION_ASSEMBLY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out GetFqnFromPropertyTypeEntryProcessor> {
        return GetFqnFromPropertyTypeEntryProcessor::class.java
    }
}