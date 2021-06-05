package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.principals.PrincipalExistsEntryProcessor
import org.springframework.stereotype.Component

@Component
class PrincipalExistsEntryProcessorStreamSerializer : NoOpSelfRegisteringStreamSerializer<PrincipalExistsEntryProcessor>() {
    override fun getClazz(): Class<out PrincipalExistsEntryProcessor> {
        return PrincipalExistsEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput?): PrincipalExistsEntryProcessor {
        return PrincipalExistsEntryProcessor()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.PRINCIPAL_EXISTS_ENTRY_PROCESSOR.ordinal
    }

}