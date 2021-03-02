package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.authorization.processors.AuthorizationEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class AuthorizationEntryProcessorStreamSerializer : NoOpSelfRegisteringStreamSerializer<AuthorizationEntryProcessor>() {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.AUTHORIZATION_ENTRY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out AuthorizationEntryProcessor> {
        return AuthorizationEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput?): AuthorizationEntryProcessor {
        return AuthorizationEntryProcessor()
    }
}