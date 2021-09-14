package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.authorization.processors.GetDbUsernameFromDbCredsEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
@Component
class GetDbUsernameFromDbCredsEntryProcessorStreamSerializer: NoOpSelfRegisteringStreamSerializer<GetDbUsernameFromDbCredsEntryProcessor>() {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.GET_DB_USERNAME_FROM_DB_CREDS_EP.ordinal
    }

    override fun getClazz(): Class<out GetDbUsernameFromDbCredsEntryProcessor> {
        return GetDbUsernameFromDbCredsEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput): GetDbUsernameFromDbCredsEntryProcessor{
        return GetDbUsernameFromDbCredsEntryProcessor()
    }
}