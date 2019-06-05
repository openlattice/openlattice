package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.apps.processors.RemoveRoleFromAppConfigProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class RemoveRoleFromAppConfigProcessorStreamSerializer : SelfRegisteringStreamSerializer<RemoveRoleFromAppConfigProcessor> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.REMOVE_ROLE_FROM_APP_CONFIG_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out RemoveRoleFromAppConfigProcessor> {
        return RemoveRoleFromAppConfigProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: RemoveRoleFromAppConfigProcessor) {
        UUIDStreamSerializer.serialize(out, `object`.roleId)
    }

    override fun read(`in`: ObjectDataInput): RemoveRoleFromAppConfigProcessor {
        return RemoveRoleFromAppConfigProcessor(UUIDStreamSerializer.deserialize(`in`))
    }

}
