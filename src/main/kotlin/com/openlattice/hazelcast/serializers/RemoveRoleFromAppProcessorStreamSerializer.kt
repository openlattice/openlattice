package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.apps.processors.RemoveRoleFromAppProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class RemoveRoleFromAppProcessorStreamSerializer : SelfRegisteringStreamSerializer<RemoveRoleFromAppProcessor> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.REMOVE_ROLE_FROM_APP_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out RemoveRoleFromAppProcessor> {
        return RemoveRoleFromAppProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: RemoveRoleFromAppProcessor) {
        UUIDStreamSerializer.serialize(out, `object`.roleId)
    }

    override fun read(`in`: ObjectDataInput): RemoveRoleFromAppProcessor {
        return RemoveRoleFromAppProcessor(UUIDStreamSerializer.deserialize(`in`))
    }
}