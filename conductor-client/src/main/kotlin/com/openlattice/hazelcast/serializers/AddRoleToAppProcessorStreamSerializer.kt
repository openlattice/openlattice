package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.apps.processors.AddRoleToAppProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class AddRoleToAppProcessorStreamSerializer : SelfRegisteringStreamSerializer<AddRoleToAppProcessor> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ADD_ROLE_TO_APP_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out AddRoleToAppProcessor> {
        return AddRoleToAppProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: AddRoleToAppProcessor) {
        AppRoleStreamSerializer.serialize(out, `object`.role)
    }

    override fun read(`in`: ObjectDataInput): AddRoleToAppProcessor {
        return AddRoleToAppProcessor(AppRoleStreamSerializer.deserialize(`in`))
    }
}