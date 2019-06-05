package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.apps.processors.AddRoleToAppConfigProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class AddRoleToAppConfigProcessorStreamSerializer : SelfRegisteringStreamSerializer<AddRoleToAppConfigProcessor> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ADD_ROLE_TO_APP_CONFIG_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out AddRoleToAppConfigProcessor> {
        return AddRoleToAppConfigProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: AddRoleToAppConfigProcessor) {
        UUIDStreamSerializer.serialize(out, `object`.roleId)
        AclKeyStreamSerializer.serialize(out, `object`.roleAclKey)
    }

    override fun read(`in`: ObjectDataInput): AddRoleToAppConfigProcessor {
        val roleId = UUIDStreamSerializer.deserialize(`in`)
        val roleAclkey = AclKeyStreamSerializer.deserialize(`in`)
        return AddRoleToAppConfigProcessor(roleId, roleAclkey)
    }

}
