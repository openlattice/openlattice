package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.apps.processors.UpdateAppRolePermissionsProcessor
import com.openlattice.authorization.Permission
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.util.*

@Component
class UpdateAppRolePermissionsProcessorStreamSerializer : SelfRegisteringStreamSerializer<UpdateAppRolePermissionsProcessor> {

    companion object {
        private val PERMISSIONS = Permission.values()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UPDATE_APP_ROLE_PERMISSIONS_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out UpdateAppRolePermissionsProcessor> {
        return UpdateAppRolePermissionsProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: UpdateAppRolePermissionsProcessor) {
        UUIDStreamSerializer.serialize(out, `object`.roleId)

        out.writeInt(`object`.permissions.size)
        `object`.permissions.forEach {
            out.writeInt(it.key.ordinal)
            out.writeInt(it.value.size)

            it.value.forEach { entitySetId, propertyTypeIds ->
                UUIDStreamSerializer.serialize(out, entitySetId)
                OptionalStreamSerializers.serialize(out, propertyTypeIds, SetStreamSerializers::fastUUIDSetSerialize)
            }
        }
    }

    override fun read(`in`: ObjectDataInput): UpdateAppRolePermissionsProcessor {
        val roleId = UUIDStreamSerializer.deserialize(`in`)

        val permissionsMapSize = `in`.readInt()
        val permissionsMap = HashMap<Permission, Map<UUID, Optional<Set<UUID>>>>(permissionsMapSize)

        for (i in 0 until permissionsMapSize) {

            val permission = PERMISSIONS[`in`.readInt()]

            val childMapSize = `in`.readInt()
            val childMap = HashMap<UUID, Optional<Set<UUID>>>(childMapSize)

            for (j in 0 until childMapSize) {
                val entitySetId = UUIDStreamSerializer.deserialize(`in`)
                val propertyTypeIds: Optional<Set<UUID>> = OptionalStreamSerializers.deserialize(`in`, SetStreamSerializers::fastUUIDSetDeserialize)

                childMap[entitySetId] = propertyTypeIds
            }

            permissionsMap[permission] = childMap
        }

        return UpdateAppRolePermissionsProcessor(roleId, permissionsMap)
    }
}