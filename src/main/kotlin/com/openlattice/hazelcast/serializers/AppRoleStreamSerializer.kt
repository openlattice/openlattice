package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.apps.AppRole
import com.openlattice.authorization.Permission
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.util.*

@Component
class AppRoleStreamSerializer : SelfRegisteringStreamSerializer<AppRole> {

    companion object {
        private val PERMISSIONS = Permission.values()

        fun serialize(out: ObjectDataOutput, `object`: AppRole) {
            UUIDStreamSerializer.serialize(out, `object`.id)
            out.writeUTF(`object`.name)
            out.writeUTF(`object`.title)
            out.writeUTF(`object`.description)

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

        fun deserialize(`in`: ObjectDataInput): AppRole {

            val id = UUIDStreamSerializer.deserialize(`in`)
            val name = `in`.readUTF()
            val title = `in`.readUTF()
            val description = `in`.readUTF()

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

            return AppRole(Optional.of(id), name, title, Optional.of(description), permissionsMap)
        }
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.APP_ROLE.ordinal
    }

    override fun getClazz(): Class<out AppRole> {
        return AppRole::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: AppRole) {
        serialize(out, `object`)
    }

    override fun read(`in`: ObjectDataInput): AppRole {
        return deserialize(`in`)
    }

}