package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.authorization.Permission
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class PermissionStreamSerializer : AbstractEnumSerializer<Permission>() {
    companion object {
        @JvmStatic
        fun serialize(out: ObjectDataOutput, `object`: Permission) = AbstractEnumSerializer.serialize(out, `object`)

        @JvmStatic
        fun deserialize(`in`: ObjectDataInput): Permission = deserialize(Permission::class.java, `in`)
    }


    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.PERMISSION.ordinal
    }

    override fun getClazz(): Class<out Permission> {
        return Permission::class.java
    }
}