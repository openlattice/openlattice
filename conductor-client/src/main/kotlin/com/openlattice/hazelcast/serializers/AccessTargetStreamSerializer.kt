package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.authorization.AccessTarget
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
@Component
class AccessTargetStreamSerializer: SelfRegisteringStreamSerializer<AccessTarget> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ACCESS_TARGET.ordinal
    }

    override fun getClazz(): Class<out AccessTarget> {
        return AccessTarget::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: AccessTarget) {
        AclKeyStreamSerializer.serialize(out, `object`.aclKey)
        PermissionStreamSerializer.serialize(out, `object`.permission)
    }

    override fun read(`in`: ObjectDataInput): AccessTarget {
        return AccessTarget(
                AclKeyStreamSerializer.deserialize(`in`),
                PermissionStreamSerializer.deserialize(`in`)
        )
    }
}