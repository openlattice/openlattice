package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.ListStreamSerializers
import com.openlattice.authorization.AclKey
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component

@Component
class AclKeyStreamSerializer : TestableSelfRegisteringStreamSerializer<AclKey> {

    companion object {

        @JvmStatic
        fun serialize(out: ObjectDataOutput, `object`: AclKey) {
            ListStreamSerializers.fastUUIDListSerialize(out, `object`)
        }

        @JvmStatic
        fun deserialize(`in`: ObjectDataInput): AclKey {
            return AclKey(*ListStreamSerializers.fastUUIDArrayDeserialize(`in`))
        }
    }

    override fun generateTestValue(): AclKey {
        return TestDataFactory.aclKey()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ACL_KEY.ordinal
    }

    override fun getClazz(): Class<out AclKey> {
        return AclKey::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: AclKey) {
        serialize(out, `object`)
    }

    override fun read(`in`: ObjectDataInput): AclKey {
        return deserialize(`in`)
    }
}