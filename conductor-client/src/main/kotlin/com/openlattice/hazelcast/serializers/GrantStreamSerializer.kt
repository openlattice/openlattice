package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organizations.Grant
import org.springframework.stereotype.Component

@Component
class GrantStreamSerializer : SelfRegisteringStreamSerializer<Grant> {

    companion object {

        @JvmStatic
        fun serialize(out: ObjectDataOutput, `object`: Grant) {
            GrantTypeStreamSerializer.serialize(out, `object`.grantType)
            SetStreamSerializers.fastStringSetSerialize(out, `object`.mappings)
            out.writeUTF(`object`.attribute)
        }

        @JvmStatic
        fun deserialize(`in`: ObjectDataInput ): Grant {
            val gt = GrantTypeStreamSerializer.deserialize(`in`)
            val mappings = SetStreamSerializers.fastStringSetDeserialize(`in`)
            val attr = `in`.readUTF()
            return Grant(gt, mappings, attr )
        }
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.GRANT.ordinal
    }

    override fun getClazz(): Class<out Grant> {
        return Grant::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: Grant) {
        serialize(out, `object`)
    }

    override fun read(`in`: ObjectDataInput): Grant {
        return deserialize(`in`)
    }
}
