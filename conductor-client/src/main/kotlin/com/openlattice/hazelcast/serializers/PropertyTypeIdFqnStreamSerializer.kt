package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.edm.PropertyTypeIdFqn
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.util.UUID

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
@Component
class PropertyTypeIdFqnStreamSerializer : SelfRegisteringStreamSerializer<PropertyTypeIdFqn> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.PROPERTY_TYPE_ID_FQN.ordinal
    }

    override fun write(out: ObjectDataOutput, `object`: PropertyTypeIdFqn) {
        out.writeLong(`object`.id.mostSignificantBits)
        out.writeLong(`object`.id.leastSignificantBits)
        FullQualifiedNameStreamSerializer.serialize(out, `object`.fqn)
    }

    override fun read(`in`: ObjectDataInput): PropertyTypeIdFqn {
        val ms = `in`.readLong()
        val ls = `in`.readLong()
        val fqn = FullQualifiedNameStreamSerializer.deserialize(`in`)
        return PropertyTypeIdFqn(UUID(ms, ls), fqn)
    }

    override fun getClazz(): Class<out PropertyTypeIdFqn> {
        return PropertyTypeIdFqn::class.java
    }
}