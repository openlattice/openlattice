package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.authorization.PrincipalType
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
@Component
class PrincipalTypeStreamSerializer: AbstractEnumSerializer<PrincipalType>() {

    companion object {
        @JvmStatic
        fun serialize(out: ObjectDataOutput, `object`: PrincipalType) =  AbstractEnumSerializer.serialize(out, `object`)
        @JvmStatic
        fun deserialize(`in`: ObjectDataInput): PrincipalType = deserialize(PrincipalType::class.java, `in`)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.PRINCIPAL_TYPE.ordinal
    }

    override fun getClazz(): Class<PrincipalType> {
        return PrincipalType::class.java
    }
}