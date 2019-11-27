package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.organizations.GrantType
import org.springframework.stereotype.Component

@Component
class GrantTypeStreamSerializer : AbstractEnumSerializer<GrantType>() {

    companion object {
        @JvmStatic
        fun serialize(out: ObjectDataOutput, `object`: GrantType) =  AbstractEnumSerializer.serialize(out, `object`)
        @JvmStatic
        fun deserialize(`in`: ObjectDataInput ): GrantType = deserialize(GrantType::class.java, `in`) as GrantType
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.GRANT_TYPE.ordinal
    }

    override fun getClazz(): Class<out Enum<GrantType>> {
        return GrantType::class.java
    }

    override fun generateTestValue(): Enum<GrantType> {
        return GrantType.Manual
    }
}