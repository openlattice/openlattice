package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class EntitySetFlagStreamSerializer : AbstractEnumSerializer<EntitySetFlag>() {

    companion object {
        @JvmStatic
        fun serialize(out: ObjectDataOutput, `object`: EntitySetFlag) =  AbstractEnumSerializer.serialize(out, `object`)
        @JvmStatic
        fun deserialize(`in`: ObjectDataInput): EntitySetFlag = deserialize(EntitySetFlag::class.java, `in`)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ENTITY_SET_FLAG.ordinal
    }

    override fun getClazz(): Class<EntitySetFlag> {
        return EntitySetFlag::class.java
    }
}