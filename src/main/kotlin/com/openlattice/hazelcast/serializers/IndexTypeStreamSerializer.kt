package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.postgres.IndexType
import org.springframework.stereotype.Component

@Component
class IndexTypeStreamSerializer : AbstractEnumSerializer<IndexType>() {

    companion object {
        @JvmStatic
        fun serialize(out: ObjectDataOutput, `object`: IndexType) =  AbstractEnumSerializer.serialize(out, `object`)
        @JvmStatic
        fun deserialize(`in`: ObjectDataInput): IndexType = deserialize(IndexType::class.java, `in`)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.INDEX_TYPE.ordinal
    }

    override fun getClazz(): Class<IndexType> {
        return IndexType::class.java
    }
}