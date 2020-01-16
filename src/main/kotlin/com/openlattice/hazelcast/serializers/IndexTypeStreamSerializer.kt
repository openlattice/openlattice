package com.openlattice.hazelcast.serializers

import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.postgres.IndexType
import org.springframework.stereotype.Component

@Component
class IndexTypeStreamSerializer : AbstractEnumSerializer<IndexType>() {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.INDEX_TYPE.ordinal
    }

    override fun getClazz(): Class<out Enum<IndexType>> {
        return IndexType::class.java
    }

    override fun generateTestValue(): Enum<IndexType> {
        return IndexType.HASH
    }
}