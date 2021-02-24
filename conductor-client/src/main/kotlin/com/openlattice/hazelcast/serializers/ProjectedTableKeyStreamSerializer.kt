package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.openlattice.collaborations.ProjectedTableKey
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component

@Component
class ProjectedTableKeyStreamSerializer : TestableSelfRegisteringStreamSerializer<ProjectedTableKey> {

    override fun generateTestValue(): ProjectedTableKey {
        return TestDataFactory.projectedTableKey()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.PROJECTED_TABLE_KEY.ordinal
    }

    override fun getClazz(): Class<out ProjectedTableKey> {
        return ProjectedTableKey::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: ProjectedTableKey) {
        UUIDStreamSerializerUtils.serialize(out, `object`.tableId)
        UUIDStreamSerializerUtils.serialize(out, `object`.collaborationId)
    }

    override fun read(`in`: ObjectDataInput): ProjectedTableKey {
        val tableId = UUIDStreamSerializerUtils.deserialize(`in`)
        val collaborationId = UUIDStreamSerializerUtils.deserialize(`in`)

        return ProjectedTableKey(tableId, collaborationId)
    }
}