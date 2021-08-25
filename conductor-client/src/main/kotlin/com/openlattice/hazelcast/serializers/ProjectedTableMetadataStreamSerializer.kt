package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.openlattice.collaborations.ProjectedTableMetadata
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component

@Component
class ProjectedTableMetadataStreamSerializer : TestableSelfRegisteringStreamSerializer<ProjectedTableMetadata> {

    override fun generateTestValue(): ProjectedTableMetadata {
        return TestDataFactory.projectedTableMetadata()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.PROJECTED_TABLE_METADATA.ordinal
    }

    override fun getClazz(): Class<out ProjectedTableMetadata> {
        return ProjectedTableMetadata::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: ProjectedTableMetadata) {
        UUIDStreamSerializerUtils.serialize(out, `object`.organizationId)
        out.writeUTF(`object`.tableName)
    }

    override fun read(`in`: ObjectDataInput): ProjectedTableMetadata {
        val organizationId = UUIDStreamSerializerUtils.deserialize(`in`)
        val tableName = `in`.readString()!!

        return ProjectedTableMetadata(organizationId, tableName)
    }
}