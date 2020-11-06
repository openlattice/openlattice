package com.openlattice.hazelcast.serializers.organizations

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.processors.organizations.UpdateOrganizationExternalDatabaseTableEntryProcessor
import com.openlattice.hazelcast.serializers.MetadataUpdateStreamSerializer
import com.openlattice.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component

@Component
class UpdateOrganizationExternalDatabaseTableEntryProcessorStreamSerializer
    : TestableSelfRegisteringStreamSerializer<UpdateOrganizationExternalDatabaseTableEntryProcessor> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UPDATE_ORG_EXTERNAL_DB_TABLE_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out UpdateOrganizationExternalDatabaseTableEntryProcessor> {
        return UpdateOrganizationExternalDatabaseTableEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput): UpdateOrganizationExternalDatabaseTableEntryProcessor {
        return UpdateOrganizationExternalDatabaseTableEntryProcessor(MetadataUpdateStreamSerializer.deserialize(`in`))
    }

    override fun write(out: ObjectDataOutput, `object`: UpdateOrganizationExternalDatabaseTableEntryProcessor) {
        MetadataUpdateStreamSerializer.serialize(out, `object`.update)
    }

    override fun generateTestValue(): UpdateOrganizationExternalDatabaseTableEntryProcessor {
        return UpdateOrganizationExternalDatabaseTableEntryProcessor(TestDataFactory.metadataUpdate())
    }
}