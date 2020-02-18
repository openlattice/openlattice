package com.openlattice.hazelcast.serializers.organizations

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.processors.organizations.UpdateOrganizationExternalDatabaseColumnEntryProcessor
import com.openlattice.hazelcast.serializers.MetadataUpdateStreamSerializer
import com.openlattice.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component

@Component
class UpdateOrganizationExternalDatabaseColumnEntryProcessorStreamSerializer
    : TestableSelfRegisteringStreamSerializer<UpdateOrganizationExternalDatabaseColumnEntryProcessor>{

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UPDATE_ORG_EXTERNAL_DB_COLUMN_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out UpdateOrganizationExternalDatabaseColumnEntryProcessor> {
        return UpdateOrganizationExternalDatabaseColumnEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput): UpdateOrganizationExternalDatabaseColumnEntryProcessor {
        return UpdateOrganizationExternalDatabaseColumnEntryProcessor(MetadataUpdateStreamSerializer.deserialize(`in`))
    }

    override fun write(out: ObjectDataOutput, `object`: UpdateOrganizationExternalDatabaseColumnEntryProcessor) {
        MetadataUpdateStreamSerializer.serialize(out, `object`.update)
    }

    override fun generateTestValue(): UpdateOrganizationExternalDatabaseColumnEntryProcessor {
        return UpdateOrganizationExternalDatabaseColumnEntryProcessor(TestDataFactory.metadataUpdate())
    }
}