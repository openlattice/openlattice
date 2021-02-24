package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.openlattice.collaborations.AlterOrganizationsInCollaborationEntryProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component
import java.util.*

@Component
class AlterOrganizationsInCollaborationEntryProcessorStreamSerializer : TestableSelfRegisteringStreamSerializer<AlterOrganizationsInCollaborationEntryProcessor> {

    override fun generateTestValue(): AlterOrganizationsInCollaborationEntryProcessor {
        return AlterOrganizationsInCollaborationEntryProcessor(setOf(UUID.randomUUID()), true)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ALTER_ORGS_IN_COLLAB_EP.ordinal
    }

    override fun getClazz(): Class<out AlterOrganizationsInCollaborationEntryProcessor> {
        return AlterOrganizationsInCollaborationEntryProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: AlterOrganizationsInCollaborationEntryProcessor) {
        SetStreamSerializers.fastUUIDSetSerialize(out, `object`.organizationIds)
        out.writeBoolean(`object`.isAdding)
    }

    override fun read(`in`: ObjectDataInput): AlterOrganizationsInCollaborationEntryProcessor {
        val organizationIds = SetStreamSerializers.fastOrderedUUIDSetDeserialize(`in`)
        val isAdding = `in`.readBoolean()

        return AlterOrganizationsInCollaborationEntryProcessor(organizationIds, isAdding)
    }
}