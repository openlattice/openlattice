package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.processors.RemoveMemberOfOrganizationEntryProcessor
import org.springframework.stereotype.Component

@Component
class RemoveMemberOfOrganizationEntryProcessorStreamSerializer
    : SelfRegisteringStreamSerializer<RemoveMemberOfOrganizationEntryProcessor> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.REMOVE_MEMBER_OF_ORGANIZATION_ENTRY_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out RemoveMemberOfOrganizationEntryProcessor> {
        return RemoveMemberOfOrganizationEntryProcessor::class.java
    }

    override fun read(input: ObjectDataInput?): RemoveMemberOfOrganizationEntryProcessor {
        return RemoveMemberOfOrganizationEntryProcessor(SetStreamSerializers.deserialize(input) { it.readUTF() })
    }

    override fun write(out: ObjectDataOutput, obj: RemoveMemberOfOrganizationEntryProcessor?) {
        SetStreamSerializers.serialize(out, obj!!.backingCollection) { out.writeUTF(it.id) }
    }

}
