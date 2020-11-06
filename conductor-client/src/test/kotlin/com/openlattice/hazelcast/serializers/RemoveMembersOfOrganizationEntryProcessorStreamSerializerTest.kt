package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.hazelcast.processors.RemoveMemberOfOrganizationEntryProcessor
import com.openlattice.mapstores.TestDataFactory

class RemoveMembersOfOrganizationEntryProcessorStreamSerializerTest
    : AbstractStreamSerializerTest<RemoveMemberOfOrganizationEntryProcessorStreamSerializer,
        RemoveMemberOfOrganizationEntryProcessor>() {

    override fun createSerializer(): RemoveMemberOfOrganizationEntryProcessorStreamSerializer {
        return RemoveMemberOfOrganizationEntryProcessorStreamSerializer()
    }

    override fun createInput(): RemoveMemberOfOrganizationEntryProcessor {
        return RemoveMemberOfOrganizationEntryProcessor( setOf( TestDataFactory.randomAlphabetic( 5 ) ) )
    }

}