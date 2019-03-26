package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.hazelcast.processors.RemoveMemberOfOrganizationEntryProcessor
import org.apache.commons.lang3.RandomStringUtils

class RemoveMembersOfOrganizationEntryProcessorStreamSerializerTest
    : AbstractStreamSerializerTest<RemoveMemberOfOrganizationEntryProcessorStreamSerializer,
        RemoveMemberOfOrganizationEntryProcessor>() {

    override fun createSerializer(): RemoveMemberOfOrganizationEntryProcessorStreamSerializer {
        return RemoveMemberOfOrganizationEntryProcessorStreamSerializer()
    }

    override fun createInput(): RemoveMemberOfOrganizationEntryProcessor {
        return RemoveMemberOfOrganizationEntryProcessor( setOf( RandomStringUtils.randomAlphabetic( 5 ) ) );
    }

}