package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.processors.UpdateMaterializedEntitySetProcessor
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organizations.processors.UpdateOrganizationSmsEntitySetInformationEntryProcessor
import org.mockito.Mockito

class UpdateOrganizationSmsEntitySetInformationEntryProcessorStreamSerializerTest  : AbstractStreamSerializerTest<UpdateOrganizationSmsEntitySetInformationEntryProcessorStreamSerializer,
        UpdateOrganizationSmsEntitySetInformationEntryProcessor>() {
    override fun createSerializer(): UpdateOrganizationSmsEntitySetInformationEntryProcessorStreamSerializer {
        return UpdateOrganizationSmsEntitySetInformationEntryProcessorStreamSerializer()
    }

    override fun createInput(): UpdateOrganizationSmsEntitySetInformationEntryProcessor {
        return UpdateOrganizationSmsEntitySetInformationEntryProcessor(listOf(TestDataFactory.smsEntitySetInformation(), TestDataFactory.smsEntitySetInformation()))
    }

}