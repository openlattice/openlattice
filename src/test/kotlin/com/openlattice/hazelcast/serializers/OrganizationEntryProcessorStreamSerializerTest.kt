package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organizations.Organization
import com.openlattice.organizations.processors.OrganizationEntryProcessor
import org.apache.commons.lang.RandomStringUtils
import org.junit.Assert

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class OrganizationEntryProcessorStreamSerializerTest : AbstractStreamSerializerTest<OrganizationsEntryProcessorStreamSerializer, OrganizationEntryProcessor>() {
    private val test = RandomStringUtils.random(32)
    private val inputOrganization = TestDataFactory.organization()
    private val outputOrganization = Organization(
            inputOrganization.securablePrincipal,
            inputOrganization.autoApprovedEmails,
            inputOrganization.members,
            inputOrganization.roles,
            inputOrganization.smsEntitySetInfo,
            inputOrganization.partitions
    )

    override fun createSerializer(): OrganizationsEntryProcessorStreamSerializer {
        return OrganizationsEntryProcessorStreamSerializer()
    }

    override fun createInput(): OrganizationEntryProcessor {
        return OrganizationEntryProcessor {
            it.autoApprovedEmails.add(test)
        }
    }

    override fun testOutput(inputObject: OrganizationEntryProcessor, outputObject: OrganizationEntryProcessor) {
        super.testOutput(inputObject, outputObject)
        Assert.assertFalse(inputOrganization.autoApprovedEmails.contains(test))
        Assert.assertFalse(outputOrganization.autoApprovedEmails.contains(test))

        inputObject.update( inputOrganization )
        outputObject.update( outputOrganization )

        Assert.assertTrue(inputOrganization.autoApprovedEmails.contains(test))
        Assert.assertTrue(outputOrganization.autoApprovedEmails.contains(test))

        Assert.assertEquals( inputObject, outputObject )
    }
}