package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organizations.Organization
import com.openlattice.organizations.processors.OrganizationEntryProcessor
import org.apache.commons.lang.RandomStringUtils
import org.junit.Assert
import java.io.Serializable
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class OrganizationEntryProcessorStreamSerializerTest : AbstractStreamSerializerTest<OrganizationsEntryProcessorStreamSerializer, OrganizationEntryProcessor>(), Serializable {
    private var test  = RandomStringUtils.random(32)
    private val inputOrganization = TestDataFactory.organization()
    private val outputOrganization = Organization(
            inputOrganization.securablePrincipal,
            inputOrganization.emailDomains.toMutableSet(),
            inputOrganization.members.toMutableSet(),
            inputOrganization.roles.toMutableSet(),
            inputOrganization.smsEntitySetInfo.toMutableSet(),
            inputOrganization.partitions.toMutableList()
    )

    override fun createSerializer(): OrganizationsEntryProcessorStreamSerializer {
        return OrganizationsEntryProcessorStreamSerializer()
    }

    override fun createInput(): OrganizationEntryProcessor {
        val t  = RandomStringUtils.random(32)
        test = t
        return OrganizationEntryProcessor {
            it.emailDomains.add(t)
        }
    }

    override fun testOutput(inputObject: OrganizationEntryProcessor, outputObject: OrganizationEntryProcessor) {
//        super.testOutput(inputObject, outputObject)
        Assert.assertFalse(inputOrganization.emailDomains.contains(test))
        Assert.assertFalse(outputOrganization.emailDomains.contains(test))

        inputObject.process(mutableMapOf(UUID.randomUUID() to inputOrganization).entries.first())
        outputObject.process(mutableMapOf(UUID.randomUUID() to outputOrganization as Organization?).entries.first())

        Assert.assertTrue(inputOrganization.emailDomains.contains(test))
        Assert.assertTrue(outputOrganization.emailDomains.contains(test))

    }
}