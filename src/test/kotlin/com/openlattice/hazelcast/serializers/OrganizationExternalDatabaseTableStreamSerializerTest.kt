package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organization.OrganizationExternalDatabaseTable

class OrganizationExternalDatabaseTableStreamSerializerTest :
        AbstractStreamSerializerTest<OrganizationExternalDatabaseTableStreamSerializer, OrganizationExternalDatabaseTable>() {

    override fun createSerializer(): OrganizationExternalDatabaseTableStreamSerializer {
        return OrganizationExternalDatabaseTableStreamSerializer()
    }

    override fun createInput(): OrganizationExternalDatabaseTable {
        return TestDataFactory.organizationExternalDatabaseTable()
    }
}