package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.organization.OrganizationExternalDatabaseColumn

class OrganizationExternalDatabaseColumnStreamSerializerTest :
        AbstractStreamSerializerTest<OrganizationExternalDatabaseColumnStreamSerializer, OrganizationExternalDatabaseColumn>() {

    override fun createSerializer(): OrganizationExternalDatabaseColumnStreamSerializer {
        return OrganizationExternalDatabaseColumnStreamSerializer()
    }

    override fun createInput(): OrganizationExternalDatabaseColumn {
        return TestDataFactory.organizationExternalDatabaseColumn()
    }
}