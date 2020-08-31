package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.apps.AppRole
import com.openlattice.mapstores.TestDataFactory

class AppRoleStreamSerializerTest : AbstractStreamSerializerTest<AppRoleStreamSerializer, AppRole>() {

    override fun createSerializer(): AppRoleStreamSerializer {
        return AppRoleStreamSerializer()
    }

    override fun createInput(): AppRole {
        return TestDataFactory.appRole()
    }
}