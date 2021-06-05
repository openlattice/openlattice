package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.apps.processors.AddRoleToAppProcessor
import com.openlattice.mapstores.TestDataFactory

class AddRoleToAppProcessorStreamSerializerTest : AbstractStreamSerializerTest<AddRoleToAppProcessorStreamSerializer, AddRoleToAppProcessor>() {

    override fun createSerializer(): AddRoleToAppProcessorStreamSerializer {
        return AddRoleToAppProcessorStreamSerializer()
    }

    override fun createInput(): AddRoleToAppProcessor {
        return AddRoleToAppProcessor(TestDataFactory.appRole())
    }
}