package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.apps.processors.AddRoleToAppConfigProcessor
import com.openlattice.mapstores.TestDataFactory
import java.util.*

class AddRoleToAppConfigProcessorStreamSerializerTest : AbstractStreamSerializerTest<AddRoleToAppConfigProcessorStreamSerializer, AddRoleToAppConfigProcessor>() {

    override fun createSerializer(): AddRoleToAppConfigProcessorStreamSerializer {
        return AddRoleToAppConfigProcessorStreamSerializer()
    }

    override fun createInput(): AddRoleToAppConfigProcessor {
        return AddRoleToAppConfigProcessor(UUID.randomUUID(), TestDataFactory.aclKey())
    }
}