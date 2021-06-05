package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.apps.processors.RemoveRoleFromAppConfigProcessor
import java.util.*

class RemoveRoleFromAppConfigProcessorStreamSerializerTest
    : AbstractStreamSerializerTest<RemoveRoleFromAppConfigProcessorStreamSerializer, RemoveRoleFromAppConfigProcessor>() {

    override fun createSerializer(): RemoveRoleFromAppConfigProcessorStreamSerializer {
        return RemoveRoleFromAppConfigProcessorStreamSerializer()
    }

    override fun createInput(): RemoveRoleFromAppConfigProcessor {
        return RemoveRoleFromAppConfigProcessor(UUID.randomUUID())
    }
}