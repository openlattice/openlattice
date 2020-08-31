package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.apps.processors.RemoveRoleFromAppProcessor
import java.util.*

class RemoveRoleFromAppProcessorStreamSerializerTest : AbstractStreamSerializerTest<RemoveRoleFromAppProcessorStreamSerializer, RemoveRoleFromAppProcessor>() {

    override fun createSerializer(): RemoveRoleFromAppProcessorStreamSerializer {
        return RemoveRoleFromAppProcessorStreamSerializer()
    }

    override fun createInput(): RemoveRoleFromAppProcessor {
        return RemoveRoleFromAppProcessor(UUID.randomUUID())
    }
}