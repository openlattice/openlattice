package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.apps.processors.UpdateDefaultAppSettingsProcessor
import com.openlattice.mapstores.TestDataFactory

class UpdateDefaultAppSettingsProcessorStreamSerializerTest
    : AbstractStreamSerializerTest<UpdateDefaultAppSettingsProcessorStreamSerializer, UpdateDefaultAppSettingsProcessor>() {

    override fun createSerializer(): UpdateDefaultAppSettingsProcessorStreamSerializer {
        return UpdateDefaultAppSettingsProcessorStreamSerializer()
    }

    override fun createInput(): UpdateDefaultAppSettingsProcessor {
        return UpdateDefaultAppSettingsProcessor(TestDataFactory.app().defaultSettings)
    }
}