package com.openlattice.hazelcast.serializers

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.apps.processors.UpdateAppConfigSettingsProcessor
import com.openlattice.mapstores.TestDataFactory

class UpdateAppConfigSettingsProcessorStreamSerializerTest
    : AbstractStreamSerializerTest<UpdateAppConfigSettingsProcessorStreamSerializer, UpdateAppConfigSettingsProcessor>() {

    override fun createSerializer(): UpdateAppConfigSettingsProcessorStreamSerializer {
        return UpdateAppConfigSettingsProcessorStreamSerializer()
    }

    override fun createInput(): UpdateAppConfigSettingsProcessor {
        return UpdateAppConfigSettingsProcessor(TestDataFactory.app().defaultSettings, TestDataFactory.app().defaultSettings.keys)
    }
}