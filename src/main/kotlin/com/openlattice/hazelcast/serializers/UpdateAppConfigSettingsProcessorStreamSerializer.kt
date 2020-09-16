package com.openlattice.hazelcast.serializers

import com.dataloom.mappers.ObjectMappers
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.apps.processors.UpdateAppConfigSettingsProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class UpdateAppConfigSettingsProcessorStreamSerializer : SelfRegisteringStreamSerializer<UpdateAppConfigSettingsProcessor> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UPDATE_APP_CONFIG_SETTINGS_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out UpdateAppConfigSettingsProcessor> {
        return UpdateAppConfigSettingsProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: UpdateAppConfigSettingsProcessor) {
        out.writeInt( `object`.settingsToAdd.size)

        `object`.settingsToAdd.forEach {
            out.writeUTF(it.key)
            out.writeObject(it.value)
        }

        SetStreamSerializers.fastStringSetSerialize(out, `object`.settingsToRemove)
    }

    override fun read(`in`: ObjectDataInput): UpdateAppConfigSettingsProcessor {
        val size = `in`.readInt()

        val settingsToAdd = (0 until size).associate {
            val key: String = `in`.readUTF()
            val value: Any = `in`.readObject<Any>()

            key to value
        }

        val settingsToRemove = SetStreamSerializers.fastStringSetDeserialize(`in`)

        return UpdateAppConfigSettingsProcessor(settingsToAdd, settingsToRemove)

    }
}