package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.apps.processors.UpdateDefaultAppSettingsProcessor
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class UpdateDefaultAppSettingsProcessorStreamSerializer : SelfRegisteringStreamSerializer<UpdateDefaultAppSettingsProcessor> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UPDATE_DEFAULT_APP_SETTINGS_PROCESSOR.ordinal
    }

    override fun getClazz(): Class<out UpdateDefaultAppSettingsProcessor> {
        return UpdateDefaultAppSettingsProcessor::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: UpdateDefaultAppSettingsProcessor) {
        out.writeInt( `object`.newSettings.size)

        `object`.newSettings.forEach {
            out.writeUTF(it.key)
            out.writeObject(it.value)
        }
    }

    override fun read(`in`: ObjectDataInput): UpdateDefaultAppSettingsProcessor {
        val size = `in`.readInt()

        val settings = (0 until size).associate {
            val key: String = `in`.readUTF()
            val value: Any = `in`.readObject<Any>()

            key to value
        }

        return UpdateDefaultAppSettingsProcessor(settings)
    }
}