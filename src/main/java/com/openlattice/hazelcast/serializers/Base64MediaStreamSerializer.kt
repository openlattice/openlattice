package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.codex.Base64Media
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component

@Component
class Base64MediaStreamSerializer : TestableSelfRegisteringStreamSerializer<Base64Media> {

    companion object {
        fun serialize(out: ObjectDataOutput, `object`: Base64Media) {
            out.writeUTF(`object`.contentType)
            out.writeUTF(`object`.data)
        }

        fun deserialize(`in`: ObjectDataInput): Base64Media {
            val contentType = `in`.readUTF()
            val data = `in`.readUTF()
            return Base64Media(contentType, data)
        }
    }

    override fun generateTestValue(): Base64Media {
        return TestDataFactory.base64Media()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.BASE_64_MEDIA.ordinal
    }

    override fun getClazz(): Class<out Base64Media> {
        return Base64Media::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: Base64Media) {
        serialize(out, `object`)
    }

    override fun read(`in`: ObjectDataInput): Base64Media {
        return deserialize(`in`)
    }
}