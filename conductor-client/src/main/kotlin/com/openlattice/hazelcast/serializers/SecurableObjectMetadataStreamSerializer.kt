package com.openlattice.hazelcast.serializers

import com.geekbeast.mappers.mappers.ObjectMappers
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.geekbeast.rhizome.hazelcast.serializers.SetStreamSerializers
import com.openlattice.datasets.SecurableObjectMetadata
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component

@Component
class SecurableObjectMetadataStreamSerializer : TestableSelfRegisteringStreamSerializer<SecurableObjectMetadata> {

    companion object {
        private val mapper = ObjectMappers.getJsonMapper()
    }

    override fun generateTestValue(): SecurableObjectMetadata {
        return TestDataFactory.securableObjectMetadata()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SECURABLE_OBJECT_METADATA.ordinal
    }

    override fun getClazz(): Class<out SecurableObjectMetadata> {
        return SecurableObjectMetadata::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: SecurableObjectMetadata) {
        out.writeUTF(`object`.title)
        out.writeUTF(`object`.description)
        SetStreamSerializers.fastStringSetSerialize(out, `object`.contacts)
        SetStreamSerializers.fastStringSetSerialize(out, `object`.flags)
        out.writeUTF(mapper.writeValueAsString(`object`.metadata))
    }

    override fun read(`in`: ObjectDataInput): SecurableObjectMetadata {
        val title = `in`.readString()!!
        val description = `in`.readString()!!
        val contacts = SetStreamSerializers.orderedFastStringSetDeserialize(`in`)
        val flags = SetStreamSerializers.orderedFastStringSetDeserialize(`in`)
        val metadata = mapper.readValue(`in`.readString()!!, jacksonTypeRef<MutableMap<String, Any>>())

        return SecurableObjectMetadata(title, description, contacts, flags, metadata)
    }
}