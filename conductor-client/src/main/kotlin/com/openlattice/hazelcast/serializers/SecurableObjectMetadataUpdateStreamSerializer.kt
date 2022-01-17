package com.openlattice.hazelcast.serializers

import com.geekbeast.mappers.mappers.ObjectMappers
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.geekbeast.rhizome.hazelcast.serializers.SetStreamSerializers
import com.openlattice.datasets.SecurableObjectMetadataUpdate
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component

@Component
class SecurableObjectMetadataUpdateStreamSerializer : TestableSelfRegisteringStreamSerializer<SecurableObjectMetadataUpdate> {

    override fun generateTestValue(): SecurableObjectMetadataUpdate {
        return TestDataFactory.securableObjectMetadataUpdate()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SECURABLE_OBJECT_METADATA_UPDATE.ordinal
    }

    override fun getClazz(): Class<out SecurableObjectMetadataUpdate> {
        return SecurableObjectMetadataUpdate::class.java
    }

    companion object {
        private val mapper = ObjectMappers.getJsonMapper()

        private fun <T> serializeNullable(out: ObjectDataOutput, value: T?, consumer: (ObjectDataOutput, T) -> Any?) {
            val isPresent = value != null
            out.writeBoolean(isPresent)
            if (isPresent) {
                consumer(out, value!!)
            }
        }

        private fun <T> deserializeNullable(`in`: ObjectDataInput, consumer: (ObjectDataInput) -> T): T? {
            val isPresent = `in`.readBoolean()

            if (!isPresent) {
                return null
            }

            return consumer(`in`)
        }

        fun serialize(out: ObjectDataOutput, `object`: SecurableObjectMetadataUpdate) {
            serializeNullable(out, `object`.title) { o, v ->
                o.writeUTF(v)
            }
            serializeNullable(out, `object`.description) { o, v ->
                o.writeUTF(v)
            }
            serializeNullable(out, `object`.contacts) { o, v ->
                SetStreamSerializers.fastStringSetSerialize(o, v)
            }
            serializeNullable(out, `object`.flags) { o, v ->
                SetStreamSerializers.fastStringSetSerialize(o, v)
            }
            serializeNullable(out, `object`.metadata) { o, v ->
                o.writeUTF(mapper.writeValueAsString(v))
            }

        }

        fun deserialize(`in`: ObjectDataInput): SecurableObjectMetadataUpdate {
            val title = deserializeNullable(`in`) { it.readString()!! }
            val description = deserializeNullable(`in`) { it.readString()!! }
            val contacts = deserializeNullable(`in`) { SetStreamSerializers.orderedFastStringSetDeserialize(it) }
            val flags = deserializeNullable(`in`) { SetStreamSerializers.orderedFastStringSetDeserialize(it) }
            val metadata = deserializeNullable(`in`) { mapper.readValue(`in`.readString()!!, jacksonTypeRef<MutableMap<String, Any>>()) }

            return SecurableObjectMetadataUpdate(title, description, contacts, flags, metadata)
        }
    }

    override fun write(out: ObjectDataOutput, `object`: SecurableObjectMetadataUpdate) {
        serialize(out, `object`)
    }

    override fun read(`in`: ObjectDataInput): SecurableObjectMetadataUpdate {
        return deserialize(`in`)
    }
}