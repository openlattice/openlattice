package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.notifications.sms.SmsEntitySetInformation

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class SmsEntitySetInformationStreamSerializer : SelfRegisteringStreamSerializer<SmsEntitySetInformation> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SMS_ENTITY_SET_INFORMATION.ordinal
    }

    override fun getClazz(): Class<out SmsEntitySetInformation> {
        return SmsEntitySetInformation::class.java
    }

    override fun write(out: ObjectDataOutput, obj: SmsEntitySetInformation) {
        serialize(out, obj)
    }

    override fun read(input: ObjectDataInput): SmsEntitySetInformation {
        return deserialize(input)
    }

    companion object {
        @JvmStatic
        fun serialize(out: ObjectDataOutput, obj: SmsEntitySetInformation) {
            out.writeUTF(obj.phoneNumber)
            UUIDStreamSerializer.serialize(out, obj.organizationId)
            SetStreamSerializers.fastUUIDSetSerialize(out, obj.entitySetIds)
            SetStreamSerializers.fastOrderedStringSetSerializeAsArray(out, obj.tags)
        }

        @JvmStatic
        fun deserialize(input: ObjectDataInput): SmsEntitySetInformation {
            val phoneNumber = input.readUTF()
            val organizationId = UUIDStreamSerializer.deserialize(input)
            val entitySetIds = SetStreamSerializers.fastUUIDSetDeserialize(input)
            val tags = SetStreamSerializers.fastOrderedStringSetDeserializeAsArray(input)
            return SmsEntitySetInformation(
                    phoneNumber,
                    organizationId,
                    entitySetIds,
                    tags
            )
        }
    }
}