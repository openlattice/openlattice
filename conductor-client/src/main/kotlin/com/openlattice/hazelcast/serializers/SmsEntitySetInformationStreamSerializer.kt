package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.geekbeast.rhizome.hazelcast.serializers.SetStreamSerializers
import com.geekbeast.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.geekbeast.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.geekbeast.serializers.Jdk8StreamSerializers
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.notifications.sms.SmsEntitySetInformation
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

@Component
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
            UUIDStreamSerializerUtils.serialize(out, obj.organizationId)
            SetStreamSerializers.fastUUIDSetSerialize(out, obj.entitySetIds)
            SetStreamSerializers.fastOrderedStringSetSerializeAsArray(out, obj.tags)
            Jdk8StreamSerializers.AbstractOffsetDateTimeStreamSerializer.serialize(out, obj.lastSync)
        }

        @JvmStatic
        fun deserialize(input: ObjectDataInput): SmsEntitySetInformation {
            val phoneNumber = input.readString()!!
            val organizationId = UUIDStreamSerializerUtils.deserialize(input)
            val entitySetIds = SetStreamSerializers.fastUUIDSetDeserialize(input)
            val tags = SetStreamSerializers.fastOrderedStringSetDeserializeFromArray(input)
            val lastSync = Jdk8StreamSerializers.AbstractOffsetDateTimeStreamSerializer.deserialize(input)

            return SmsEntitySetInformation(
                    phoneNumber,
                    organizationId,
                    entitySetIds,
                    tags,
                    lastSync
            )
        }
    }
}