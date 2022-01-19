
package com.openlattice.hazelcast.serializers

import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.geekbeast.rhizome.hazelcast.serializers.SetStreamSerializers
import com.geekbeast.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.geekbeast.serializers.Jdk8StreamSerializers
import com.openlattice.codex.Base64Media
import com.openlattice.codex.MessageRequest
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component

@Component
class MessageRequestStreamSerializer : TestableSelfRegisteringStreamSerializer<MessageRequest> {

    companion object {

        fun serialize(out: ObjectDataOutput, `object`: MessageRequest) {
            UUIDStreamSerializerUtils.serialize(out, `object`.organizationId)
            UUIDStreamSerializerUtils.serialize(out, `object`.messageEntitySetId)
            out.writeUTF(`object`.messageContents)
            SetStreamSerializers.fastStringSetSerialize(out, `object`.phoneNumbers)
            out.writeUTF(`object`.senderId)

            out.writeBoolean(`object`.attachment != null)
            `object`.attachment?.let { Base64MediaStreamSerializer.serialize(out, it) }

            Jdk8StreamSerializers.AbstractOffsetDateTimeStreamSerializer.serialize(out, `object`.scheduledDateTime)
        }

        fun deserialize(`in`: ObjectDataInput): MessageRequest {
            val organizationId = UUIDStreamSerializerUtils.deserialize(`in`)
            val messageEntitySetId = UUIDStreamSerializerUtils.deserialize(`in`)
            val messageContents = `in`.readString()!!
            val phoneNumbers = SetStreamSerializers.fastStringSetDeserialize(`in`)
            val senderId = `in`.readString()!!
            val attachment: Base64Media? = if (`in`.readBoolean()) Base64MediaStreamSerializer.deserialize(`in`) else null
            val scheduledDateTime = Jdk8StreamSerializers.AbstractOffsetDateTimeStreamSerializer.deserialize(`in`)

            return MessageRequest(organizationId, messageEntitySetId, messageContents, phoneNumbers, senderId, attachment, scheduledDateTime)
        }

    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.MESSAGE_REQUEST.ordinal
    }

    override fun getClazz(): Class<out MessageRequest> {
        return MessageRequest::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: MessageRequest) {
        serialize(out, `object`)
    }

    override fun read(`in`: ObjectDataInput): MessageRequest {
        return deserialize(`in`)
    }

    override fun generateTestValue(): MessageRequest {
        return TestDataFactory.messageRequest()
    }
}