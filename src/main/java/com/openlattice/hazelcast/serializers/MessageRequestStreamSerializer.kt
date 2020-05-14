
package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers
import com.openlattice.codex.Base64Media
import com.openlattice.codex.MessageRequest
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component

@Component
class MessageRequestStreamSerializer : TestableSelfRegisteringStreamSerializer<MessageRequest> {

    companion object {

        fun serialize(out: ObjectDataOutput, `object`: MessageRequest) {
            UUIDStreamSerializer.serialize(out, `object`.organizationId)
            UUIDStreamSerializer.serialize(out, `object`.messageEntitySetId)
            out.writeUTF(`object`.messageContents)
            SetStreamSerializers.fastStringSetSerialize(out, `object`.phoneNumbers)
            out.writeUTF(`object`.senderId)

            out.writeBoolean(`object`.attachment != null)
            `object`.attachment?.let { Base64MediaStreamSerializer.serialize(out, it) }

            OffsetDateTimeStreamSerializer.serialize(out, `object`.scheduledDateTime)
        }

        fun deserialize(`in`: ObjectDataInput): MessageRequest {
            val organizationId = UUIDStreamSerializer.deserialize(`in`)
            val messageEntitySetId = UUIDStreamSerializer.deserialize(`in`)
            val messageContents = `in`.readUTF()
            val phoneNumbers = SetStreamSerializers.fastStringSetDeserialize(`in`)
            val senderId = `in`.readUTF()
            val attachment: Base64Media? = if (`in`.readBoolean()) Base64MediaStreamSerializer.deserialize(`in`) else null
            val scheduledDateTime = OffsetDateTimeStreamSerializer.deserialize(`in`)

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