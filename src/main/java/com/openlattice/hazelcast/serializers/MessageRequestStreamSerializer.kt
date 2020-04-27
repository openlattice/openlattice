package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.codex.Base64Media
import com.openlattice.codex.MessageRequest
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.mapstores.TestDataFactory
import org.springframework.stereotype.Component

@Component
class MessageRequestStreamSerializer : TestableSelfRegisteringStreamSerializer<MessageRequest> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.MESSAGE_REQUEST.ordinal
    }

    override fun getClazz(): Class<out MessageRequest> {
        return MessageRequest::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: MessageRequest) {
        UUIDStreamSerializer.serialize(out, `object`.organizationId)
        UUIDStreamSerializer.serialize(out, `object`.messageEntitySetId)
        out.writeUTF(`object`.messageContents)
        out.writeUTF(`object`.phoneNumber)
        out.writeUTF(`object`.senderId)

        out.writeBoolean(`object`.attachment != null)
        `object`.attachment?.let { Base64MediaStreamSerializer.serialize(out, it) }
    }

    override fun read(`in`: ObjectDataInput): MessageRequest {
        val organizationId = UUIDStreamSerializer.deserialize(`in`)
        val messageEntitySetId = UUIDStreamSerializer.deserialize(`in`)
        val messageContents = `in`.readUTF()
        val phoneNumber = `in`.readUTF()
        val senderId = `in`.readUTF()
        val attachment: Base64Media? = if (`in`.readBoolean()) Base64MediaStreamSerializer.deserialize(`in`) else null

        return MessageRequest(organizationId, messageEntitySetId, messageContents, phoneNumber, senderId, attachment)
    }

    override fun generateTestValue(): MessageRequest {
        return TestDataFactory.messageRequest()
    }
}