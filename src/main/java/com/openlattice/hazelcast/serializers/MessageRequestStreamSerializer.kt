package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
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
    }

    override fun read(`in`: ObjectDataInput): MessageRequest {
        val organizationId = UUIDStreamSerializer.deserialize(`in`)
        val messageEntitySetId = UUIDStreamSerializer.deserialize(`in`)
        val messageContents = `in`.readUTF()
        val phoneNumber = `in`.readUTF()
        val senderId = `in`.readUTF()

        return MessageRequest(organizationId, messageEntitySetId, messageContents, phoneNumber, senderId)
    }

    override fun generateTestValue(): MessageRequest {
        return TestDataFactory.messageRequest()
    }
}