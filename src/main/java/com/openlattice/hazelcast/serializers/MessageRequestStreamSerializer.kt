package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.codex.MessageRequest
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class MessageRequestStreamSerializer : SelfRegisteringStreamSerializer<MessageRequest> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.MESSAGE_REQUEST.ordinal;
    }

    override fun getClazz(): Class<out MessageRequest> {
        return MessageRequest::class.java
    }

    override fun write(out: ObjectDataOutput?, `object`: MessageRequest?) {
        UUIDStreamSerializer.serialize(out, `object`?.organizationId)
        out?.writeUTF( `object`?.messageContents )
        out?.writeUTF( `object`?.phoneNumber )
    }

    override fun read(`in`: ObjectDataInput?): MessageRequest {
        val organizationId = UUIDStreamSerializer.deserialize(`in`)
        val messageContents = `in`!!.readUTF()
        val phoneNumber = `in`.readUTF()
        return MessageRequest(organizationId, messageContents, phoneNumber)
    }

}