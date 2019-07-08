package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.notifications.sms.SmsInformationKey
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class SmsInformationKeyStreamSerializer : SelfRegisteringStreamSerializer<SmsInformationKey> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SMS_INFORMATION_KEY.ordinal
    }

    override fun getClazz(): Class<out SmsInformationKey> {
        return SmsInformationKey::class.java
    }

    override fun write(out: ObjectDataOutput, obj: SmsInformationKey) {
        out.writeUTF(obj.phoneNumber)
        UUIDStreamSerializer.serialize(out, obj.organizationId)
    }

    override fun read(input: ObjectDataInput): SmsInformationKey {
        val phoneNumber = input.readUTF()
        val organizationId = UUIDStreamSerializer.deserialize(input)
        return SmsInformationKey(phoneNumber, organizationId)
    }
}