package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.InternalTestDataFactory
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.notifications.sms.SubscriptionNotification
import org.springframework.stereotype.Component

@Component
class SubscriptionNotificationStreamSerializer() : TestableSelfRegisteringStreamSerializer<SubscriptionNotification> {

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SUBSCRIPTION_NOTIFICATION.ordinal
    }

    override fun getClazz(): Class<out SubscriptionNotification> {
        return SubscriptionNotification::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: SubscriptionNotification) {
        out.writeUTF(`object`.messageContents)
        out.writeUTF(`object`.phoneNumber)
    }

    override fun read(`in`: ObjectDataInput): SubscriptionNotification {
        val messageContents = `in`.readUTF()
        val phoneNumber = `in`.readUTF()

        return SubscriptionNotification(messageContents, phoneNumber)
    }

    override fun generateTestValue(): SubscriptionNotification {
        return InternalTestDataFactory.subscriptionNotification()
    }
}