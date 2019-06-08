package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class SubscriptionNotificationTaskStreamSerializer : SelfRegisteringStreamSerializer<SubscriptionNotificationTaskStreamSerializer> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SUBSCRIPTION_NOTIFICATION_TASK.ordinal
    }

    override fun getClazz(): Class<out SubscriptionNotificationTaskStreamSerializer> {
        return SubscriptionNotificationTaskStreamSerializer::class.java
    }

    override fun write(out: ObjectDataOutput?, `object`: SubscriptionNotificationTaskStreamSerializer?) {

    }

    override fun read(`in`: ObjectDataInput?): SubscriptionNotificationTaskStreamSerializer {
        return SubscriptionNotificationTaskStreamSerializer()
    }

}