package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.subscriptions.SubscriptionNotificationTask
import org.springframework.stereotype.Component

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class SubscriptionNotificationTaskStreamSerializer : SelfRegisteringStreamSerializer<SubscriptionNotificationTask> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SUBSCRIPTION_NOTIFICATION_TASK.ordinal
    }

    override fun getClazz(): Class<out SubscriptionNotificationTask> {
        return SubscriptionNotificationTask::class.java
    }

    override fun write(out: ObjectDataOutput?, `object`: SubscriptionNotificationTask?) {

    }

    override fun read(`in`: ObjectDataInput?): SubscriptionNotificationTask {
        return SubscriptionNotificationTask()
    }

}