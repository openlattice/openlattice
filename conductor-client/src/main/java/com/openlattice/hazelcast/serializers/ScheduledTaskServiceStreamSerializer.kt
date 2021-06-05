package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.scheduling.ScheduledTaskService
import org.springframework.stereotype.Component

@Component
class ScheduledTaskServiceStreamSerializer : NoOpSelfRegisteringStreamSerializer<ScheduledTaskService>() {

    override fun getClazz(): Class<out ScheduledTaskService> {
        return ScheduledTaskService::class.java
    }

    override fun read(`in`: ObjectDataInput?): ScheduledTaskService {
        return ScheduledTaskService()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SCHEDULED_TASK_SERVICE.ordinal
    }
}