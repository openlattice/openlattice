package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.codex.ScheduledMessageTask
import com.openlattice.hazelcast.InternalTestDataFactory
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class ScheduledMessageTaskStreamSerializer : TestableSelfRegisteringStreamSerializer<ScheduledMessageTask> {

    override fun generateTestValue(): ScheduledMessageTask {
        return InternalTestDataFactory.scheduledMessageTask()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SCHEDULED_MESSAGE_TASK.ordinal
    }

    override fun getClazz(): Class<out ScheduledMessageTask> {
        return ScheduledMessageTask::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: ScheduledMessageTask) {
        MessageRequestStreamSerializer.serialize(out, `object`.message)
    }

    override fun read(`in`: ObjectDataInput): ScheduledMessageTask {
        val messageRequest = MessageRequestStreamSerializer.deserialize(`in`)
        return ScheduledMessageTask(messageRequest)
    }
}