package com.openlattice.hazelcast.serializers

import com.dataloom.mappers.ObjectMappers
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.InternalTestDataFactory
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.scheduling.RunnableTask
import com.openlattice.scheduling.ScheduledTask
import org.springframework.stereotype.Component

@Component
class ScheduledTaskStreamSerializer : TestableSelfRegisteringStreamSerializer<ScheduledTask> {

    companion object {
        val mapper = ObjectMappers.getJsonMapper()
    }

    override fun generateTestValue(): ScheduledTask {
        return InternalTestDataFactory.scheduledTask()

    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SCHEDULED_TASK.ordinal
    }

    override fun getClazz(): Class<out ScheduledTask> {
        return ScheduledTask::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: ScheduledTask) {
        UUIDStreamSerializer.serialize(out, `object`.id)
        OffsetDateTimeStreamSerializer.serialize(out, `object`.scheduledDateTime)

        out.writeUTF(`object`.task.javaClass.name)
        out.writeUTF(mapper.writeValueAsString(`object`.task))

    }

    override fun read(`in`: ObjectDataInput): ScheduledTask {
        val id = UUIDStreamSerializer.deserialize(`in`)
        val scheduledDateTime = OffsetDateTimeStreamSerializer.deserialize(`in`)

        val clazz = Class.forName(`in`.readUTF()) as Class<out RunnableTask>
        val task = mapper.readValue(`in`.readUTF(), clazz)

        return ScheduledTask(id, scheduledDateTime, task)
    }
}