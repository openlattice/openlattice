package com.openlattice.hazelcast.serializers

import com.geekbeast.mappers.mappers.ObjectMappers
import com.fasterxml.jackson.databind.ObjectMapper
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.geekbeast.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.openlattice.hazelcast.InternalTestDataFactory
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.scheduling.RunnableTask
import com.openlattice.scheduling.ScheduledTask
import org.springframework.stereotype.Component

@Component
class ScheduledTaskStreamSerializer : TestableSelfRegisteringStreamSerializer<ScheduledTask> {

    companion object {
        val mapper: ObjectMapper = ObjectMappers.getJsonMapper()
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
        UUIDStreamSerializerUtils.serialize(out, `object`.id)
        Jdk8StreamSerializers.AbstractOffsetDateTimeStreamSerializer.serialize(out, `object`.scheduledDateTime)

        out.writeUTF(`object`.task.javaClass.name)
        out.writeUTF(mapper.writeValueAsString(`object`.task))

    }

    override fun read(`in`: ObjectDataInput): ScheduledTask {
        val id = UUIDStreamSerializerUtils.deserialize(`in`)
        val scheduledDateTime = Jdk8StreamSerializers.AbstractOffsetDateTimeStreamSerializer.deserialize(`in`)

        val clazz = Class.forName(`in`.readString()!!) as Class<out RunnableTask>
        val task = mapper.readValue(`in`.readString()!!, clazz)

        return ScheduledTask(id, scheduledDateTime, task)
    }
}