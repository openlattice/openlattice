package com.openlattice.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.codex.SendCodexMessageTask
import com.openlattice.hazelcast.InternalTestDataFactory
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class ScheduledMessageTaskStreamSerializer : TestableSelfRegisteringStreamSerializer<SendCodexMessageTask> {

    override fun generateTestValue(): SendCodexMessageTask {
        return InternalTestDataFactory.sendCodexMessageTask()
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.SEND_CODEX_MESSAGE_TASK.ordinal
    }

    override fun getClazz(): Class<out SendCodexMessageTask> {
        return SendCodexMessageTask::class.java
    }

    override fun write(out: ObjectDataOutput, `object`: SendCodexMessageTask) {
        MessageRequestStreamSerializer.serialize(out, `object`.message)
    }

    override fun read(`in`: ObjectDataInput): SendCodexMessageTask {
        val messageRequest = MessageRequestStreamSerializer.deserialize(`in`)
        return SendCodexMessageTask(messageRequest)
    }
}