package com.openlattice.hazelcast.serializers.shuttle

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.InternalTestDataFactory
import com.openlattice.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.openlattice.hazelcast.serializers.UUIDStreamSerializer
import com.openlattice.shuttle.QueuedIntegrationJob
import org.springframework.stereotype.Component
import java.util.*

@Component
class QueuedIntegrationJobStreamSerializer : TestableSelfRegisteringStreamSerializer<QueuedIntegrationJob> {
    companion object {
        fun serialize(output: ObjectDataOutput, obj: QueuedIntegrationJob) {
            UUIDStreamSerializer.serialize(output, obj.jobId)
            IntegrationJobStreamSerializer.serialize(output, obj.integrationJob)
        }

        fun deserialize(input: ObjectDataInput): QueuedIntegrationJob {
            val jobId = UUIDStreamSerializer.deserialize(input)
            val integrationJob = IntegrationJobStreamSerializer.deserialize(input)
            return QueuedIntegrationJob(jobId, integrationJob)
        }
    }

    override fun write(output: ObjectDataOutput, obj: QueuedIntegrationJob) {
        serialize(output, obj)
    }

    override fun read(input: ObjectDataInput): QueuedIntegrationJob {
        return deserialize(input)
    }

    override fun getClazz(): Class<out QueuedIntegrationJob> {
        return QueuedIntegrationJob::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.QUEUED_INTEGRATION_JOB.ordinal
    }

    override fun generateTestValue(): QueuedIntegrationJob {
        return QueuedIntegrationJob(UUID.randomUUID(), InternalTestDataFactory.integrationJob())
    }

}