package com.openlattice.hazelcast.serializers.shuttle

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.InternalTestDataFactory
import com.openlattice.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.openlattice.shuttle.IntegrationJob
import org.springframework.stereotype.Component

@Component
class IntegrationJobStreamSerializer : TestableSelfRegisteringStreamSerializer<IntegrationJob> {

    companion object {
        fun serialize(output: ObjectDataOutput, obj: IntegrationJob) {
            output.writeUTF(obj.integrationName)
            IntegrationStatusStreamSerializer.serialize(output, obj.integrationStatus)
        }

        fun deserialize(input: ObjectDataInput): IntegrationJob {
            val name = input.readUTF()
            val status = IntegrationStatusStreamSerializer.deserialize(input)
            return IntegrationJob(name, status)
        }
    }

    override fun write(output: ObjectDataOutput, obj: IntegrationJob) {
        serialize(output, obj)
    }

    override fun read(input: ObjectDataInput): IntegrationJob {
        return deserialize(input)
    }

    override fun getClazz(): Class<out IntegrationJob> {
        return IntegrationJob::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.INTEGRATION_JOB.ordinal
    }

    override fun generateTestValue(): IntegrationJob {
        return InternalTestDataFactory.integrationJob()
    }

}