package com.openlattice.hazelcast.serializers

import com.geekbeast.rhizome.jobs.JobStatus
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import org.springframework.stereotype.Component

@Component
class JobStatusStreamSerializer : AbstractEnumSerializer<JobStatus>() {

    companion object {
        @JvmStatic
        fun serialize(out: ObjectDataOutput, `object`: JobStatus) = AbstractEnumSerializer.serialize(out, `object`)

        @JvmStatic
        fun deserialize(`in`: ObjectDataInput): JobStatus = deserialize(JobStatus::class.java, `in`)
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.JOB_STATUS.ordinal
    }

    override fun getClazz(): Class<JobStatus> {
        return JobStatus::class.java
    }
}