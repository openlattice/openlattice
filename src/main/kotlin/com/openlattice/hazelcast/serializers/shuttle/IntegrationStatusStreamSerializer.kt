package com.openlattice.hazelcast.serializers.shuttle

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.serializers.AbstractEnumSerializer
import com.openlattice.shuttle.IntegrationStatus

class IntegrationStatusStreamSerializer : AbstractEnumSerializer<IntegrationStatus>() {
    companion object {
        @JvmStatic
        fun serialize(output: ObjectDataOutput, obj: IntegrationStatus) = AbstractEnumSerializer.serialize(output, obj)
        @JvmStatic
        fun deserialize(input: ObjectDataInput): IntegrationStatus = deserialize(IntegrationStatus::class.java, input) as IntegrationStatus
    }

    override fun generateTestValue(): IntegrationStatus {
        return IntegrationStatus.IN_PROGRESS
    }

    override fun getClazz(): Class<out IntegrationStatus> {
        return IntegrationStatus::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.INTEGRATION_STATUS.ordinal
    }

}