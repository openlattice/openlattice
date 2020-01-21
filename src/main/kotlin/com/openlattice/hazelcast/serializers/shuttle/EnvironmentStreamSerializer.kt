package com.openlattice.hazelcast.serializers.shuttle

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.client.RetrofitFactory
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.serializers.AbstractEnumSerializer
import org.springframework.stereotype.Component

@Component
class EnvironmentStreamSerializer : AbstractEnumSerializer<RetrofitFactory.Environment>() {
    companion object {
        @JvmStatic
        fun serialize(output: ObjectDataOutput, obj: RetrofitFactory.Environment) = AbstractEnumSerializer.serialize(output, obj)
        @JvmStatic
        fun deserialize(input: ObjectDataInput): RetrofitFactory.Environment = deserialize(RetrofitFactory.Environment::class.java, input) as RetrofitFactory.Environment
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.ENVIRONMENT.ordinal
    }

    override fun getClazz(): Class<out RetrofitFactory.Environment> {
        return RetrofitFactory.Environment::class.java
    }

    override fun generateTestValue(): RetrofitFactory.Environment {
        return RetrofitFactory.Environment.LOCAL
    }
}