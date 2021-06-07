package com.openlattice.hazelcast.serializers.shuttle

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.openlattice.hazelcast.InternalTestDataFactory
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.serializers.OptionalStreamSerializers
import com.openlattice.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.openlattice.shuttle.Integration
import org.springframework.stereotype.Component

@Component
class IntegrationStreamSerializer : TestableSelfRegisteringStreamSerializer<Integration> {

    companion object {
        fun serialize(output: ObjectDataOutput, obj: Integration) {
            UUIDStreamSerializerUtils.serialize(output, obj.key)
            EnvironmentStreamSerializer.serialize(output, obj.environment)
            output.writeUTF(obj.s3bucket)
            output.writeUTFArray(obj.contacts.toTypedArray())
            UUIDStreamSerializerUtils.serialize(output, obj.organizationId)
            OptionalStreamSerializers.serialize(output, obj.logEntitySetId, UUIDStreamSerializerUtils::serialize)
            OptionalStreamSerializers.serialize(output, obj.maxConnections, ObjectDataOutput::writeInt)
            OptionalStreamSerializers.serializeList(output, obj.callbackUrls, ObjectDataOutput::writeUTF)
            output.writeUTFArray(obj.flightPlanParameters.keys.toTypedArray())
            obj.flightPlanParameters.values.forEach { FlightPlanParametersStreamSerializer.serialize(output, it) }
        }

        fun deserialize(input: ObjectDataInput): Integration {
            val key = UUIDStreamSerializerUtils.deserialize(input)
            val environment = EnvironmentStreamSerializer.deserialize(input)
            val s3bucket = input.readUTF()
            val contacts = input.readUTFArray().toSet()
            val orgId = UUIDStreamSerializerUtils.deserialize(input)
            val logEntitySetId = OptionalStreamSerializers.deserialize(input, UUIDStreamSerializerUtils::deserialize)
            val maxConnections = OptionalStreamSerializers.deserialize(input, ObjectDataInput::readInt)
            val callbackUrls = OptionalStreamSerializers.deserializeList(input, ObjectDataInput::readUTF)
            val flightPlanParamsKeys = input.readUTFArray()
            val flightPlanParamsValues = flightPlanParamsKeys.map{
                FlightPlanParametersStreamSerializer.deserialize(input)
            }.toList()
            val flightPlanParams = flightPlanParamsKeys.zip(flightPlanParamsValues).toMap().toMutableMap()
            return Integration(
                    key,
                    environment,
                    s3bucket,
                    contacts,
                    orgId,
                    logEntitySetId,
                    maxConnections,
                    callbackUrls,
                    flightPlanParams
            )
        }
    }

    override fun write(output: ObjectDataOutput, obj: Integration) {
        serialize(output, obj)
    }

    override fun read(input: ObjectDataInput): Integration {
        return deserialize(input)
    }

    override fun getClazz(): Class<out Integration> {
        return Integration::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.INTEGRATION.ordinal
    }

    override fun generateTestValue(): Integration {
        return InternalTestDataFactory.integration()
    }

}