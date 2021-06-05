package com.openlattice.hazelcast.serializers.shuttle

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.hazelcast.serializers.UUIDStreamSerializerUtils
import com.openlattice.client.RetrofitFactory
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.serializers.OptionalStreamSerializers
import com.openlattice.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.openlattice.shuttle.FlightPlanParametersUpdate
import com.openlattice.shuttle.IntegrationUpdate
import org.springframework.stereotype.Component
import java.util.*

@Component
class IntegrationUpdateStreamSerializer : TestableSelfRegisteringStreamSerializer<IntegrationUpdate> {

    companion object {
        fun serialize(output: ObjectDataOutput, obj: IntegrationUpdate) {
            OptionalStreamSerializers.serialize(output, obj.environment) { out: ObjectDataOutput, env: RetrofitFactory.Environment ->
                EnvironmentStreamSerializer.serialize(out, env)
            }
            OptionalStreamSerializers.serialize(output, obj.s3bucket, ObjectDataOutput::writeUTF)
            OptionalStreamSerializers.serializeSet(output, obj.contacts, ObjectDataOutput::writeUTF)
            OptionalStreamSerializers.serialize(output, obj.organizationId) { out: ObjectDataOutput, orgId: UUID ->
                UUIDStreamSerializerUtils.serialize(out, orgId)
            }
            OptionalStreamSerializers.serialize(output, obj.maxConnections, ObjectDataOutput::writeInt)
            OptionalStreamSerializers.serializeList(output, obj.callbackUrls, ObjectDataOutput::writeUTF)
            if (obj.flightPlanParameters.isPresent) {
                output.writeBoolean(true)
                val flightPlanParameters = obj.flightPlanParameters.get()
                output.writeInt(flightPlanParameters.size)
                output.writeUTFArray(flightPlanParameters.keys.map { it }.toTypedArray())
                flightPlanParameters.values.forEach {
                    FlightPlanParametersUpdateStreamSerializer.serialize(output, it)
                }
            } else {
                output.writeBoolean(false)
            }
        }

        fun deserialize(input: ObjectDataInput): IntegrationUpdate {
            val env = OptionalStreamSerializers.deserialize(input) {
                EnvironmentStreamSerializer.deserialize(it)
            }
            val s3bucket = OptionalStreamSerializers.deserialize(input, ObjectDataInput::readUTF)
            val contacts = OptionalStreamSerializers.deserializeSet(input, ObjectDataInput::readUTF)
            val orgId = OptionalStreamSerializers.deserialize(input) {
                UUIDStreamSerializerUtils.deserialize(it)
            }
            val maxConnections = OptionalStreamSerializers.deserialize(input, ObjectDataInput::readInt)
            val callbackUrls = OptionalStreamSerializers.deserializeList(input, ObjectDataInput::readUTF)
            val flightPlanParameters = if (input.readBoolean()) {
                val size = input.readInt()
                val keys = input.readUTFArray()
                val vals = mutableListOf<FlightPlanParametersUpdate>()
                for (i in 0 until size) {
                    vals.add(FlightPlanParametersUpdateStreamSerializer.deserialize(input))
                }
                Optional.of(keys.zip(vals).toMap())
            } else {
                Optional.empty()
            }
            return IntegrationUpdate(
                    env,
                    s3bucket,
                    contacts,
                    orgId,
                    maxConnections,
                    callbackUrls,
                    flightPlanParameters
            )
        }
    }

    override fun write(output: ObjectDataOutput, obj: IntegrationUpdate) {
        serialize(output, obj)
    }

    override fun read(input: ObjectDataInput): IntegrationUpdate {
        return deserialize(input)
    }

    override fun getClazz(): Class<out IntegrationUpdate> {
        return IntegrationUpdate::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.INTEGRATION_UPDATE.ordinal
    }

    override fun generateTestValue(): IntegrationUpdate {
        return IntegrationUpdate(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())
    }

}