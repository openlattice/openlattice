package com.openlattice.hazelcast.serializers.shuttle

import com.geekbeast.mappers.mappers.ObjectMappers
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.InternalTestDataFactory
import com.geekbeast.hazelcast.serializers.StreamSerializers
import com.geekbeast.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.openlattice.shuttle.Flight
import com.openlattice.shuttle.FlightPlanParameters
import org.springframework.stereotype.Component

@Component
class FlightPlanParametersStreamSerializer : TestableSelfRegisteringStreamSerializer<FlightPlanParameters> {

    companion object {
        private val mapper = ObjectMappers.getJsonMapper()

        fun serialize(output: ObjectDataOutput, obj: FlightPlanParameters) {
            output.writeUTF(obj.sql)
            output.writeUTFArray(obj.source.keys.map { it }.toTypedArray())
            output.writeUTFArray(obj.source.values.map { it }.toTypedArray())
            output.writeUTFArray(obj.sourcePrimaryKeyColumns.toTypedArray())
            StreamSerializers.serializeMaybeValue(output, obj.flightFilePath) {
                output.writeUTF(obj.flightFilePath!!)
            }
            val flightJson = mapper.writeValueAsString(obj.flight)
            output.writeUTF(flightJson)
        }

        fun deserialize(input: ObjectDataInput): FlightPlanParameters {
            val sql = input.readString()!!
            val sourceKeys = input.readStringArray()!!.toList()
            val sourceValues = input.readStringArray()!!.toList()
            val source = sourceKeys.zip(sourceValues) { key, value -> key to value }.toMap()
            val srcPkeyCols = input.readStringArray()!!.toList()
            val flightFilePath = StreamSerializers.deserializeMaybeValue(input) {
                input.readString()!!
            }
            val flightJson = input.readString()!!
            val flight = mapper.readValue(flightJson, Flight::class.java)
            return FlightPlanParameters(
                    sql,
                    source,
                    srcPkeyCols,
                    flightFilePath,
                    flight
            )
        }

    }

    override fun write(output: ObjectDataOutput, obj: FlightPlanParameters) {
        serialize(output, obj)
    }

    override fun read(input: ObjectDataInput): FlightPlanParameters {
        return deserialize(input)
    }

    override fun getClazz(): Class<out FlightPlanParameters> {
        return FlightPlanParameters::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.FLIGHT_PLAN_PARAMETERS.ordinal
    }

    override fun generateTestValue(): FlightPlanParameters {
        return InternalTestDataFactory.flightPlanParameters()
    }
}