package com.openlattice.hazelcast.serializers.shuttle

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.serializers.OptionalStreamSerializers
import com.openlattice.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.openlattice.shuttle.FlightPlanParametersUpdate
import org.springframework.stereotype.Component
import java.util.*

@Component
class FlightPlanParametersUpdateStreamSerializer : TestableSelfRegisteringStreamSerializer<FlightPlanParametersUpdate> {

    companion object {

        @JvmStatic
        fun serialize(output: ObjectDataOutput, obj: FlightPlanParametersUpdate) {
            OptionalStreamSerializers.serialize(output, obj.sql, ObjectDataOutput::writeUTF)
            OptionalStreamSerializers.serialize(output, obj.source) { out: ObjectDataOutput, source: Map<String, String> ->
                out.writeUTFArray(source.keys.map { it }.toTypedArray())
                out.writeUTFArray(source.values.map { it }.toTypedArray())
            }
            OptionalStreamSerializers.serializeList(output, obj.sourcePrimaryKeyColumns, ObjectDataOutput::writeUTF)
            OptionalStreamSerializers.serialize(output, obj.flightFilePath, ObjectDataOutput::writeUTF)
        }

        @JvmStatic
        fun deserialize(input: ObjectDataInput): FlightPlanParametersUpdate {
            val sql = OptionalStreamSerializers.deserialize(input, ObjectDataInput::readUTF)
            val source = OptionalStreamSerializers.deserialize(input) {
                val sourceKeys = input.readUTFArray().toList()
                val sourceValues = input.readUTFArray().toList()
                sourceKeys.zip(sourceValues) { key, value -> key to value }.toMap()
            }
            val sourcePKeyCols = OptionalStreamSerializers.deserializeList(input, ObjectDataInput::readUTF)
            val flightFilePath = OptionalStreamSerializers.deserialize(input, ObjectDataInput::readUTF)
            return FlightPlanParametersUpdate(sql, source, sourcePKeyCols, flightFilePath)
        }
    }

    override fun write(output: ObjectDataOutput, obj: FlightPlanParametersUpdate) {
        serialize(output, obj)
    }

    override fun read(input: ObjectDataInput): FlightPlanParametersUpdate {
        return deserialize(input)
    }

    override fun getClazz(): Class<out FlightPlanParametersUpdate> {
        return FlightPlanParametersUpdate::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.FLIGHT_PLAN_PARAMETERS_UPDATE.ordinal
    }

    override fun generateTestValue(): FlightPlanParametersUpdate {
        return FlightPlanParametersUpdate(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())
    }

}