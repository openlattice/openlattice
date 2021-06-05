package com.openlattice.hazelcast.serializers

import com.dataloom.mappers.ObjectMappers
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.openlattice.conductor.rpc.ReIndexEntitySetMetadataLambdas
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.io.IOException
import java.util.*

@Component
class ReIndexEntitySetMetadataLambdasStreamSerializer(
        private val mapper: ObjectMapper = ObjectMappers.getSmileMapper()
) : Serializer<ReIndexEntitySetMetadataLambdas>() {

    companion object {
        private val logger = LoggerFactory.getLogger(ReIndexEntitySetMetadataLambdasStreamSerializer::class.java)
    }

    private fun writeUUID(output: Output, id: UUID) {
        output.writeLong(id.leastSignificantBits)
        output.writeLong(id.mostSignificantBits)
    }

    private fun readUUID(input: Input): UUID {
        val lsb = input.readLong()
        val msb = input.readLong()
        return UUID(msb, lsb)
    }

    override fun write(kryo: Kryo, output: Output, `object`: ReIndexEntitySetMetadataLambdas) {
        try {
            output.writeInt(`object`.entitySets.size)

            `object`.entitySets.forEach { (entitySet, propertyTypeIds) ->

                val bytes = mapper.writeValueAsBytes(entitySet)
                output.writeInt(bytes.size)
                output.writeBytes(bytes)

                output.writeInt(propertyTypeIds.size)
                propertyTypeIds.forEach { writeUUID(output, it) }

            }

            output.writeInt(`object`.propertyTypes.size)
            `object`.propertyTypes.forEach { (propertyTypeId, propertyType) ->

                writeUUID(output, propertyTypeId)

                val bytes = mapper.writeValueAsBytes(propertyType)
                output.writeInt(bytes.size)
                output.writeBytes(bytes)
            }


        } catch (e: JsonProcessingException) {
            logger.debug("Unable to serialize entity set metadata reindex request", e)
        }
    }

    override fun read(kryo: Kryo, input: Input, type: Class<ReIndexEntitySetMetadataLambdas>): ReIndexEntitySetMetadataLambdas {
        try {
            val entitySetsSize = input.readInt()

            val entitySets = (0 until entitySetsSize).associate {
                val entitySetBytes = input.readInt()
                val entitySet = mapper.readValue(input.readBytes(entitySetBytes), EntitySet::class.java)

                val numPropertyTypes = input.readInt()
                val propertyTypeIds = (0 until numPropertyTypes).map { readUUID(input) }.toSet()

                entitySet to propertyTypeIds
            }

            val propertyTypesSize = input.readInt()

            val propertyTypes = (0 until propertyTypesSize).associate {

                val propertyTypeId = readUUID(input)

                val propertyTypeBytes = input.readInt()
                val propertyType = mapper.readValue(input.readBytes(propertyTypeBytes), PropertyType::class.java)

                propertyTypeId to propertyType
            }

            return ReIndexEntitySetMetadataLambdas(entitySets, propertyTypes)

        } catch (e: IOException) {
            logger.debug("Unable to deserialize entity set metadata reindex request", e)
        }

        return ReIndexEntitySetMetadataLambdas(mapOf(), mapOf())
    }
}