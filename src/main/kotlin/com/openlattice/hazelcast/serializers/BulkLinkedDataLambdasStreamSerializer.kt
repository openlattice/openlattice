package com.openlattice.hazelcast.serializers

import com.dataloom.mappers.ObjectMappers
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.openlattice.conductor.rpc.BulkLinkedDataLambdas
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.io.IOException
import java.util.UUID

@Component
class BulkLinkedDataLambdasStreamSerializer(
        private val mapper: ObjectMapper = ObjectMappers.getSmileMapper()
) : Serializer<BulkLinkedDataLambdas>() {

    companion object {
        private val logger = LoggerFactory.getLogger(BulkLinkedDataLambdasStreamSerializer::class.java)
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

    override fun write(kryo: Kryo, output: Output, data: BulkLinkedDataLambdas) {
        writeUUID(output, data.entityTypeId)

        try {
            output.writeInt(data.entitiesByLinkingId.size)
            data.entitiesByLinkingId.forEach {
                writeUUID(output, it.key)

                output.writeInt(it.value.size)
                it.value.forEach { entry ->
                    writeUUID(output, entry.key)
                    val bytes = mapper.writeValueAsBytes(entry.value)
                    output.writeInt(bytes.size)
                    output.writeBytes(bytes)
                }
            }
        } catch (e: JsonProcessingException) {
            logger.debug("Unable to serialize linking entities with linking ids: {}", data.entitiesByLinkingId.keys)
        }
    }


    override fun read(kryo: Kryo, input: Input, type: Class<BulkLinkedDataLambdas>): BulkLinkedDataLambdas {
        val entityTypeId = readUUID(input)

        val linkingIdsSize = input.readInt()
        val entitiesByLinkingId = HashMap<UUID, Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>>(linkingIdsSize)
        for (i in 1..linkingIdsSize) {
            val linkingId = readUUID(input)

            val entitySetsSize = input.readInt()
            val entitiesByEntitySetId = HashMap<UUID, Map<UUID, Map<UUID, Set<Any>>>>(entitySetsSize)
            for (j in 1..entitySetsSize) {
                val entitySetId = readUUID(input)

                val originIdsSize = input.readInt()
                val entitiesByOriginId = HashMap<UUID, Map<UUID, Set<Any>>>(originIdsSize)
                for (k in 1..originIdsSize) {
                    val originId = readUUID(input)

                    val numBytes = input.readInt()
                    try {
                        val entityData = mapper.readValue<Map<UUID, Set<Any>>>(input.readBytes(numBytes))
                        entitiesByOriginId[originId] = entityData
                    } catch (e: IOException) {
                        logger.debug("Unable to deserialize entities for linking id: {}", linkingId)
                    }
                }
                entitiesByEntitySetId[entitySetId] = entitiesByOriginId
            }
            entitiesByLinkingId[linkingId] = entitiesByEntitySetId
        }

        return BulkLinkedDataLambdas(entityTypeId, entitiesByLinkingId)
    }
}
