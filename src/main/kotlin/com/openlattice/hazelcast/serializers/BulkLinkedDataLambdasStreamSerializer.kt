package com.openlattice.hazelcast.serializers

import com.dataloom.mappers.ObjectMappers
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.HashMultimap
import com.google.common.collect.Maps
import com.google.common.collect.Multimaps
import com.google.common.collect.SetMultimap
import com.openlattice.conductor.rpc.BulkLinkedDataLambdas
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.io.IOException
import java.util.UUID

@Component
class BulkLinkedDataLambdasStreamSerializer(
        private val mapper: ObjectMapper = ObjectMappers.getSmileMapper(),
        private val ref: TypeReference<SetMultimap<UUID, Any>> = object : TypeReference<SetMultimap<UUID, Any>>() {}
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
        writeUUID(output, data.linkingEntitySetId)

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
            logger.debug("Unable to serialize entity with for entity set: {}", data.linkingEntitySetId)
        }
    }


    override fun read(kryo: Kryo, input: Input, type: Class<BulkLinkedDataLambdas>): BulkLinkedDataLambdas {
        val linkedEntitySetId = readUUID(input)

        val linkingIdsSize = input.readInt()
        val entitiesByLinkingId = HashMap<UUID, Map<UUID, Map<UUID, Set<Any>>>>(linkingIdsSize)
        for (i in 1..linkingIdsSize) {
            val linkingId = readUUID(input)

            val entitySetsSize = input.readInt()
            val entitiesByEntitySetId = HashMap<UUID, SetMultimap<UUID, Any>>(entitySetsSize)
            for (j in 1..entitySetsSize) {
                val entitySetId = readUUID(input)

                val numBytes = input.readInt()
                var entityData: HashMultimap<UUID, Any>
                try {
                    entityData = mapper.readValue(input.readBytes(numBytes), ref)
                    entitiesByEntitySetId[entitySetId] = entityData
                } catch (e: IOException) {
                    logger.debug("Unable to deserialize entities for linking id: {}", linkingId)
                }
            }
            entitiesByLinkingId[linkingId] = Maps.transformValues(entitiesByEntitySetId, Multimaps::asMap)
        }

        return BulkLinkedDataLambdas(linkedEntitySetId, entitiesByLinkingId)
    }
}
