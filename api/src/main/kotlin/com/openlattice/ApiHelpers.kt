package com.openlattice

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.core.JsonProcessingException
import com.google.common.base.Charsets
import com.google.common.base.Preconditions
import com.openlattice.ApiHelpers.Companion.toUtf8Bytes
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class ApiHelpers {

    companion object {
        private val encoder = Base64.getEncoder()
        private val logger = LoggerFactory.getLogger(ApiHelpers::class.java)

        @JvmStatic
        fun generateDefaultEntityId(keys: Set<UUID>, entityDetails: Map<UUID, Set<Any>>): String {
            val entityId = keys.asSequence().mapNotNull { ekid ->
                entityDetails[ekid]
            }.map { data ->
                data.map { datum ->
                    ByteBuffer.wrap(toBytes(datum))
                }.sorted().joinToString { sorted ->
                    encoder.encodeToString(sorted.array())
                }
            }.joinToString { joined ->
                encoder.encodeToString(toUtf8Bytes(joined))
            }
            Preconditions.checkArgument(entityId.isNotEmpty(), "Entity ids cannot be empty strings")
            return entityId
        }

        @JvmStatic
        fun toUtf8Bytes(s: String): ByteArray {
            return s.toByteArray(Charsets.UTF_8)
        }

        @JvmStatic
        fun dbQuote(s: String): String {
            return "\"$s\""
        }

        @JvmStatic
        fun toBytes(o: Any): ByteArray {
            return if (o is String) {
                toUtf8Bytes(o)
            } else try {
                ObjectMappers.getJsonMapper().writeValueAsBytes(o)
            } catch (e: JsonProcessingException) {
                logger.error("Unable to serialize object for building entity id", e)
                ByteArray(0)
            }
        }
    }
}