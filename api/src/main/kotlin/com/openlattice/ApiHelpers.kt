package com.openlattice

import com.google.common.base.Preconditions
import java.nio.ByteBuffer
import java.util.*

/**
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class ApiHelpers {

    companion object {
        private val encoder = Base64.getEncoder()

        @JvmStatic
        fun generateDefaultEntityId(keys: Set<UUID>, entityDetails: Map<UUID, Set<Any>>): String {
            val entityId = keys.asSequence().mapNotNull { ekid ->
                entityDetails[ekid]
            }.map { data ->
                data.map { datum ->
                    ByteBuffer.wrap(ApiUtil.toBytes(datum))
                }.sorted().joinToString { sorted ->
                    encoder.encodeToString(sorted.array())
                }
            }.joinToString { joined ->
                encoder.encodeToString(ApiUtil.toUtf8Bytes(joined))
            }
            Preconditions.checkArgument(entityId.isNotEmpty(), "Entity ids cannot be empty strings")
            return entityId
        }

        @JvmStatic
        fun dbQuote(s: String): String? {
            return "\"$s\""
        }

    }
}