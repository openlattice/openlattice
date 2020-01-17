package com.openlattice.shuttle.payload

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface Payload {
    fun getPayload(): Iterable<Map<String, Any?>>
}
