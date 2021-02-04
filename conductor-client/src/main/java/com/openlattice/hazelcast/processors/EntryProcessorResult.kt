package com.openlattice.hazelcast.processors

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class EntryProcessorResult(val value: Any?, val modified: Boolean = true )