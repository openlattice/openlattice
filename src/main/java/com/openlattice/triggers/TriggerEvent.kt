package com.openlattice.triggers

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class TriggerEvent(
        val nodes: Set<String>,
        val name: String
)