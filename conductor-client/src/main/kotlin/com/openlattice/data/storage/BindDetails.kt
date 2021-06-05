package com.openlattice.data.storage

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class BindDetails(
        val nextIndex: Int,
        val bindInfo: LinkedHashSet<SqlBinder>,
        val sql: String
)