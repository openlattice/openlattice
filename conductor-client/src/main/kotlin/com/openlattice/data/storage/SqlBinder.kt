package com.openlattice.data.storage

import com.openlattice.analysis.SqlBindInfo
import java.sql.PreparedStatement

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class SqlBinder(val bindInfo: SqlBindInfo, val binding: (PreparedStatement, SqlBindInfo) -> Unit) {
    fun bind(ps:PreparedStatement) {
        binding(ps, bindInfo)
    }
}