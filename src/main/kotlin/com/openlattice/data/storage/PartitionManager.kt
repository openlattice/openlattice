package com.openlattice.data.storage

import com.openlattice.postgres.PostgresMaterializedViews
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PartitionManager(val hds:HikariDataSource) {
    fun assignPartitions( organizationId: UUID ) {

    }

    fun setDesiredPartitionCount( organizationId: UUID, partitionCount : Int ) {

    }

    fun repartition( organizationId: UUID) {

    }

    fun createMaterializedViewIfNotExists( ) {
        hds.connection.use { conn ->
            conn.createStatement().use {stmt ->
                stmt.execute(PostgresMaterializedViews.PARTITION_COUNTS.createSql)
            }
        }
    }

    fun getEmptiestPartitions( desiredPartitions : Int ) {
        
    }
}

