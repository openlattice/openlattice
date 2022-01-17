package com.openlattice.data.storage

import com.geekbeast.tasks.HazelcastTaskDependencies
import com.zaxxer.hikari.HikariDataSource

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class PostgresEntitySetSizesTaskDependency( val hikariDataSource: HikariDataSource) : HazelcastTaskDependencies