package com.openlattice.datastore.configuration

import com.zaxxer.hikari.HikariDataSource

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class ReadonlyDatasourceSupplier( val readOnlyReplica: HikariDataSource )