package com.openlattice.external

import com.openlattice.organizations.JdbcConnection
import java.util.*

/**
 * Hazelcast wrapper for a collection of JdbcConnections.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class JdbcConnections(
        private val jdbcConnections: MutableMap<UUID,JdbcConnection> = mutableMapOf()
) : MutableMap<UUID, JdbcConnection> by jdbcConnections