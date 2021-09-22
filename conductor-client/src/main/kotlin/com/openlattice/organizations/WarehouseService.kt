package com.openlattice.organizations

import com.google.common.base.Preconditions
import com.hazelcast.core.HazelcastInstance
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.JdbcConnectionParameters
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import java.util.*

/**
 * @author Andrew Carter andrew@openlattice.com
 */

class WarehouseService(
        hazelcast: HazelcastInstance
) {
    private val warehouses = HazelcastMap.WAREHOUSES.getMap(hazelcast)

    fun getWarehouses(): Iterable<JdbcConnectionParameters> {
        return warehouses.values
    }

    fun getWarehouse(id: UUID): JdbcConnectionParameters {
        return warehouses.getValue(id)
    }

    fun createWarehouse(jdbc: JdbcConnectionParameters): UUID {
        warehouses.set(jdbc.id, jdbc)
        return jdbc.id
    }

    fun deleteWarehouse(id: UUID) {
        ensureValidWarehouseId(id)
        warehouses.delete(id)
    }

    fun updateWarehouse(jdbc: JdbcConnectionParameters) {
        ensureValidWarehouseId(jdbc.id)
        warehouses.set(jdbc.id, jdbc)
    }

    private fun ensureValidWarehouseId(id: UUID) {
        check(warehouses.containsKey(id)) {"No collaboration exists with id $id"}
    }
}