package com.openlattice.organizations

import com.google.common.base.Preconditions
import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.JdbcConnectionParameters
import org.springframework.stereotype.Service
import java.util.UUID

/**
 * @author Andrew Carter andrew@openlattice.com
 */

@Service
class WarehousesService(
    hazelcastInstance: HazelcastInstance,
    private val authorizationManager: AuthorizationManager,
    private val aclKeyReservationService: HazelcastAclKeyReservationService
) {

    private val warehouses = HazelcastMap.WAREHOUSES.getMap(hazelcastInstance)

    fun getWarehouses(): Iterable<JdbcConnectionParameters> {
        return warehouses.values
    }

    fun getWarehouse(id: UUID): JdbcConnectionParameters {
        return warehouses.getValue(id)
    }

    fun createWarehouse(jdbc: JdbcConnectionParameters): UUID {
        val aclKey = reserveWarehouseIfNotExists(jdbc)
        authorizationManager.setSecurableObjectType(aclKey, SecurableObjectType.JdbcConnectionParameters)
        return jdbc._id
    }

    fun deleteWarehouse(id: UUID) {
        ensureValidWarehouseId(id)
        warehouses.delete(id)
    }

    fun updateWarehouse(jdbc: JdbcConnectionParameters) {
        ensureValidWarehouseId(jdbc._id)
        warehouses.replace(jdbc._id, jdbc)
    }

    private fun reserveWarehouseIfNotExists(jdbc: JdbcConnectionParameters): AclKey {
        aclKeyReservationService.reserveIdAndValidateType(jdbc) { jdbc._title }

        Preconditions.checkState(warehouses.putIfAbsent(jdbc.id, jdbc) == null, "Warehouse already exists.")
        return AclKey(jdbc.id)
    }

    private fun ensureValidWarehouseId(id: UUID) {
        check(warehouses.containsKey(id)) {"No collaboration exists with id $id"}
    }
}