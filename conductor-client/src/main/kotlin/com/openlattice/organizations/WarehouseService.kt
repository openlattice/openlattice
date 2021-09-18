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
        hazelcast: HazelcastInstance,
        private val aclKeyReservationService: HazelcastAclKeyReservationService,
        private val authorizationManager: AuthorizationManager
) {
    private val warehouses = HazelcastMap.WAREHOUSES.getMap(hazelcast)

    fun getWarehouses(ids: Set<UUID>): Map<UUID, JdbcConnectionParameters> {
        return warehouses.getAll(ids)
    }

    fun createWarehouse(jdbc: JdbcConnectionParameters, ownerPrincipal: Principal): UUID {
        ensureValidWarehouse(jdbc)

        val aclKey = reserveWarehouseIfNotExists(jdbc)

        authorizationManager.setSecurableObjectType(aclKey, SecurableObjectType.JdbcConnectionParameters)
        authorizationManager.addPermission(aclKey, ownerPrincipal, EnumSet.allOf(Permission::class.java))

        warehouses.set(jdbc.id, jdbc)

        return jdbc.id
    }

    private fun reserveWarehouseIfNotExists(jdbc: JdbcConnectionParameters): AclKey {
        aclKeyReservationService.reserveIdAndValidateType(jdbc) { jdbc.title }

        Preconditions.checkState(warehouses.putIfAbsent(jdbc.id, jdbc) == null, "Warehouse already exists.")
        return AclKey(jdbc.id)
    }

    private fun ensureValidWarehouse(jdbc: JdbcConnectionParameters): Boolean {
        //TODO
        return true
    }

}