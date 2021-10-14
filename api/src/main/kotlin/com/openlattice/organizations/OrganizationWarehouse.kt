package com.openlattice.organizations

import java.util.UUID

/**
 * @author Andrew Carter andrew@openlattice.com
 */

data class OrganizationWarehouse(
    val organizationWarehouseId: UUID = UUID.randomUUID() ,
    val organizationId: UUID,
    val warehouseKey: UUID,
    val name: String
)