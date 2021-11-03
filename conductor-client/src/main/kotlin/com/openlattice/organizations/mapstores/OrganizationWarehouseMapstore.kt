package com.openlattice.organizations.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MapConfig
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organizations.OrganizationWarehouse
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.openlattice.postgres.PostgresTable.ORGANIZATION_WAREHOUSES
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.ResultSetAdapters
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Component
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

/**
 * @author Andrew Carter andrew@openlattice.com
 */

@Component
class OrganizationWarehouseMapstore(
    hds: HikariDataSource
) : AbstractBasePostgresMapstore<UUID, OrganizationWarehouse>(
    HazelcastMap.ORGANIZATION_WAREHOUSES, ORGANIZATION_WAREHOUSES, hds
){
    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): OrganizationWarehouse {
        return TestDataFactory.organizationWarehouse()
    }

    override fun bind(ps: PreparedStatement, key: UUID, value: OrganizationWarehouse) {
        var index = 1

        // insert
        ps.setObject(index++, value.organizationWarehouseId)
        ps.setObject(index++, value.organizationId)
        ps.setObject(index++, value.warehouseKey)
        ps.setString(index++, value.name)

        // update
        ps.setObject(index++, value.organizationId)
        ps.setObject(index++, value.warehouseKey)
        ps.setString(index, value.name)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        var index = offset
        ps.setObject(index++, key)
        return index
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return ResultSetAdapters.id(rs)
    }

    override fun mapToValue(rs: ResultSet): OrganizationWarehouse {
        return ResultSetAdapters.organizationWarehouse(rs)
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
            .setInMemoryFormat(InMemoryFormat.OBJECT)
    }
}