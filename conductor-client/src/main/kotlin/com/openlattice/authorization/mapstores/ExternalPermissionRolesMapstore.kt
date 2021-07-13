package com.openlattice.authorization.mapstores

import com.openlattice.authorization.AccessTarget
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Component
import org.apache.commons.lang3.RandomStringUtils
import java.sql.PreparedStatement
import java.sql.ResultSet

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
@Component
class ExternalPermissionRolesMapstore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<AccessTarget, String>(
        HazelcastMap.EXTERNAL_PERMISSION_ROLES, PostgresTable.EXTERNAL_PERMISSION_ROLES, hds
) {
    override fun generateTestKey(): AccessTarget {
        return AccessTarget(
                TestDataFactory.role().aclKey,
                TestDataFactory.permissions().first()
        )
    }

    override fun generateTestValue(): String {
        return RandomStringUtils.randomAlphabetic(15)
    }

    override fun bind(ps: PreparedStatement, key: AccessTarget, value: String) {
        var index = bind(ps, key, 1)
        ps.setString(index++, value)

        ps.setString(index++, value)
    }

    override fun mapToKey(rs: ResultSet): AccessTarget {
        return ResultSetAdapters.accessTarget(rs)
    }

    override fun bind(ps: PreparedStatement, key: AccessTarget, offset: Int): Int {
        var index = offset
        ps.setArray(index++, PostgresArrays.createUuidArray(ps.connection, key.aclKey))
        ps.setString(index++, key.permission.name)
        return index
    }

    override fun mapToValue(rs: ResultSet): String {
        return ResultSetAdapters.roleId(rs)
    }
}