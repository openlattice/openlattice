package com.openlattice.authorization.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.IndexConfig
import com.hazelcast.config.IndexType
import com.hazelcast.config.MapConfig
import com.openlattice.authorization.AceKey
import com.openlattice.authorization.AceValue
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Permission
import com.openlattice.authorization.Principal
import com.openlattice.authorization.PrincipalType
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Component

import java.sql.Array
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.UUID

/**
 * temp mapstore for migration
 */

const val SECURABLE_OBJECT_TYPE_INDEX = "securableObjectType"

@Component
class LegacyPermissionMapstore(
    hds: HikariDataSource
) : AbstractBasePostgresMapstore<AceKey, AceValue>(
    HazelcastMap.LEGACY_PERMISSIONS, PostgresTable.LEGACY_PERMISSIONS, hds
) {
    override fun bind(ps: PreparedStatement, key: AceKey, value: AceValue) {
        val permissions = PostgresArrays.createTextArray(
            ps.getConnection(),
            value.getPermissions().stream().map { it -> it.name }
        )
        val expirationDate = value.getExpirationDate()
        val securableObjectType = value.getSecurableObjectType().name

        var index = bind(ps, key, 1)

        //create
        ps.setArray(index++, permissions);
        ps.setObject(index++, expirationDate);
        ps.setString(index++, securableObjectType);

        //update
        ps.setArray(index++, permissions);
        ps.setObject(index++, expirationDate);
        ps.setString(index++, securableObjectType);
    }

    override fun bind(ps: PreparedStatement, key: AceKey, offset: Int): Int {
        var index = offset

        val p = key.getPrincipal()
        ps.setArray(index++, PostgresArrays.createUuidArray(ps.getConnection(), key.getAclKey().stream()))
        ps.setString(index++, p.getType().name);
        ps.setString(index++, p.getId())

        return index
    }

    override fun mapToKey(rs: ResultSet): AceKey {
        return ResultSetAdapters.aceKey(rs);
    }

    override fun mapToValue(rs: ResultSet): AceValue {
        // I am assuming there's no NULL securableObjectTypes
        return AceValue(
            ResultSetAdapters.permissions(rs),
            ResultSetAdapters.securableObjectType(rs),
            ResultSetAdapters.expirationDate(rs)
        )
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
            // .addIndexConfig(IndexConfig(IndexType.HASH, ACL_KEY_INDEX))
            // .addIndexConfig(IndexConfig(IndexType.HASH, PRINCIPAL_INDEX))
            // .addIndexConfig(IndexConfig(IndexType.HASH, PRINCIPAL_TYPE_INDEX))
            // .addIndexConfig(IndexConfig(IndexType.HASH, PERMISSIONS_INDEX))
            // .addIndexConfig(IndexConfig(IndexType.SORTED, EXPIRATION_DATE_INDEX))
            .addIndexConfig(IndexConfig(IndexType.HASH, SECURABLE_OBJECT_TYPE_INDEX))
            .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    override fun generateTestKey(): AceKey {
        return AceKey(
            AclKey(UUID.randomUUID()),
            TestDataFactory.userPrincipal()
        )
    }

    override fun generateTestValue(): AceValue {
        return TestDataFactory.aceValue()
    }
}