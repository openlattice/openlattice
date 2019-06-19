package com.openlattice.organizations.mapstores

import com.geekbeast.rhizome.hazelcast.DelegatedIntList
import com.google.common.collect.ImmutableList
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.ID
import com.openlattice.postgres.PostgresColumn.PARTITIONS
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresTable.ORGANIZATIONS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class OrganizationDefaultPartitionsMapstore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<UUID, DelegatedIntList>(
        HazelcastMap.ORGANIZATION_DEFAULT_PARTITIONS.name,
        ORGANIZATIONS,
        hds
) {


    public override fun initValueColumns(): List<PostgresColumnDefinition> {
        return ImmutableList.of(PARTITIONS)
    }

    @Throws(SQLException::class)
    public override fun bind(ps: PreparedStatement, key: UUID, value: DelegatedIntList) {
        bind(ps, key, 1)
        val valueArr = PostgresArrays.createIntArray(ps.connection, value.toTypedArray())
        ps.setArray(2, valueArr)

        // UPDATE
        ps.setArray(3, valueArr)
    }

    @Throws(SQLException::class)
    public override fun bind(ps: PreparedStatement, key: UUID, parameterIndex: Int): Int {
        ps.setObject(parameterIndex, key)
        return parameterIndex + 1
    }

    @Throws(SQLException::class)
    public override fun mapToValue(rs: ResultSet): DelegatedIntList {
        return DelegatedIntList(ResultSetAdapters.partitions(rs).toList())
    }

    @Throws(SQLException::class)
    public override fun mapToKey(rs: ResultSet): UUID {
        return ResultSetAdapters.id(rs)
    }

    override fun getInsertColumns(): List<PostgresColumnDefinition> {
        return ImmutableList.of(ID, PARTITIONS)
    }

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): DelegatedIntList {
        return DelegatedIntList(listOf(1, 2, 3))
    }
}
