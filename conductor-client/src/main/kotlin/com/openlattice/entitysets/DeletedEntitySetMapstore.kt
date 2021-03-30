package com.openlattice.entitysets

import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.openlattice.rhizome.DelegatedIntSet
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

open class DeletedEntitySetMapstore(hds: HikariDataSource) : AbstractBasePostgresMapstore<UUID, DelegatedIntSet>(
        HazelcastMap.DELETED_ENTITY_SETS, PostgresTable.DELETED_ENTITY_SETS, hds
) {

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): DelegatedIntSet {
        return DelegatedIntSet(setOf(0, 1, 2))
    }

    override fun bind(ps: PreparedStatement, key: UUID, value: DelegatedIntSet) {
        var index = bind(ps, key, 1)

        val partitionsArr = PostgresArrays.createIntArray(ps.connection, value)

        // create
        ps.setArray(index++, partitionsArr)

        // update
        ps.setArray(index, partitionsArr)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        ps.setObject(offset, key)
        return offset + 1
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return ResultSetAdapters.id(rs)
    }

    override fun mapToValue(rs: ResultSet): DelegatedIntSet {
        return DelegatedIntSet(ResultSetAdapters.partitions(rs).toSet())
    }
}