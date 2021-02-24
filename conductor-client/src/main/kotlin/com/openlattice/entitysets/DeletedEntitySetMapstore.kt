package com.openlattice.entitysets

import com.geekbeast.rhizome.hazelcast.DelegatedIntList
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Component
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

@Component
class DeletedEntitySetMapstore(hds: HikariDataSource) : AbstractBasePostgresMapstore<UUID, DelegatedIntList>(
        HazelcastMap.DELETED_ENTITY_SETS, PostgresTable.DELETED_ENTITY_SETS, hds
) {

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): DelegatedIntList {
        return DelegatedIntList(listOf(
                TestDataFactory.integer(),
                TestDataFactory.integer(),
                TestDataFactory.integer()
        ))
    }

    override fun bind(ps: PreparedStatement, key: UUID, value: DelegatedIntList) {
        val arr = PostgresArrays.createIntArray(ps.connection, value)

        var index = bind(ps, key, 1)

        // insert
        ps.setArray(index++, arr)

        // update
        ps.setArray(index, arr)
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        ps.setObject(offset, key)
        return offset + 1
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return ResultSetAdapters.id(rs)
    }

    override fun mapToValue(rs: ResultSet): DelegatedIntList {
        return DelegatedIntList(ResultSetAdapters.partitions(rs).toList())
    }
}