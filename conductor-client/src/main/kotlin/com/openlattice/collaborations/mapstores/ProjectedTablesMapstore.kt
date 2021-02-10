package com.openlattice.collaborations.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.IndexConfig
import com.hazelcast.config.IndexType
import com.hazelcast.config.MapConfig
import com.openlattice.collaborations.ProjectedTableKey
import com.openlattice.collaborations.ProjectedTableMetadata
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.PostgresTable.PROJECTED_TABLES
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet

open class ProjectedTablesMapstore(val hds: HikariDataSource) : AbstractBasePostgresMapstore<ProjectedTableKey, ProjectedTableMetadata>(
        HazelcastMap.PROJECTED_TABLES, PROJECTED_TABLES, hds
) {

    companion object {
        const val TABLE_ID_INDEX = "__key.tableId"
        const val COLLABORATION_ID_INDEX = "__key.collaborationId"
        const val ORGANIZATION_ID_INDEX = "organizationId"
        const val TABLE_NAME_INDEX = "tableName"
    }

    override fun generateTestKey(): ProjectedTableKey {
        return TestDataFactory.projectedTableKey()
    }

    override fun generateTestValue(): ProjectedTableMetadata {
        return TestDataFactory.projectedTableMetadata()
    }

    override fun bind(ps: PreparedStatement, key: ProjectedTableKey, value: ProjectedTableMetadata) {
        var index = bind(ps, key, 1)

        // insert
        ps.setObject(index++, value.organizationId)
        ps.setString(index++, value.tableName)

        // update
        ps.setObject(index++, value.organizationId)
        ps.setString(index++, value.tableName)
    }

    override fun bind(ps: PreparedStatement, key: ProjectedTableKey, offset: Int): Int {
        var index = offset
        ps.setObject(index++, key.tableId)
        ps.setObject(index++, key.collaborationId)
        return index
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addIndexConfig(IndexConfig(IndexType.HASH, TABLE_ID_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH, COLLABORATION_ID_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH, ORGANIZATION_ID_INDEX))
                .addIndexConfig(IndexConfig(IndexType.HASH, TABLE_NAME_INDEX))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    override fun mapToKey(rs: ResultSet): ProjectedTableKey {
        return ResultSetAdapters.projectedTableKey(rs)
    }

    override fun mapToValue(rs: ResultSet): ProjectedTableMetadata {
        return ResultSetAdapters.projectedTableMetadata(rs)
    }
}