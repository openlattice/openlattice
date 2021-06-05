package com.openlattice.hazelcast.mapstores.shuttle

import com.hazelcast.config.*
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.InternalTestDataFactory
import com.openlattice.postgres.PostgresTable.INTEGRATION_JOBS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.ResultSetAdapters.id
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.openlattice.shuttle.IntegrationJob
import com.zaxxer.hikari.HikariDataSource
import org.springframework.stereotype.Component
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

const val INTEGRATION_STATUS = "integrationStatus"

@Component
class IntegrationJobsMapstore(hds: HikariDataSource) : AbstractBasePostgresMapstore<UUID, IntegrationJob>(
        HazelcastMap.INTEGRATION_JOBS.name, INTEGRATION_JOBS, hds
) {

    override fun bind(ps: PreparedStatement, key: UUID, value: IntegrationJob) {
        var index = bind(ps, key, 1)

        //create
        ps.setString(index++, value.integrationName)
        ps.setString(index++, value.integrationStatus.toString())

        //update
        ps.setString(index++, value.integrationName)
        ps.setString(index++, value.integrationStatus.toString())
    }

    override fun bind(ps: PreparedStatement, key: UUID, offset: Int): Int {
        var index = offset
        ps.setObject(index++, key)
        return index
    }

    override fun generateTestKey(): UUID {
        return UUID.randomUUID()
    }

    override fun generateTestValue(): IntegrationJob {
        return InternalTestDataFactory.integrationJob()
    }

    override fun mapToKey(rs: ResultSet): UUID {
        return id(rs)
    }

    override fun mapToValue(rs: ResultSet): IntegrationJob {
        return ResultSetAdapters.integrationJob(rs)
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addIndexConfig(IndexConfig(IndexType.HASH,INTEGRATION_STATUS))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }
}