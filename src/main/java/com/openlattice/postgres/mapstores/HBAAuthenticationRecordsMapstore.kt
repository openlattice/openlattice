package com.openlattice.postgres.mapstores

import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.MapConfig
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.PostgresAuthenticationRecord
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet

open class HBAAuthenticationRecordsMapstore(
        hds: HikariDataSource
) : AbstractBasePostgresMapstore<String, PostgresAuthenticationRecord>
(HazelcastMap.HBA_AUTHENTICATION_RECORDS.name, PostgresTable.HBA_AUTHENTICATION_RECORDS, hds) {

    override fun bind(ps: PreparedStatement, key: String, value: PostgresAuthenticationRecord) {
        var index = bind(ps, key, 1)

        //create
        ps.setString(index++, value.connectionType)
        ps.setString(index++, value.database)
        ps.setString(index++, value.username)
        ps.setString(index++, value.ipAddress)
        ps.setString(index++, value.ipMask)
        ps.setString(index++, value.authenticationMethod)

        //update
        ps.setString(index++, value.connectionType)
        ps.setString(index++, value.database)
        ps.setString(index++, value.username)
        ps.setString(index++, value.ipAddress)
        ps.setString(index++, value.ipMask)
        ps.setString(index++, value.authenticationMethod)
    }

    override fun bind(ps: PreparedStatement, key: String, offset: Int): Int {
        var index = offset
        ps.setObject(index++, key)
        return index
    }

    override fun mapToKey(rs: ResultSet?): String {
        return ResultSetAdapters.user(rs)
    }

    override fun mapToValue(rs: ResultSet?): PostgresAuthenticationRecord {
        return ResultSetAdapters.postgresAuthenticationRecord(rs)
    }

    override fun generateTestKey(): String {
        return TestDataFactory.randomAlphanumeric(10)
    }

    override fun generateTestValue(): PostgresAuthenticationRecord {
        return TestDataFactory.postgresAuthenticationRecord()
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .setInMemoryFormat( InMemoryFormat.OBJECT )
    }
}