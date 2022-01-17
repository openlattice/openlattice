package com.openlattice.datasets

import com.geekbeast.mappers.mappers.ObjectMappers
import com.hazelcast.config.InMemoryFormat
import com.hazelcast.config.IndexConfig
import com.hazelcast.config.IndexType
import com.hazelcast.config.MapConfig
import com.openlattice.authorization.AclKey
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet

open class ObjectMetadataMapstore(
        val hds: HikariDataSource
) : AbstractBasePostgresMapstore<AclKey, SecurableObjectMetadata>(
        HazelcastMap.OBJECT_METADATA, PostgresTable.OBJECT_METADATA, hds
) {

    companion object {
        private val mapper = ObjectMappers.getJsonMapper()

        const val ROOT_OBJECT_INDEX = "__key.root";
    }

    override fun generateTestKey(): AclKey {
        return TestDataFactory.aclKey()
    }

    override fun generateTestValue(): SecurableObjectMetadata {
        return TestDataFactory.securableObjectMetadata()
    }

    override fun bind(ps: PreparedStatement, key: AclKey, value: SecurableObjectMetadata) {
        var offset = bind(ps, key, 1)

        val metadataAsString = mapper.writeValueAsString(value)

        // insert
        ps.setObject(offset++, metadataAsString)

        // update
        ps.setObject(offset, metadataAsString)
    }

    override fun bind(ps: PreparedStatement, key: AclKey, offset: Int): Int {
        ps.setArray(offset, PostgresArrays.createUuidArray(ps.connection, key))
        return offset + 1
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
                .addIndexConfig(IndexConfig(IndexType.HASH, ROOT_OBJECT_INDEX))
                .setInMemoryFormat(InMemoryFormat.OBJECT)
    }

    override fun mapToKey(rs: ResultSet): AclKey {
        return ResultSetAdapters.aclKey(rs)
    }

    override fun mapToValue(rs: ResultSet): SecurableObjectMetadata {
        return ResultSetAdapters.securableObjectMetadata(rs)
    }


}