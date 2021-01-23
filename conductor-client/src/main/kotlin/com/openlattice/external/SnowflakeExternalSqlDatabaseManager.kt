package com.openlattice.external

import com.google.common.base.Suppliers
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.JdbcConnection
import com.openlattice.organizations.external.*
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.security.InvalidParameterException
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class SnowflakeExternalSqlDatabaseManager(private val jdbcConnection: JdbcConnection) : ExternalSqlDatabaseManager {
    override fun getDriverName(): String = "net.snowflake.client.jdbc.SnowflakeDriver"

    private val hdsSupplier = Suppliers.memoizeWithExpiration(
            { connect(jdbcConnection) },
            5,
            TimeUnit.MINUTES
    )

    private val hds: HikariDataSource
        get() = hdsSupplier.get()

    private fun connect(jdbcConnection: JdbcConnection): HikariDataSource {
        val config = HikariConfig()

        config.driverClassName = getDriverName()
        config.dataSourceProperties = jdbcConnection.properties
        config.jdbcUrl = jdbcConnection.url
        config.username = jdbcConnection.username
        config.password = jdbcConnection.password

        return HikariDataSource(config)
    }

    override fun getTables(): Map<TableKey, TableMetadata> {
        val tables = BasePostgresIterable(PreparedStatementHolderSupplier(hds, SELECT_TABLES) {}) { rs ->
            readTableMetadata(rs)
        }.toMap()

        getColumns().forEach { (tableKey, columns) ->
            tables.getValue(tableKey).columns.addAll(columns)
        }

        return tables
    }

    private fun getColumns(): Map<TableKey, List<ColumnMetadata>> {
        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, SELECT_COLUMNS) {}) { rs ->
            readColumnMetadata(rs)
        }.groupBy { TableKey(it.name, it.schema, it.externalId) }
    }

    private fun readColumnMetadata(rs: ResultSet): ColumnMetadata {
        return ColumnMetadata(
                name = rs.getString("TABLE_NAME"),
                schema = rs.getString("TABLE_SCHEMA"),
                sqlDataType = rs.getString("DATA_TYPE"),
                ordinalPosition = rs.getInt("ORDINAL_POSITION"),
                isPrimaryKey = false, // rs.getBoolean("IS_PRIMARY_KEY"), //SNOWFLAKE DOES NOT EXPOSE THIS YET https://community.snowflake.com/s/question/0D50Z00007XY2UMSA1/how-to-get-list-of-my-primary-key
                isNullable = when(val nullable = rs.getString("IS_NULLABLE")) {
                    "YES" -> true
                    "NO" -> false
                    else -> throw InvalidParameterException("Unrecognized IS_NULLABLE column value: $nullable")
                },
                privileges = mutableMapOf(),
                maskingPolicy = ""//Will have to pull later with SHOW MASKING POLICY;
        )
    }

    private fun readTableMetadata(rs: ResultSet): Pair<TableKey, TableMetadata> {
        val tableMetadata = TableMetadata(
                name = rs.getString("TABLE_NAME"),
                schema = rs.getString("TABLE_SCHEMA"),
                comment = rs.getString("COMMENT") ?: "",
                privileges = mutableMapOf(),
                columns = mutableListOf(),
                lastUpdated = OffsetDateTime.now()
        )
        return tableMetadata.tableKey to tableMetadata
    }


    override fun getSchemas(): Map<String, SchemaMetadata> {
        return mutableMapOf()
    }

    override fun getViews(): Map<String, ViewMetadata> {
        return mutableMapOf()
    }

    override fun grantPrivilegeOnSchemaToRole(privilege: SchemaPrivilege, role: Role) {
        TODO("Not yet implemented")
    }

    override fun grantPrivilegeOnTableToRole(privilege: TablePrivilege, role: Role) {
        TODO("Not yet implemented")
    }

    override fun grantPrivilegeOnViewToRole(privilege: TablePrivilege, role: Role) {
        TODO("Not yet implemented")
    }

    override fun grantPrivilegeOnDatabaseToRole(privilege: DatabasePrivilege, role: Role) {
        TODO("Not yet implemented")
    }

    override fun grantRoleToRole(roleToGrant: Role, target: Role) {
        TODO("Not yet implemented")
    }

    override fun createSchema(schema: SchemaMetadata, owner: Role) {
        TODO("Not yet implemented")
    }

    override fun createTable(table: TableMetadata, owner: Role) {
        TODO("Not yet implemented")
    }

    override fun createView(view: ViewMetadata, owner: Role) {
        TODO("Not yet implemented")
    }

    override fun isDataMaskingNativelySupported(): Boolean {
        TODO("Not yet implemented")
    }

    override fun createDataMaskingPolicy(maskingPolicy: String, schema: String) {
        TODO("Not yet implemented")
    }

    override fun applyDataMaskingPolicy(table: TableMetadata) {
        TODO("Not yet implemented")
    }

    override fun isRoleManagementEnabled(): Boolean {
        TODO("Not yet implemented")
    }

    override fun createUser(user: SecurablePrincipal) {
        TODO("Not yet implemented")
    }

    override fun createRole(role: SecurablePrincipal) {
        TODO("Not yet implemented")
    }

    override fun deleteUser(user: SecurablePrincipal) {
        TODO("Not yet implemented")
    }

    override fun deleteRole(role: SecurablePrincipal) {
        TODO("Not yet implemented")
    }

    companion object {
        private const val SELECT_TABLES = "SELECT * from information_schema.tables where table_schema != 'INFORMATION_SCHEMA'"
        private const val SELECT_COLUMNS = "SELECT * from information_schema.columns where table_schema != 'INFORMATION_SCHEMA' ORDER BY (TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION)"
    }
}

