package com.openlattice.external

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.openlattice.authorization.*
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseSchema
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organization.OrganizationExternalDatabaseView
import com.openlattice.organizations.JdbcConnection
import com.openlattice.organizations.OrganizationEntitySetsService
import com.openlattice.organizations.external.*
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.security.InvalidParameterException
import java.util.*

/**
 * This class enables database specific plugins to not have to be aware of internal OL semantics.
 *
 */
@Service
class ExternalSqlDatabasesManagementService(
        hazelcastInstance: HazelcastInstance,
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val aclKeyReservations: HazelcastAclKeyReservationService,
        private val authorizationManager: AuthorizationManager,
        private val organizationEntitySetsService: OrganizationEntitySetsService,
        private val dbCredentialService: DbCredentialService,
        private val hds: HikariDataSource
) {
    companion object {
        /**
         * For backwards compatibility with existing atlas tables. Eventually, we will have to decide what to do with
         * tables that have default data source.
         */
        @JvmField
        val DEFAULT_DATA_SOURCE_ID = UUID(0, 0)
        const val EXTERNAL = "EXTERNAL"
        private val logger = LoggerFactory.getLogger(ExternalSqlDatabasesManagementService::class.java)
    }

    //TODO: Switch to a loading cache with a long expiration to avoid holding too many connections open forever.
    private val externalSqlDatabases: IMap<UUID, JdbcConnections> = HazelcastMap.EXTERNAL_SQL_DATABASES.getMap(
            hazelcastInstance
    )
    private val externalSqlDatabaseManagers: MutableMap<UUID, MutableMap<UUID, ExternalSqlDatabaseManager>> = mutableMapOf()
    private val organizationExternalDatabaseColumns = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COLUMN.getMap(
            hazelcastInstance
    )
    private val organizationExternalDatabaseTables = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.getMap(
            hazelcastInstance
    )
    private val organizationExternalDatabaseSchemas = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_SCHEMA.getMap(
            hazelcastInstance
    )
    private val organizationExternalDatabaseViews = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_VIEW.getMap(
            hazelcastInstance
    )


    /**
     * Retrieves the registered jdbc connections for an organization.
     *
     * @param organizationId The organization id for which to retrieve jdbc connections.
     *
     * @return A map of jdbc connections by id
     */
    fun getExternalSqlDatabases(organizationId: UUID): JdbcConnections = externalSqlDatabases.getValue(organizationId)

    /**
     * Retrives all the sql database managers for an organization.
     */
    fun getExternalSqlDatabaseManagers(
            organizationId: UUID
    ): MutableMap<UUID, ExternalSqlDatabaseManager> = getExternalSqlDatabases(organizationId)
            .mapValues { (dataSourceId, _) -> getExternalSqlDatabaseManager(organizationId, dataSourceId) }
            .toMutableMap()

    /**
     * Retrieves the sql database manager for a specific datasource.
     */
    fun getExternalSqlDatabaseManager(organizationId: UUID, dataSourceId: UUID) = externalSqlDatabaseManagers
            .getOrPut(organizationId) { mutableMapOf() }
            .getOrPut(dataSourceId) { connect(getExternalSqlDatabases(organizationId).getValue(dataSourceId)) }

    private fun connect(jdbcConnection: JdbcConnection): ExternalSqlDatabaseManager {
        return when (jdbcConnection.driver) {
            "org.postgresql.Driver" -> PostgresExternalSqlDatabaseManager(jdbcConnection)
            "net.snowflake.client.jdbc.SnowflakeDriver" -> SnowflakeExternalSqlDatabaseManager(jdbcConnection)
            else -> throw InvalidParameterException(
                    "${jdbcConnection.driver} is not yet supported by OpenLattice platform. "
            )
        }
    }

    /**
     * For every organization, we have to scan every single database. Given the regular scanning, we should definitely
     * run it on conductor-- but that suggests that API call
     */

    fun syncExternalDatasources(organizationId: UUID) {
        getExternalSqlDatabaseManagers(organizationId).forEach { (dataSourceId, externalSqlDatabaseManager) ->
            syncExternalDatasource(organizationId, dataSourceId, externalSqlDatabaseManager)
        }
    }

    fun syncExternalDatasource(
            organizationId: UUID,
            dataSourceId: UUID,
            externalSqlDatabaseManager: ExternalSqlDatabaseManager
    ) {
        externalSqlDatabaseManager.getTables().forEach { (_, tableMetadata) ->
            syncTable(organizationId, dataSourceId, tableMetadata)
        }
        externalSqlDatabaseManager.getSchemas().forEach { (_, schemaMetadata) ->
            syncSchema(organizationId, dataSourceId, schemaMetadata)
        }
        externalSqlDatabaseManager.getViews().forEach { (_, viewMetadata) ->
            syncView(organizationId, dataSourceId, viewMetadata)
        }
    }

    fun syncPermissions(permissions: Map<AclKey, Map<Principal, Set<Permission>>>) {
        permissions.forEach { (aclKey, ace) ->
            ace.forEach { (principal, pset) ->
                authorizationManager.setPermission(aclKey, principal, EnumSet.copyOf(pset))
            }
        }
    }

    fun buildSchemaFqn(organizationId: UUID, schemaMetadata: SchemaMetadata): String {
        return "$organizationId.$EXTERNAL.${schemaMetadata.externalId}"
    }

    fun buildViewFqn(organizationId: UUID, viewMetadata: ViewMetadata): String {
        return "$organizationId.$EXTERNAL.${viewMetadata.schema}.${viewMetadata.externalId}"
    }

    fun buildTableFqn(organizationId: UUID, tableMetadata: TableMetadata): String {
        return "$organizationId.$EXTERNAL.${tableMetadata.schema}.${tableMetadata.externalId}"
    }

    fun buildColumnFqn(organizationId: UUID, tableMetadata: TableMetadata, columnMetadata: ColumnMetadata): String {
        return "${buildTableFqn(organizationId, tableMetadata)}.${columnMetadata.externalId}"
    }

    fun syncSchema(
            organizationId: UUID,
            dataSourceId: UUID,
            schemaMetadata: SchemaMetadata
    ): Pair<OrganizationExternalDatabaseSchema, Map<AclKey, Map<Principal, Set<Permission>>>> {
        val schemaFqn = buildSchemaFqn(organizationId, schemaMetadata)
        val schemaId = aclKeyReservations.reserveOrGetId(schemaFqn)
        val schemaAclKey = AclKey(schemaId)

        val schema = OrganizationExternalDatabaseSchema(
                schemaId,
                schemaMetadata.name,
                schemaMetadata.name,
                Optional.empty(),
                organizationId,
                dataSourceId,
                schemaMetadata.externalId
        )
        val schemaPermissions = mapSchemaPrivileges(schemaAclKey, schemaMetadata.privileges)
        //It's safe to do set here since this information is pass-through at this point. This mostly just
        //unnecessary crud to leverage the authorization system.
        organizationExternalDatabaseSchemas.set(schemaId, schema)
        syncPermissions(schemaPermissions)
        return schema to schemaPermissions
    }

    fun syncView(
            organizationId: UUID,
            dataSourceId: UUID,
            viewMetadata: ViewMetadata
    ): Pair<OrganizationExternalDatabaseView, Map<AclKey, Map<Principal, Set<Permission>>>> {
        val viewFqn = buildViewFqn(organizationId, viewMetadata)
        val viewId = aclKeyReservations.reserveOrGetId(viewFqn)
        val viewAclKey = AclKey(viewId)

        val view = OrganizationExternalDatabaseView(
                viewId,
                viewMetadata.name,
                viewMetadata.name,
                Optional.empty(),
                organizationId,
                dataSourceId,
                viewMetadata.externalId
        )

        val viewPermissions = mapViewPrivileges(viewAclKey, viewMetadata.privileges)
        //It's safe to do set here since this information is pass-through at this point. This mostly just
        //unnecessary crud to leverage the authorization system.
        organizationExternalDatabaseViews.set(viewId, view)
        syncPermissions(viewPermissions)
        return view to viewPermissions
    }

    /**
     * This function maps the metadata and creates corresponding securable objects.
     */
    fun syncTable(
            organizationId: UUID,
            dataSourceId: UUID,
            tableMetadata: TableMetadata
    ): ExternalSqlDatabaseTableMapping {
        val tableFqn = buildTableFqn(organizationId, tableMetadata)
        val tableId = aclKeyReservations.reserveOrGetId(tableFqn)
        val tableAclKey = AclKey(tableId)

        val table = OrganizationExternalDatabaseTable(
                tableId,
                tableMetadata.name,
                tableMetadata.name,
                Optional.of(tableMetadata.comment),
                organizationId,
                dataSourceId,
                tableMetadata.externalId,
                tableMetadata.schema
        )

        val tablePermissions = mapTablePrivileges(tableAclKey, tableMetadata.privileges)

        organizationEntitySetsService.addDataset(organizationId, table)

        //It's safe to do set here since this information is pass-through at this point. This mostly just
        //unnecessary crud to leverage the authorization system.
        organizationExternalDatabaseTables.set(tableId, table)
        syncPermissions(tablePermissions)

        val columnMappings = tableMetadata.columns.map {
            mapColumn(organizationId, dataSourceId, tableMetadata, tableId, it)
        }

        return ExternalSqlDatabaseTableMapping(
                table,
                tablePermissions,
                columnMappings
        )

    }

    fun listDataSources(organizationId: UUID): Map<UUID, JdbcConnection> {
        return externalSqlDatabases.getValue(organizationId)
    }

    fun updateDataSource(organizationId: UUID, dataSourceId: UUID, dataSource: JdbcConnection) {
        require(dataSourceId == dataSource.id) { "Path id and object id do not match." }
        externalSqlDatabases.executeOnKey(
                organizationId,
                AddJdbcConnectionsEntryProcessor(
                        JdbcConnections(mutableMapOf(dataSourceId to dataSource))
                )
        )
    }

    fun registerDataSource(organizationId: UUID, dataSource: JdbcConnection): UUID {
        val dataSourceWithReservedId = aclKeyReservations.reserveAnonymousId(dataSource)
        val dataSourceId = dataSourceWithReservedId.id
        externalSqlDatabases.executeOnKey(
                organizationId,
                AddJdbcConnectionsEntryProcessor(JdbcConnections(mutableMapOf(dataSourceId to dataSource)))
        )

        syncExternalDatasource(
                organizationId,
                dataSourceId,
                getExternalSqlDatabaseManager(organizationId, dataSourceId)
        )

        return dataSourceId
    }

    private fun mapColumn(
            organizationId: UUID,
            dataSourceId: UUID,
            tableMetadata: TableMetadata,
            tableId: UUID,
            columnMetadata: ColumnMetadata
    ): ExternalSqlDatabaseColumnMapping {
        val columnFqn = buildColumnFqn(organizationId, tableMetadata, columnMetadata)
        val columnId = aclKeyReservations.reserveOrGetId(columnFqn)
        val columnAclKey = AclKey(tableId, columnId)

        val column = OrganizationExternalDatabaseColumn(
                columnId,
                columnMetadata.name,
                columnMetadata.name,
                Optional.of(""),
                columnMetadata.externalId,
                tableId,
                organizationId,
                dataSourceId,
                columnMetadata.sqlDataType,
                columnMetadata.isPrimaryKey,
                columnMetadata.ordinalPosition
        )
        val columnPrivileges = mapColumnPrivileges(columnAclKey, columnMetadata.privileges)

        //It's safe to do set here since this information is pass-through at this point. This mostly just
        //unnecessary crud to leverage the authorization system.
        organizationExternalDatabaseColumns.set(columnId, column)
        syncPermissions(columnPrivileges)

        return ExternalSqlDatabaseColumnMapping(column, columnPrivileges)
    }

    private fun mapSchemaPrivileges(
            tableAclKey: AclKey,
            privileges: Map<String, Set<SchemaPrivilege>>
    ): Map<AclKey, Map<Principal, Set<Permission>>> {
        //TODO: Actually implement this
        return mapPrivileges(tableAclKey, privileges) { stp ->
            stp.flatMap { tp ->
                when (tp) {
                    SchemaPrivilege.CONNECT -> EnumSet.of(Permission.READ, Permission.WRITE)
                    else -> EnumSet.noneOf(Permission::class.java)
                }
            }.toSet()
        }
    }

    private fun mapViewPrivileges(
            tableAclKey: AclKey,
            privileges: Map<String, Set<TablePrivilege>>
    ): Map<AclKey, Map<Principal, Set<Permission>>> {
        //TODO: Actually implement this
        return mapPrivileges(tableAclKey, privileges) { stp ->
            stp.flatMap { tp ->
                when (tp) {
                    TablePrivilege.ALL -> EnumSet.of(Permission.READ, Permission.WRITE)
                    else -> EnumSet.noneOf(Permission::class.java)
                }
            }.toSet()
        }
    }

    private fun mapTablePrivileges(
            tableAclKey: AclKey,
            privileges: Map<String, Set<TablePrivilege>>
    ): Map<AclKey, Map<Principal, Set<Permission>>> {
        //TODO: Actually implement this
        return mapPrivileges(tableAclKey, privileges) { stp ->
            stp.flatMap { tp ->
                when (tp) {
                    TablePrivilege.ALL -> EnumSet.of(Permission.READ, Permission.WRITE)
                    else -> EnumSet.noneOf(Permission::class.java)
                }
            }.toSet()
        }

    }

    private fun mapColumnPrivileges(
            columnAclKey: AclKey,
            privileges: Map<String, Set<ColumnPrivilege>>
    ): Map<AclKey, Map<Principal, Set<Permission>>> {
        //TODO: Actually implement this
        return mapPrivileges(columnAclKey, privileges) { stp ->
            stp.flatMap { tp ->
                when (tp) {
                    ColumnPrivilege.ALL -> EnumSet.of(Permission.READ, Permission.WRITE)
                    else -> EnumSet.noneOf(Permission::class.java)
                }
            }.toSet()
        }
    }

    private fun <T> mapPrivileges(
            objectAclKey: AclKey,
            privileges: Map<String, Set<T>>, mapper: (Set<T>) -> Set<Permission>
    ): Map<AclKey, Map<Principal, Set<Permission>>> {
        val usernameToAclKeys = dbCredentialService.getSecurablePrincipalAclKeysFromUsernames(privileges.keys)

        return mapOf(objectAclKey to usernameToAclKeys
                .map { (dbUser, userAclKey) ->
                    securePrincipalsManager.getSecurablePrincipal(userAclKey).principal to
                            mapper(privileges.getValue(dbUser))
                }
                .toMap())
    }


}
