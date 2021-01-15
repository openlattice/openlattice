package com.openlattice.external

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.openlattice.authorization.*
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organizations.JdbcConnection
import com.openlattice.organizations.OrganizationExternalDatabaseConfiguration
import com.openlattice.organizations.OrganizationMetadataEntitySetsService
import com.openlattice.organizations.external.*
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.security.InvalidParameterException
import java.util.*
import java.util.concurrent.TimeUnit

/**
 *
 * The requirement is that each individual table and catalog can be permissioned independently. So if we want to use
 * securable objects that means we can't use our flexible data api as tables are merely rows.
 *
 * So challenge is to actually link database metadata ->  entities associated with that data.
 * The only ACL that needs to be directly managed by organizations is the one for row level visibility in metadata
 * catalog,
 *
 * Q: Should an API that can modify the actual SQL table make the call as the user or should it connect to db via
 * persistent connection? A: The data steward shouldn't be required to have a database account with access to information
 * in order to do their job and approve access requests (though frequently they will).
 *
 *
 * This class is intended to be called to impleme
 */
@Service
class ExternalSqlDatabasesManagementService(
        hazelcastInstance: HazelcastInstance,
        private val externalDbManager: ExternalDatabaseConnectionManager,
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val aclKeyReservations: HazelcastAclKeyReservationService,
        private val authorizationManager: AuthorizationManager,
        private val organizationExternalDatabaseConfiguration: OrganizationExternalDatabaseConfiguration,
        private val organizationMetadataEntitySetsService: OrganizationMetadataEntitySetsService,
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
    private val securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(hazelcastInstance)
    private val aces = HazelcastMap.PERMISSIONS.getMap(hazelcastInstance)
    private val logger = LoggerFactory.getLogger(ExternalSqlDatabasesManagementService::class.java)
    private val primaryKeyConstraint = "PRIMARY KEY"
    private val FETCH_SIZE = 100_000

    /**
     * Only needed for materialize entity set, which should move elsewhere eventually
     */
    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcastInstance)
    private val entityTypes = HazelcastMap.ENTITY_TYPES.getMap(hazelcastInstance)
    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(hazelcastInstance)
    private val organizations = HazelcastMap.ORGANIZATIONS.getMap(hazelcastInstance)

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
            "com.snowflake.client.jdbc.SnowflakeDriver" -> SnowflakeExternalSqlDatabaseManager(jdbcConnection)
            else -> throw InvalidParameterException(
                    "${jdbcConnection.driver} is not yet supported by OpenLattice platform. "
            )
        }
    }

    /**
     * For every organization, we have to scan every single database. Given the regular scanning, we should definitely
     * run it on conductor-- but that suggests that API call
     */

    fun syncExternalDatabases(organizationId: UUID) {
        getExternalSqlDatabaseManagers(organizationId).forEach { (dataSourceId, externalSqlDatabaseManager) ->
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
    }


    fun syncTable(organizationId: UUID, dataSourceId: UUID, tableMetadata: TableMetadata) {
        val externalSqlDatabaseTableMapping = mapTable(organizationId, dataSourceId, tableMetadata)

        organizationExternalDatabaseTables.set(
                externalSqlDatabaseTableMapping.organizationExternalDatabaseTable.id,
                externalSqlDatabaseTableMapping.organizationExternalDatabaseTable
        )

        organizationMetadataEntitySetsService.addDataset(
                organizationId,
                externalSqlDatabaseTableMapping.organizationExternalDatabaseTable
        )

        syncPermissions(externalSqlDatabaseTableMapping.tablePermissions)

        externalSqlDatabaseTableMapping.columnMappings.forEach { (_, columnPermissions) ->
            syncPermissions(columnPermissions)
        }
    }

    fun syncSchema(organizationId: UUID, dataSourceId: UUID, schemaMetadata: SchemaMetadata) {

    }

    fun syncView(organizationId: UUID, dataSourceId: UUID, schemaMetadata: ViewMetadata) {

    }

    fun syncPermissions(permissions: Map<AclKey, Map<Principal, Set<Permission>>>) {
        permissions.forEach { (aclKey, ace) ->
            ace.forEach { (principal, pset) ->
                authorizationManager.setPermission(aclKey, principal, EnumSet.copyOf(pset))
            }
        }
    }

    fun buildTableFqn(organizationId: UUID, tableMetadata: TableMetadata): String {
        return "$organizationId.${tableMetadata.schema}.${tableMetadata.externalId}"
    }

    fun buildColumnFqn(organizationId: UUID, tableMetadata: TableMetadata, columnMetadata: ColumnMetadata): String {
        return "${buildTableFqn(organizationId, tableMetadata)}.${columnMetadata.externalId}"
    }

    /**
     * This function maps the metadata and creates corresponding securable objects.
     */
    private fun mapTable(
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
                tableMetadata.externalId
        )

        val columnMappings = tableMetadata.columns.map {
            mapColumn(organizationId, dataSourceId, tableMetadata, tableId, it)
        }

        return ExternalSqlDatabaseTableMapping(
                table,
                mapTablePrivileges(tableAclKey, tableMetadata.privileges),
                columnMappings
        )

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

        //It's safe to do set here since this information is pass-through at this point. This mostly just
        //unnecessary crud to leverage the authorization system.
        organizationExternalDatabaseColumns.set(columnId, column)

        return ExternalSqlDatabaseColumnMapping(column, mapColumnPrivileges(columnAclKey, columnMetadata.privileges))
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
