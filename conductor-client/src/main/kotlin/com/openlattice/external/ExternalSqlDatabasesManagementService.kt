package com.openlattice.external

import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.assembler.AssemblerConnectionManager.Companion.OPENLATTICE_SCHEMA
import com.openlattice.assembler.AssemblerConnectionManager.Companion.STAGING_SCHEMA
import com.openlattice.assembler.PostgresRoles.Companion.getSecurablePrincipalIdFromUserName
import com.openlattice.assembler.PostgresRoles.Companion.isPostgresUserName
import com.openlattice.assembler.dropAllConnectionsToDatabaseSql
import com.openlattice.authorization.*
import com.openlattice.authorization.processors.PermissionMerger
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.edm.processors.GetEntityTypeFromEntitySetEntryProcessor
import com.openlattice.edm.processors.GetFqnFromPropertyTypeEntryProcessor
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.hazelcast.processors.organizations.UpdateOrganizationExternalDatabaseColumnEntryProcessor
import com.openlattice.hazelcast.processors.organizations.UpdateOrganizationExternalDatabaseTableEntryProcessor
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organization.OrganizationExternalDatabaseTableColumnsPair
import com.openlattice.organizations.JdbcConnection
import com.openlattice.organizations.OrganizationExternalDatabaseConfiguration
import com.openlattice.organizations.external.ColumnMetadata
import com.openlattice.organizations.external.ColumnPrivilege
import com.openlattice.organizations.external.TableMetadata
import com.openlattice.organizations.external.TablePrivilege
import com.openlattice.organizations.mapstores.ORGANIZATION_ID_INDEX
import com.openlattice.organizations.mapstores.TABLE_ID_INDEX
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.quote
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.ResultSetAdapters.*
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.postgres.streams.StatementHolderSupplier
import com.openlattice.transporter.processors.DestroyTransportedEntitySetEntryProcessor
import com.openlattice.transporter.processors.GetPropertyTypesFromTransporterColumnSetEntryProcessor
import com.openlattice.transporter.processors.TransportEntitySetEntryProcessor
import com.openlattice.transporter.types.TransporterDatastore
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.io.BufferedOutputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.security.InvalidParameterException
import java.time.OffsetDateTime
import java.util.*
import kotlin.collections.HashMap
import kotlin.streams.toList

/**
 * This class is intended to be run inside of conductor. W
 */
@Service
class ExternalSqlDatabasesManagementService(
        hazelcastInstance: HazelcastInstance,
        private val externalSqlDatabases: IMap<UUID, JdbcConnections>,
        private val externalDbManager: ExternalDatabaseConnectionManager,
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val aclKeyReservations: HazelcastAclKeyReservationService,
        private val authorizationManager: AuthorizationManager,
        private val organizationExternalDatabaseConfiguration: OrganizationExternalDatabaseConfiguration,
        private val transporterDatastore: TransporterDatastore,
        private val dbCredentialService: DbCredentialService,
        private val hds: HikariDataSource
) {

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
    fun getExternalSqlDatabaseManager(organizationId: UUID, dataSourceId: UUID) = externalSqlDatabaseManagers
            .getValue(organizationId)
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

    fun syncTable(organizationId: UUID, tableMetadata: TableMetadata) {

        val externalSqlDatabaseTableMapping = fromMetadata(tableMetadata)

        aclKeyReservations.reserveIdAndValidateType(externalSqlDatabaseTableMapping.)
    }

    fun buildTableFqn(organizationId: UUID, tableMetadata: TableMetadata): String {
        return "$organizationId.${tableMetadata.schema}.${tableMetadata.externalId}"
    }

    fun buildColumnFqn(organizationId: UUID, tableMetadata: TableMetadata, columnMetadata: ColumnMetadata): String {
        return "${buildTableFqn(organizationId, tableMetadata)}.${columnMetadata.externalId}"
    }

    private fun fromMetadata(organizationId: UUID, tableMetadata: TableMetadata): ExternalSqlDatabaseTableMapping {
        val tableFqn = buildTableFqn(organizationId, tableMetadata)
        val tableId = aclKeyReservations.reserveOrGetId(tableFqn)
        val table = OrganizationExternalDatabaseTable(
                tableId,
                tableMetadata.name,
                tableMetadata.name,
                Optional.of(tableMetadata.comment),
                organizationId,
                tableMetadata.externalId
        )

        val columnMappings = tableMetadata.columns.associate {
            fromMetadata(organizationId, tableMetadata,tableId, it) to mapColumnPrivileges(it.privileges)
        }
        return ExternalSqlDatabaseTableMapping(
                table,
                mapTablePrivileges(tableMetadata.privileges),
                columnMappings
        )

    }

    private fun mapTablePrivileges(privileges: Map<String, Set<TablePrivilege>>): Map<AclKey, Set<Permission>> {
        //TODO: Actually implement this
        return mapPrivileges(privileges) { stp ->
            stp.flatMap { tp ->
                when (tp) {
                    TablePrivilege.ALL -> EnumSet.of(Permission.READ, Permission.WRITE)
                    else -> EnumSet.noneOf(Permission::class.java)
                }
            }.toSet()
        }

    }

    private fun mapColumnPrivileges(privileges: Map<String, Set<ColumnPrivilege>>): Map<AclKey, Set<Permission>> {
        //TODO: Actually implement this
        return mapPrivileges(privileges) { stp ->
            stp.flatMap { tp ->
                when (tp) {
                    ColumnPrivilege.ALL -> EnumSet.of(Permission.READ, Permission.WRITE)
                    else -> EnumSet.noneOf(Permission::class.java)
                }
            }.toSet()
        }
    }

    private fun <T> mapPrivileges(
            privileges: Map<String, Set<T>>, mapper: (Set<T>) -> Set<Permission>
    ): Map<AclKey, Set<Permission>> {
        val usernameToAclKeys = dbCredentialService.getSecurablePrincipalAclKeysFromUsernames(privileges.keys)
        return usernameToAclKeys
                .map { (dbUser, aclKey) -> aclKey to mapper(privileges.getValue(dbUser)) }
                .toMap()
    }


    private fun fromMetadata(
            organizationId: UUID,
            tableMetadata: TableMetadata,
            tableId: UUID,
            columnMetadata: ColumnMetadata
    ): ExternalSqlDatabaseColumnMapping {
        val columnFqn = buildColumnFqn(organizationId, tableMetadata, columnMetadata)
        val columnId = aclKeyReservations.reserveOrGetId(columnFqn)
        val column = OrganizationExternalDatabaseColumn(
                columnId,
                columnMetadata.name,
                columnMetadata.name,
                Optional.of(""),
                tableId,
                organizationId, 
        )
        return ExternalSqlDatabaseColumnMapping()
    }

}
