package com.openlattice.organizations.controllers

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Preconditions.checkState
import com.google.common.net.InetAddresses
import com.openlattice.authorization.*
import com.openlattice.controllers.exceptions.ForbiddenException
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.organization.*
import com.openlattice.organizations.ExternalDatabaseManagementService
import com.openlattice.postgres.PostgresConnectionType
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.web.bind.annotation.*
import java.util.*
import java.util.stream.Collectors
import javax.inject.Inject

@SuppressFBWarnings(
        value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "BC_BAD_CAST_TO_ABSTRACT_COLLECTION"],
        justification = "Allowing redundant kotlin null check on lateinit variables, " +
                "Allowing kotlin collection mapping cast to List")
@RestController
@RequestMapping(CONTROLLER)
class DatasetController : DatasetApi, AuthorizingComponent {

    @Inject
    private lateinit var edms: ExternalDatabaseManagementService

    @Inject
    private lateinit var authorizations: AuthorizationManager

    @Inject
    private lateinit var aclKeyReservations: HazelcastAclKeyReservationService

    @Timed
    @PostMapping(path = [ID_PATH + USER_ID_PATH + CONNECTION_TYPE_PATH + EXTERNAL_DATABASE])
    override fun addHBARecord(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(USER_ID) userId: String,
            @PathVariable(CONNECTION_TYPE) connectionType: PostgresConnectionType,
            @RequestBody ipAddress: String
    ) {
        ensureOwnerAccess(AclKey(organizationId))
        validateHBAParameters(connectionType, ipAddress)
        val userPrincipal = Principal(PrincipalType.USER, userId)
        edms.addHBARecord(organizationId, userPrincipal, connectionType, ipAddress)
    }

    @Timed
    @DeleteMapping(path = [ID_PATH + USER_ID_PATH + CONNECTION_TYPE_PATH + EXTERNAL_DATABASE])
    override fun removeHBARecord(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(USER_ID) userId: String,
            @PathVariable(CONNECTION_TYPE) connectionType: PostgresConnectionType,
            @RequestBody ipAddress: String
    ) {
        ensureOwnerAccess(AclKey(organizationId))
        validateHBAParameters(connectionType, ipAddress)
        val userPrincipal = Principal(PrincipalType.USER, userId)
        edms.removeHBARecord(organizationId, userPrincipal, connectionType, ipAddress)
    }

    @Timed
    @GetMapping(path = [ID_PATH + EXTERNAL_DATABASE_TABLE])
    override fun getExternalDatabaseTables(
            @PathVariable(ID) organizationId: UUID): Set<OrganizationExternalDatabaseTable> {
        val tables = edms.getExternalDatabaseTables(organizationId)
        val authorizedTableIds = getAuthorizedTableIds(tables.map { it.key }.toSet(), Permission.READ)
        return tables.filter { it.key in authorizedTableIds }.map { it.value }.toSet()
    }

    @Timed
    @GetMapping(path = [ID_PATH + EXTERNAL_DATABASE_TABLE + EXTERNAL_DATABASE_COLUMN])
    override fun getExternalDatabaseTablesWithColumnMetadata(
            @PathVariable(ID) organizationId: UUID): Set<OrganizationExternalDatabaseTableColumnsPair> {
        val columnsByTable = edms.getExternalDatabaseTablesWithColumns(organizationId)
        val authorizedTableIds = getAuthorizedTableIds(columnsByTable.keys.map { it.first }.toSet(), Permission.READ)
        val columnsByAuthorizedTable = columnsByTable.filter { it.key.first in authorizedTableIds }
        return columnsByAuthorizedTable.map {
            val table = it.key.second
            val columns = it.value.map { entry -> entry.value }.toSet()
            return@map OrganizationExternalDatabaseTableColumnsPair(table, columns)
        }.toSet()
    }

    @Timed
    @GetMapping(path = [ID_PATH + PERMISSION_PATH + EXTERNAL_DATABASE_TABLE + EXTERNAL_DATABASE_COLUMN + AUTHORIZED])
    override fun getAuthorizedExternalDbTablesWithColumnMetadata(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(PERMISSION) permission: Permission
    ): Set<OrganizationExternalDatabaseTableColumnsPair> {
        val columnsByTable = edms.getExternalDatabaseTablesWithColumns(organizationId)
        val authorizedTableIds = getAuthorizedTableIds(columnsByTable.keys.map { it.first }.toSet(), permission)
        val columnsByAuthorizedTable = columnsByTable.filter { it.key.first in authorizedTableIds }
        return columnsByAuthorizedTable.map {
            val table = it.key.second
            val columns = it.value.filter { (columnId, _) ->
                val authorizedColumnIds = getAuthorizedColumnIds(it.key.first, it.value.map { entry -> entry.key }.toSet(), permission)
                columnId in authorizedColumnIds
            }.map { entry -> entry.value }.toSet()
            return@map OrganizationExternalDatabaseTableColumnsPair(table, columns)
        }.toSet()

    }

    @Timed
    @GetMapping(path = [ID_PATH + TABLE_ID_PATH + EXTERNAL_DATABASE_TABLE + EXTERNAL_DATABASE_COLUMN])
    override fun getExternalDatabaseTableWithColumnMetadata(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(TABLE_ID) tableId: UUID): OrganizationExternalDatabaseTableColumnsPair {
        ensureReadAccess(AclKey(tableId))
        return edms.getExternalDatabaseTableWithColumns(tableId)
    }

    @Timed
    @GetMapping(path = [ID_PATH + TABLE_ID_PATH + ROW_COUNT_PATH + DATA])
    override fun getExternalDatabaseTableData(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(TABLE_ID) tableId: UUID,
            @PathVariable(ROW_COUNT) rowCount: Int): Map<UUID, List<Any?>> {
        ensureReadAccess(AclKey(tableId))
        val columns = edms.getExternalDatabaseTableWithColumns(tableId).columns
        val authorizedColumnIds = getAuthorizedColumnIds(tableId, columns.map { it.id }.toSet(), Permission.READ)
        if (authorizedColumnIds.isEmpty()) {
            throw ForbiddenException("Unable to read data from table $tableId. Missing ${Permission.READ} permission on all columns.")
        }
        val authorizedColumns = columns.filter { it.id in authorizedColumnIds }.toSet()
        return edms.getExternalDatabaseTableData(organizationId, tableId, authorizedColumns, rowCount)
    }

    @Timed
    @GetMapping(path = [ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_TABLE])
    override fun getExternalDatabaseTable(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(TABLE_NAME) tableName: String): OrganizationExternalDatabaseTable {
        val (_, tableId) = getExternalDatabaseObjectFqnToIdPair(organizationId, tableName)
        ensureReadAccess(AclKey(tableId))
        return edms.getOrganizationExternalDatabaseTable(tableId)
    }

    @Timed
    @GetMapping(path = [ID_PATH + TABLE_NAME_PATH + COLUMN_NAME_PATH + EXTERNAL_DATABASE_COLUMN])
    override fun getExternalDatabaseColumn(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(TABLE_NAME) tableName: String,
            @PathVariable(COLUMN_NAME) columnName: String): OrganizationExternalDatabaseColumn {
        val (_, tableId) = getExternalDatabaseObjectFqnToIdPair(organizationId, tableName)
        val (_, columnId) = getExternalDatabaseObjectFqnToIdPair(tableId, columnName)
        ensureReadAccess(AclKey(tableId, columnId))
        return edms.getOrganizationExternalDatabaseColumn(columnId)
    }

    //TODO Metadata update probably won't work
    @Timed
    @PatchMapping(path = [ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_TABLE])
    override fun updateExternalDatabaseTable(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(TABLE_NAME) tableName: String,
            @RequestBody metadataUpdate: MetadataUpdate) {
        val tableFqnToId = getExternalDatabaseObjectFqnToIdPair(organizationId, tableName)
        ensureWriteAccess(AclKey(tableFqnToId.second))
        edms.updateOrganizationExternalDatabaseTable(organizationId, tableFqnToId, metadataUpdate)
    }

    @SuppressFBWarnings(
            value = ["RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"],
            justification = "lateinit prevents NPE here"
    )
    @Timed
    @PatchMapping(path = [ID_PATH + TABLE_NAME_PATH + COLUMN_NAME_PATH + EXTERNAL_DATABASE_COLUMN])
    override fun updateExternalDatabaseColumn(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(TABLE_NAME) tableName: String,
            @PathVariable(COLUMN_NAME) columnName: String,
            @RequestBody metadataUpdate: MetadataUpdate) {
        val tableFqnToId = getExternalDatabaseObjectFqnToIdPair(organizationId, tableName)
        ensureWriteAccess(AclKey(tableFqnToId.second))
        val columnFqnToId = getExternalDatabaseObjectFqnToIdPair(tableFqnToId.second, columnName)
        edms.updateOrganizationExternalDatabaseColumn(organizationId, tableFqnToId, columnFqnToId, metadataUpdate)
    }

    @Timed
    @DeleteMapping(path = [ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_TABLE])
    override fun deleteExternalDatabaseTable(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(TABLE_NAME) tableName: String) {
        deleteExternalDatabaseTables(organizationId, setOf(tableName))
    }

    @Timed
    @DeleteMapping(path = [ID_PATH + EXTERNAL_DATABASE_TABLE])
    override fun deleteExternalDatabaseTables(
            @PathVariable(ID) organizationId: UUID,
            @RequestBody tableNames: Set<String>) {
        val tableIdByFqn = getExternalDatabaseObjectIdByFqnMap(organizationId, tableNames)
        val tableIds = tableIdByFqn.values.toSet()
        val authorizedTableIds = getAuthorizedTableIds(tableIds, Permission.OWNER)
        if (tableIds.size != authorizedTableIds.size) {
            throw ForbiddenException("Insufficient permissions on tables to perform this action")
        }

        tableIds.forEach { tableId ->
            ensureObjectCanBeDeleted(tableId)
            val columnIds = edms.getExternalDatabaseTableWithColumns(tableId).columns.map { it.id }.toSet()
            val authorizedColumnIds = getAuthorizedColumnIds(tableId, columnIds, Permission.OWNER)
            if (columnIds.size != authorizedColumnIds.size) {
                throw ForbiddenException("Insufficient permissions on column objects to perform this action")
            }
            columnIds.forEach { ensureObjectCanBeDeleted(it) }
        }
        edms.deleteOrganizationExternalDatabaseTables(organizationId, tableIdByFqn)
    }

    @Timed
    @DeleteMapping(path = [ID_PATH + TABLE_NAME_PATH + COLUMN_NAME_PATH + EXTERNAL_DATABASE_COLUMN])
    override fun deleteExternalDatabaseColumn(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(TABLE_NAME) tableName: String,
            @PathVariable(COLUMN_NAME) columnName: String
    ) {
        deleteExternalDatabaseColumns(organizationId, tableName, setOf(columnName))
    }

    @Timed
    @DeleteMapping(path = [ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_COLUMN])
    override fun deleteExternalDatabaseColumns(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(TABLE_NAME) tableName: String,
            @RequestBody columnNames: Set<String>
    ) {
        val tableFqnToId = getExternalDatabaseObjectFqnToIdPair(organizationId, tableName)
        val columnIdByFqn = getExternalDatabaseObjectIdByFqnMap(tableFqnToId.second, columnNames)
        columnIdByFqn.forEach { ensureObjectCanBeDeleted(it.value) }
        val aclKeys = columnIdByFqn.map { AclKey(tableFqnToId.second, it.value) }.toSet()
        aclKeys.forEach { aclKey ->
            ensureOwnerAccess(aclKey)
        }
        edms.deleteOrganizationExternalDatabaseColumns(organizationId, mapOf(tableFqnToId to columnIdByFqn))
    }

    private fun getExternalDatabaseObjectFqnToIdPair(containingObjectId: UUID, name: String): Pair<String, UUID> {
        val fqn = FullQualifiedName(containingObjectId.toString(), name).toString()
        val id = aclKeyReservations.getId(fqn)
        checkState(id != null, "External database object with name $name does not exist")
        return Pair(fqn, id!!)
    }

    private fun getExternalDatabaseObjectIdByFqnMap(containingObjectId: UUID, names: Set<String>): Map<String, UUID> {
        val fqns = names.map { FullQualifiedName(containingObjectId.toString(), it).toString() }.toSet()
        return aclKeyReservations.getIdsByFqn(fqns)
    }

    private fun getAuthorizedTableIds(tableIds: Set<UUID>, permission: Permission): Set<UUID> {
        return authorizations.accessChecksForPrincipals(
                tableIds.map { AccessCheck(AclKey(it), EnumSet.of<Permission>(permission)) }.toSet(),
                Principals.getCurrentPrincipals()
        )
                .filter { it.permissions[permission]!! }
                .map { it.aclKey[0] }.collect(Collectors.toSet())
    }

    private fun getAuthorizedColumnIds(tableId: UUID, columnIds: Set<UUID>, permission: Permission): Set<UUID> {
        return authorizations.accessChecksForPrincipals(
                columnIds.map { columnId -> AccessCheck(AclKey(tableId, columnId), EnumSet.of(permission)) }.toSet(),
                Principals.getCurrentPrincipals()
        )
                .filter { authz -> authz.permissions[permission]!! }
                .map { authz -> authz.aclKey[1] }.collect(Collectors.toSet())
    }

    private fun validateHBAParameters(connectionType: PostgresConnectionType, ipAddress: String) {
        if (connectionType == PostgresConnectionType.LOCAL) {
            checkState(ipAddress.isEmpty(), "Local connections may not specify an IP address")
        } else {
            checkState(ipAddress.isNotEmpty(), "Host connections must specify at least one IP address")
        }
        val splitIpAddress = ipAddress.split("/")
        if (!InetAddresses.isInetAddress(splitIpAddress[0]) || splitIpAddress.size != 2) {
            throw IllegalStateException("Invalid IP address")
        }
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizations
    }
}