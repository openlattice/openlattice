package com.openlattice.organizations

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.authorization.*
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.DatasetApi.Companion.CONTROLLER
import com.openlattice.organization.DatasetApi
import com.openlattice.organization.DatasetApi.Companion.COLUMN_NAME
import com.openlattice.organization.DatasetApi.Companion.COLUMN_NAME_PATH
import com.openlattice.organization.DatasetApi.Companion.CONNECTION_TYPE
import com.openlattice.organization.DatasetApi.Companion.CONNECTION_TYPE_PATH
import com.openlattice.organization.DatasetApi.Companion.DATA
import com.openlattice.organization.DatasetApi.Companion.EXTERNAL_DATABASE
import com.openlattice.organization.DatasetApi.Companion.EXTERNAL_DATABASE_COLUMN
import com.openlattice.organization.DatasetApi.Companion.EXTERNAL_DATABASE_TABLE
import com.openlattice.organization.DatasetApi.Companion.ID
import com.openlattice.organization.DatasetApi.Companion.ID_PATH
import com.openlattice.organization.DatasetApi.Companion.ROW_COUNT
import com.openlattice.organization.DatasetApi.Companion.ROW_COUNT_PATH
import com.openlattice.organization.DatasetApi.Companion.TABLE_ID
import com.openlattice.organization.DatasetApi.Companion.TABLE_ID_PATH
import com.openlattice.organization.DatasetApi.Companion.TABLE_NAME
import com.openlattice.organization.DatasetApi.Companion.TABLE_NAME_PATH
import com.openlattice.organization.DatasetApi.Companion.USER_ID
import com.openlattice.organization.DatasetApi.Companion.USER_ID_PATH
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organization.OrganizationExternalDatabaseTableColumnsPair
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.annotation.PostConstruct
import javax.inject.Inject

@RestController
@RequestMapping(CONTROLLER)
class DatasetController : DatasetApi, AuthorizingComponent {

    companion object {
        private val logger = LoggerFactory.getLogger(DatasetController::class.java)
    }

    @Inject
    private lateinit var edms: ExternalDatabaseManagementService

    @Inject
    private lateinit var authorizations: AuthorizationManager

    @Inject
    private lateinit var securableObjectTypes: SecurableObjectResolveTypeService

    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance

    @Inject
    private lateinit var aclKeyReservations: HazelcastAclKeyReservationService

    @Timed
    @PostMapping(path = [ID_PATH + USER_ID_PATH + CONNECTION_TYPE_PATH + EXTERNAL_DATABASE])
    override fun addHBARecord(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(USER_ID) userId: String,
            @PathVariable(CONNECTION_TYPE) connectionType: String,
            @RequestBody ipAddresses: Set<String>
    ) {
        ensureOwnerAccess(AclKey(organizationId))
        if (connectionType == "local") {
            checkState(ipAddresses.isEmpty(), "Local connections may not specify an IP address")
        } else {
            checkState(ipAddresses.isNotEmpty(), "Host connections must specify at least one IP address")
        }
        val userPrincipal = Principal(PrincipalType.USER, userId)
        edms.addHBARecord(organizationId, userPrincipal, connectionType, ipAddresses)
    }

    @Timed
    @DeleteMapping(path = [ID_PATH + USER_ID_PATH + EXTERNAL_DATABASE])
    override fun removeHBARecord(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(USER_ID) userId: String
    ) {
        ensureOwnerAccess(AclKey(organizationId))
        val userPrincipal = Principal(PrincipalType.USER, userId)
        edms.removeHBARecord(organizationId, userPrincipal)
    }

    @Timed
    @GetMapping(path = [ID_PATH + EXTERNAL_DATABASE_TABLE])
    override fun getExternalDatabaseTables(
            @PathVariable(ID) organizationId: UUID): Set<OrganizationExternalDatabaseTable> {
        ensureOwnerAccess(AclKey(organizationId))
        return edms.getExternalDatabaseTables(organizationId)
    }

    @Timed
    @GetMapping(path = [ID_PATH + EXTERNAL_DATABASE_TABLE + EXTERNAL_DATABASE_COLUMN])
    override fun getExternalDatabaseTablesWithColumns(
            @PathVariable(ID) organizationId: UUID): Map<OrganizationExternalDatabaseTable, Set<OrganizationExternalDatabaseColumn>> {
        ensureOwnerAccess(AclKey(organizationId))
        return edms.getExternalDatabaseTablesWithColumns(organizationId)
    }

    @Timed
    @GetMapping(path = [ID_PATH + TABLE_ID_PATH + EXTERNAL_DATABASE_TABLE + EXTERNAL_DATABASE_COLUMN])
    override fun getExternalDatabaseTableWithColumns(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(TABLE_ID) tableId: UUID): OrganizationExternalDatabaseTableColumnsPair {
        ensureReadAccess(AclKey(organizationId, tableId))
        return edms.getExternalDatabaseTableWithColumns(tableId)
    }

    @Timed
    @GetMapping(path = [ID_PATH + TABLE_ID_PATH + ROW_COUNT_PATH + DATA])
    override fun getExternalDatabaseTableData(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(TABLE_ID) tableId: UUID,
            @PathVariable(ROW_COUNT) rowCount: Int): Map<UUID, List<Any?>> {
        ensureReadAccess(AclKey(organizationId, tableId))
        return edms.getExternalDatabaseTableData(organizationId, tableId, rowCount)
    }

    @Timed
    @GetMapping(path = [ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_TABLE])
    override fun getExternalDatabaseTable(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(TABLE_NAME) tableName: String): OrganizationExternalDatabaseTable {
        val tableId = getExternalDatabaseObjectId(organizationId, tableName)
        ensureReadAccess(AclKey(organizationId, tableId))
        return edms.getOrganizationExternalDatabaseTable(tableId)
    }

    @Timed
    @GetMapping(path = [ID_PATH + TABLE_NAME_PATH + COLUMN_NAME_PATH + EXTERNAL_DATABASE_COLUMN])
    override fun getExternalDatabaseColumn(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(TABLE_NAME) tableName: String,
            @PathVariable(COLUMN_NAME) columnName: String): OrganizationExternalDatabaseColumn {
        val tableId = getExternalDatabaseObjectId(organizationId, tableName)
        val columnId = getExternalDatabaseObjectId(tableId, columnName)
        ensureReadAccess(AclKey(organizationId, tableId, columnId))
        return edms.getOrganizationExternalDatabaseColumn(columnId)
    }

    @Timed
    @DeleteMapping(path = [ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_TABLE])
    override fun deleteExternalDatabaseTable(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(TABLE_NAME) tableName: String) {
        val tableId = getExternalDatabaseObjectId(organizationId, tableName)
        val aclKey = AclKey(organizationId, tableId)
        ensureOwnerAccess(aclKey)
        ensureObjectCanBeDeleted(tableId)
        authorizations.deletePermissions(aclKey)
        securableObjectTypes.deleteSecurableObjectType(aclKey)
        edms.deleteOrganizationExternalDatabaseTable(organizationId, tableId)
    }

    @Timed
    @DeleteMapping(path = [ID_PATH + EXTERNAL_DATABASE_TABLE])
    override fun deleteExternalDatabaseTables(
            @PathVariable(ID) organizationId: UUID,
            @RequestBody tableNames: Set<String>) {
        val tableIds = getExternalDatabaseObjectIds(organizationId, tableNames)
        tableIds.forEach { ensureObjectCanBeDeleted(it) }
        val aclKeys = tableIds.map { AclKey(organizationId, it) }.toSet()
        aclKeys.forEach { aclKey ->
            ensureOwnerAccess(aclKey)
            authorizations.deletePermissions(aclKey)
            securableObjectTypes.deleteSecurableObjectType(aclKey)
        }
        edms.deleteOrganizationExternalDatabaseTables(organizationId, tableIds)
    }

    @Timed
    @DeleteMapping(path = [ID_PATH + TABLE_NAME_PATH + COLUMN_NAME_PATH + EXTERNAL_DATABASE_COLUMN])
    override fun deleteExternalDatabaseColumn(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(TABLE_NAME) tableName: String,
            @PathVariable(COLUMN_NAME) columnName: String
    ) {
        val tableId = getExternalDatabaseObjectId(organizationId, tableName)
        val columnId = getExternalDatabaseObjectId(tableId, columnName)
        val aclKey = AclKey(organizationId, tableId, columnId)
        ensureOwnerAccess(aclKey)
        ensureObjectCanBeDeleted(columnId)
        authorizations.deletePermissions(aclKey)
        securableObjectTypes.deleteSecurableObjectType(aclKey)
        edms.deleteOrganizationExternalDatabaseColumn(organizationId, columnId)
    }

    @Timed
    @DeleteMapping(path = [ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_COLUMN])
    override fun deleteExternalDatabaseColumns(
            @PathVariable(ID) organizationId: UUID,
            @PathVariable(TABLE_NAME) tableName: String,
            @RequestBody columnNames: Set<String>
    ) {
        val tableId = getExternalDatabaseObjectId(organizationId, tableName)
        val columnIds = getExternalDatabaseObjectIds(tableId, columnNames)
        columnIds.forEach { ensureObjectCanBeDeleted(it) }
        val aclKeys = columnIds.map { AclKey(organizationId, tableId, it) }.toSet()
        aclKeys.forEach { aclKey ->
            ensureOwnerAccess(aclKey)
            authorizations.deletePermissions(aclKey)
            securableObjectTypes.deleteSecurableObjectType(aclKey)
        }
        edms.deleteOrganizationExternalDatabaseColumns(organizationId, columnIds)
    }

    private fun getExternalDatabaseObjectId(containingObjectId: UUID, name: String): UUID {
        val fqn = FullQualifiedName(containingObjectId.toString(), name).toString()
        val id = aclKeyReservations.getId(fqn)
        checkState(id != null, "External database object with name $name does not exist")
        return id!!
    }

    private fun getExternalDatabaseObjectIds( containingObjectId: UUID, names: Set<String>): Set<UUID> {
        val fqns = names.map{FullQualifiedName(containingObjectId.toString(), it).toString()}.toSet()
        val ids = aclKeyReservations.getIds(fqns).toSet()
        ids.forEach{
            checkState(it != null, "External database object with name $it does not exist")
        }
        return ids
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizations
    }
}