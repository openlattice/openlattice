package com.openlattice.organizations

import com.codahale.metrics.annotation.Timed
import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.authorization.*
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.OrganizationExternalDatabaseApi.Companion.CONTROLLER
import com.openlattice.organization.OrganizationExternalDatabaseApi
import com.openlattice.organization.OrganizationExternalDatabaseApi.Companion.COLUMN_NAME_PATH
import com.openlattice.organization.OrganizationExternalDatabaseApi.Companion.EXTERNAL_DATABASE
import com.openlattice.organization.OrganizationExternalDatabaseApi.Companion.EXTERNAL_DATABASE_COLUMN
import com.openlattice.organization.OrganizationExternalDatabaseApi.Companion.EXTERNAL_DATABASE_TABLE
import com.openlattice.organization.OrganizationExternalDatabaseApi.Companion.ID_PATH
import com.openlattice.organization.OrganizationExternalDatabaseApi.Companion.TABLE_NAME_PATH
import com.openlattice.organization.OrganizationExternalDatabaseApi.Companion.USER_ID_PATH
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organization.OrganizationsApi
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.*
import java.util.*
import java.util.stream.Collectors
import javax.annotation.PostConstruct
import javax.inject.Inject

@RestController
@RequestMapping(CONTROLLER)
class OrganizationExternalDatabaseController : OrganizationExternalDatabaseApi, AuthorizingComponent {

    companion object {
        private val logger = LoggerFactory.getLogger(OrganizationExternalDatabaseController::class.java)
    }

    @Inject
    private lateinit var edms: ExternalDatabaseManagementService

    @Inject
    private lateinit var authorizations: AuthorizationManager

    @Inject
    private lateinit var securableObjectTypes: SecurableObjectResolveTypeService

    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance

    private lateinit var aclKeysMap: IMap<String, UUID>

    @PostConstruct
    fun init() {
        this.aclKeysMap = hazelcastInstance.getMap(HazelcastMap.ACL_KEYS.name)
    }

    //TODO timed?
    @Timed
    @PostMapping(path = [ID_PATH + EXTERNAL_DATABASE_TABLE])
    override fun createExternalDatabaseTable(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @RequestBody organizationExternalDatabaseTable: OrganizationExternalDatabaseTable): UUID {
        ensureOwner(organizationId)
        return edms.createOrganizationExternalDatabaseTable(organizationId, organizationExternalDatabaseTable)
    }

    @Timed
    @PostMapping(path = [ID_PATH + EXTERNAL_DATABASE_COLUMN])
    override fun createExternalDatabaseColumn(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @RequestBody organizationExternalDatabaseColumn: OrganizationExternalDatabaseColumn): UUID {
        ensureOwner(organizationId)
        return edms.createOrganizationExternalDatabaseColumn(organizationId, organizationExternalDatabaseColumn)
    }

    @Timed
    @PostMapping(path = [ID_PATH + USER_ID_PATH + EXTERNAL_DATABASE])
    override fun addTrustedUser(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.USER_ID) userId: String,
            @RequestBody ipAddresses: Set<String>
    ) {
        ensureOwner(organizationId)
        val userPrincipal = Principal(PrincipalType.USER, userId)
        edms.addTrustedUser(organizationId, userPrincipal, ipAddresses)
    }

    @Timed
    @GetMapping(path = [ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_TABLE])
    override fun getExternalDatabaseTable(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.TABLE_NAME) tableName: String): OrganizationExternalDatabaseTable {
        val tableId = getExternalDatabaseObjectId(organizationId, tableName)
        ensureReadAccess(AclKey(organizationId, tableId))
        return edms.getOrganizationExternalDatabaseTable(tableId)
    }

    @Timed
    @GetMapping(path = [ID_PATH + TABLE_NAME_PATH + COLUMN_NAME_PATH + EXTERNAL_DATABASE_COLUMN])
    override fun getExternalDatabaseColumn(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.TABLE_NAME) tableName: String,
            @PathVariable(OrganizationsApi.COLUMN_NAME) columnName: String): OrganizationExternalDatabaseColumn {
        val tableId = getExternalDatabaseObjectId(organizationId, tableName)
        val columnId = getExternalDatabaseObjectId(tableId, columnName)
        ensureReadAccess(AclKey(organizationId, tableId, columnId))
        return edms.getOrganizationExternalDatabaseColumn(columnId)
    }

    @Timed
    @DeleteMapping(path = [ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_TABLE])
    override fun deleteExternalDatabaseTable(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.TABLE_NAME) tableName: String) {
        val tableId = getExternalDatabaseObjectId(organizationId, tableName)
        val aclKey = AclKey(organizationId, tableId)
        ensureOwnerAccess(aclKey)
        ensureObjectCanBeDeleted(tableId)
        authorizations.deletePermissions(aclKey)
        securableObjectTypes.deleteSecurableObjectType(aclKey)
        edms.deleteOrganizationExternalDatabaseTable(organizationId, tableName, tableId)
    }

    @Timed
    @DeleteMapping(path = [ID_PATH + EXTERNAL_DATABASE_TABLE])
    override fun deleteExternalDatabaseTables(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @RequestBody tableNames: Set<String>) {
        val tableNameById = tableNames.stream().collect<Map<UUID, String>, Any>(
                Collectors.toMap({ tableName -> getExternalDatabaseObjectId(organizationId, tableName) },
                        { tableName -> tableName }))
        val aclKeys = tableNameById.keys.stream().map { tableId -> AclKey(organizationId, tableId) }
                .collect<Set<AclKey>, Any>(Collectors.toSet())
        tableNameById.forEach { (id, name) -> ensureObjectCanBeDeleted(id) }
        aclKeys.forEach { aclKey ->
            ensureOwnerAccess(aclKey)
            authorizations.deletePermissions(aclKey)
            securableObjectTypes.deleteSecurableObjectType(aclKey)
        }
        edms.deleteOrganizationExternalDatabaseTables(organizationId, tableNameById)
    }

    @Timed
    @DeleteMapping(path = [ID_PATH + TABLE_NAME_PATH + COLUMN_NAME_PATH + EXTERNAL_DATABASE_COLUMN])
    override fun deleteExternalDatabaseColumn(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.TABLE_NAME) tableName: String,
            @PathVariable(OrganizationsApi.COLUMN_NAME) columnName: String
    ) {
        val tableId = getExternalDatabaseObjectId(organizationId, tableName)
        val columnId = getExternalDatabaseObjectId(tableId, columnName)
        val aclKey = AclKey(organizationId, tableId, columnId)
        ensureOwnerAccess(aclKey)
        ensureObjectCanBeDeleted(tableId)
        authorizations.deletePermissions(aclKey)
        securableObjectTypes.deleteSecurableObjectType(aclKey)
        edms.deleteOrganizationExternalDatabaseColumn(organizationId, tableName, columnName, columnId)
    }

    @Timed
    @DeleteMapping(path = [ID_PATH + TABLE_NAME_PATH + EXTERNAL_DATABASE_COLUMN])
    override fun deleteExternalDatabaseColumns(
            @PathVariable(OrganizationsApi.ID) organizationId: UUID,
            @PathVariable(OrganizationsApi.TABLE_NAME) tableName: String,
            @RequestBody columnNames: Set<String>
    ) {
        val tableId = getExternalDatabaseObjectId(organizationId, tableName)
        val columnNameById = columnNames.stream().collect<Map<UUID, String>, Any>(
                Collectors.toMap({ columnName -> getExternalDatabaseObjectId(tableId, columnName) },
                        { columnName -> columnName }))
        val aclKeys = columnNameById.keys.stream().map { columnId -> AclKey(organizationId, tableId, columnId) }
                .collect<Set<AclKey>, Any>(Collectors.toSet())
        columnNameById.forEach { (id, name) -> ensureObjectCanBeDeleted(id) }
        aclKeys.forEach { aclKey ->
            ensureOwnerAccess(aclKey)
            authorizations.deletePermissions(aclKey)
            securableObjectTypes.deleteSecurableObjectType(aclKey)
        }
        edms.deleteOrganizationExternalDatabaseColumns(organizationId, tableName, columnNameById)
    }

    private fun ensureOwner(organizationId: UUID): AclKey {
        val aclKey = AclKey(organizationId)
        accessCheck(aclKey, EnumSet.of(Permission.OWNER))
        return aclKey
    }

    private fun getExternalDatabaseObjectId(containingObjectId: UUID, name: String): UUID {
        val fqn = FullQualifiedName(containingObjectId.toString(), name)
        val id = aclKeysMap.get(fqn.fullQualifiedNameAsString)
        checkState(id != null, "External database object with name $name does not exist")
        return id!!
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizations
    }
}