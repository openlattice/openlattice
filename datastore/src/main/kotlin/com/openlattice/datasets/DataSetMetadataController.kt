package com.openlattice.datasets

import com.codahale.metrics.annotation.Timed
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.auditing.AuditingComponent
import com.openlattice.auditing.AuditingManager
import com.openlattice.authorization.*
import com.openlattice.authorization.EdmAuthorizationHelper.READ_PERMISSION
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.controllers.exceptions.ResourceNotFoundException
import com.openlattice.data.DataDeletionManager
import com.openlattice.data.DataGraphManager
import com.openlattice.datasets.DataSetMetadataApi.Companion.COLUMNS_PATH
import com.openlattice.datasets.DataSetMetadataApi.Companion.COLUMN_ID_PARAM
import com.openlattice.datasets.DataSetMetadataApi.Companion.COLUMN_ID_PATH
import com.openlattice.datasets.DataSetMetadataApi.Companion.DATA_SETS_PATH
import com.openlattice.datasets.DataSetMetadataApi.Companion.DATA_SET_ID_PARAM
import com.openlattice.datasets.DataSetMetadataApi.Companion.DATA_SET_ID_PATH
import com.openlattice.datasets.DataSetMetadataApi.Companion.ORGANIZATIONS_PATH
import com.openlattice.datasets.DataSetMetadataApi.Companion.ORGANIZATION_ID_PARAM
import com.openlattice.datasets.DataSetMetadataApi.Companion.ORGANIZATION_ID_PATH
import com.openlattice.datasets.DataSetMetadataApi.Companion.UPDATE_PATH
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.organizations.ExternalDatabaseManagementService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import java.util.*
import java.util.stream.Collectors
import javax.inject.Inject

@SuppressFBWarnings(
    value = ["BC_BAD_CAST_TO_ABSTRACT_COLLECTION"],
    justification = "Allowing kotlin collection mapping cast to List"
)
@RestController
@RequestMapping(DataSetMetadataApi.CONTROLLER)
class DataSetMetadataController @Inject
constructor(
    private val authorizations: AuthorizationManager,
    private val edmManager: EdmManager,
    private val aresManager: AuditRecordEntitySetsManager,
    private val auditingManager: AuditingManager,
    private val dgm: DataGraphManager,
    private val spm: SecurePrincipalsManager,
    private val authzHelper: EdmAuthorizationHelper,
    private val deletionManager: DataDeletionManager,
    private val entitySetManager: EntitySetManager,
    private val dataSetService: DataSetService,
    private val edms: ExternalDatabaseManagementService
) : DataSetMetadataApi, AuthorizingComponent, AuditingComponent {

    @Timed
    @GetMapping(
        path = [DATA_SETS_PATH + DATA_SET_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getDataSetMetadata(@PathVariable(DATA_SET_ID_PARAM) dataSetId: UUID): DataSet {
        ensureReadAccess(AclKey(dataSetId))
        return dataSetService.getDataSet(dataSetId)
            ?: throw ResourceNotFoundException("data set $dataSetId not found")
    }

    @Timed
    @PostMapping(
        path = [DATA_SETS_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getDataSetsMetadata(@RequestBody dataSetIds: Set<UUID>): Map<UUID, DataSet> {
        accessCheck(dataSetIds.associate { AclKey(it) to EnumSet.of(Permission.READ) })
        return dataSetService.getDataSets(dataSetIds)
    }

    @Timed
    @GetMapping(
        path = [COLUMNS_PATH + DATA_SET_ID_PATH + COLUMN_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getDataSetColumnMetadata(
        @PathVariable(DATA_SET_ID_PARAM) dataSetId: UUID,
        @PathVariable(COLUMN_ID_PARAM) columnId: UUID
    ): DataSetColumn {
        val aclKey = AclKey(dataSetId, columnId)
        ensureReadAccess(aclKey)
        return dataSetService.getDatasetColumn(aclKey)
    }

    @Timed
    @PostMapping(
        path = [COLUMNS_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getDataSetColumnsMetadata(@RequestBody dataSetIds: Set<UUID>): Map<UUID, List<DataSetColumn>> {
        accessCheck(dataSetIds.associate { AclKey(it) to EnumSet.of(Permission.READ) })
        val datasetToColumns = dataSetService.getColumnsInDatasets(dataSetIds)
        val accessChecks = datasetToColumns
            .flatMapTo(mutableSetOf()) { it.value }
            .mapTo(mutableSetOf()) { AccessCheck(it.getAclKey(), EnumSet.of(Permission.READ)) }
        val authorizedColumns = authorizations
            .accessChecksForPrincipals(accessChecks, Principals.getCurrentPrincipals())
            .filter { it.permissions.getOrDefault(Permission.READ, false) }
            .map { it.aclKey }
            .collect(Collectors.toSet())
        return datasetToColumns.mapValues { it.value.filter { col -> authorizedColumns.contains(col.getAclKey()) } }
    }

    @Timed
    @GetMapping(
        path = [DATA_SETS_PATH + ORGANIZATIONS_PATH + ORGANIZATION_ID_PATH],
        produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getOrganizationDataSetsMetadata(
        @PathVariable(ORGANIZATION_ID_PARAM) organizationId: UUID
    ): Map<UUID, DataSet> {
        val aclKey = AclKey(organizationId)
        ensureReadAccess(aclKey)
        val dataSetIds = dataSetService.getOrganizationDataSetIds(organizationId)
        val accessChecks = dataSetIds.mapTo(mutableSetOf()) { AccessCheck(AclKey(it), READ_PERMISSION) }
        val authorizedDataSetIds = authorizations
            .accessChecksForPrincipals(accessChecks, Principals.getCurrentPrincipals())
            .filter { it.permissions.getOrDefault(Permission.READ, false) }
            .map { it.aclKey.first() }
            .collect(Collectors.toSet())
        return dataSetService.getDataSets(authorizedDataSetIds)
    }

    @Timed
    @PatchMapping(
        path = [UPDATE_PATH + DATA_SET_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateDataSetMetadata(
        @PathVariable(DATA_SET_ID_PARAM) dataSetId: UUID,
        @RequestBody metadata: SecurableObjectMetadataUpdate
    ) {
        val aclKey = AclKey(dataSetId)
        ensureOwnerAccess(aclKey)
        updateSecurableObjectMetadata(aclKey, metadata)
        dataSetService.updateObjectMetadata(aclKey, metadata)
    }

    @Timed
    @PatchMapping(
        path = [UPDATE_PATH + DATA_SET_ID_PATH + COLUMN_ID_PATH],
        consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateDataSetColumnMetadata(
        @PathVariable(DATA_SET_ID_PARAM) dataSetId: UUID,
        @PathVariable(COLUMN_ID_PARAM) columnId: UUID,
        @RequestBody metadata: SecurableObjectMetadataUpdate
    ) {
        val aclKey = AclKey(dataSetId, columnId)
        ensureOwnerAccess(aclKey)
        updateSecurableObjectMetadata(aclKey, metadata)
        dataSetService.updateObjectMetadata(aclKey, metadata)
    }

    private fun updateSecurableObjectMetadata(aclKey: AclKey, update: SecurableObjectMetadataUpdate) {
        val metadataUpdate = SecurableObjectMetadataUpdate.toMetadataUpdate(update)

        when (val objectType = dataSetService.getObjectType(aclKey)) {
            SecurableObjectType.EntitySet -> {
                entitySetManager.updateEntitySetMetadata(aclKey.first(), metadataUpdate)
            }
            SecurableObjectType.OrganizationExternalDatabaseTable -> {
                edms.updateExternalTableMetadata(aclKey.first(), metadataUpdate)
            }
            SecurableObjectType.OrganizationExternalDatabaseColumn -> {
                edms.updateExternalColumnMetadata(aclKey.first(), aclKey.last(), metadataUpdate)
            }
            SecurableObjectType.PropertyTypeInEntitySet -> {
            }
            else -> throw IllegalArgumentException("Cannot update metadata for object of type $objectType")
        }
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizations
    }

    override fun getAuditingManager(): AuditingManager {
        return auditingManager
    }
}
