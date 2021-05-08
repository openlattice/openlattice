package com.openlattice.datasets

import com.codahale.metrics.annotation.Timed
import com.openlattice.auditing.AuditRecordEntitySetsManager
import com.openlattice.auditing.AuditingComponent
import com.openlattice.auditing.AuditingManager
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.authorization.EdmAuthorizationHelper
import com.openlattice.authorization.Permission
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.data.DataDeletionManager
import com.openlattice.data.DataGraphManager
import com.openlattice.datasets.DatasetMetadataApi.Companion.COLUMN_PATH
import com.openlattice.datasets.DatasetMetadataApi.Companion.DATASET_ID
import com.openlattice.datasets.DatasetMetadataApi.Companion.DATASET_ID_PATH
import com.openlattice.datasets.DatasetMetadataApi.Companion.DATASET_PATH
import com.openlattice.datasets.DatasetMetadataApi.Companion.ID
import com.openlattice.datasets.DatasetMetadataApi.Companion.ID_PATH
import com.openlattice.datasets.DatasetMetadataApi.Companion.UPDATE_PATH
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.organizations.ExternalDatabaseManagementService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController
import java.util.*
import javax.inject.Inject

@SuppressFBWarnings(
        value = ["BC_BAD_CAST_TO_ABSTRACT_COLLECTION"],
        justification = "Allowing kotlin collection mapping cast to List"
)
@RestController
@RequestMapping(DatasetMetadataApi.CONTROLLER)
class DatasetMetadataController @Inject
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
        private val datasetService: DatasetService,
        private val edms: ExternalDatabaseManagementService
) : DatasetMetadataApi, AuthorizingComponent, AuditingComponent {

    @Timed
    @RequestMapping(
            path = [DATASET_PATH + ID_PATH],
            method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getDataset(@PathVariable(ID) datasetId: UUID): Dataset {
        ensureReadAccess(AclKey(datasetId))
        return datasetService.getDataset(datasetId)
    }

    @Timed
    @RequestMapping(
            path = [DATASET_PATH],
            method = [RequestMethod.POST],
            consumes = [MediaType.APPLICATION_JSON_VALUE],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getDatasets(@RequestBody datasetIds: Set<UUID>): Map<UUID, Dataset> {
        accessCheck(datasetIds.associate { AclKey(it) to EnumSet.of(Permission.READ) })
        return datasetService.getDatasets(datasetIds)
    }

    @Timed
    @RequestMapping(
            path = [COLUMN_PATH + DATASET_ID_PATH + ID_PATH],
            method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getDatasetColumn(
            @PathVariable(DATASET_ID) datasetId: UUID,
            @PathVariable(ID) datasetColumnId: UUID
    ): DatasetColumn {
        val aclKey = AclKey(datasetId, datasetColumnId)
        ensureReadAccess(aclKey)
        return datasetService.getDatasetColumn(aclKey)
    }

    @Timed
    @RequestMapping(
            path = [COLUMN_PATH],
            method = [RequestMethod.POST],
            consumes = [MediaType.APPLICATION_JSON_VALUE],
            produces = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun getDatasetColumns(@RequestBody datasetColumnAclKeys: Set<AclKey>): Map<AclKey, DatasetColumn> {
        accessCheck(datasetColumnAclKeys.associateWith { EnumSet.of(Permission.READ) })
        return datasetService.getDatasetColumns(datasetColumnAclKeys)
    }

    @Timed
    @RequestMapping(
            path = [UPDATE_PATH + ID_PATH],
            method = [RequestMethod.PATCH],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateDatasetMetadata(@PathVariable(ID) id: UUID, @RequestBody update: SecurableObjectMetadataUpdate) {
        val aclKey = AclKey(id)
        ensureOwnerAccess(aclKey)

        updateSecurableObjectMetadata(aclKey, update)
        datasetService.updateObjectMetadata(aclKey, update)

    }

    @Timed
    @RequestMapping(
            path = [UPDATE_PATH + DATASET_ID_PATH + ID_PATH],
            method = [RequestMethod.PATCH],
            consumes = [MediaType.APPLICATION_JSON_VALUE]
    )
    override fun updateDatasetColumnMetadata(
            @PathVariable(DATASET_ID) datasetId: UUID,
            @PathVariable(ID) id: UUID,
            @RequestBody update: SecurableObjectMetadataUpdate
    ) {
        val aclKey = AclKey(datasetId, id)
        ensureOwnerAccess(aclKey)

        updateSecurableObjectMetadata(aclKey, update)
        datasetService.updateObjectMetadata(aclKey, update)
    }

    private fun updateSecurableObjectMetadata(aclKey: AclKey, update: SecurableObjectMetadataUpdate) {
        val metadataUpdate = SecurableObjectMetadataUpdate.toMetadataUpdate(update)

        when (val objectType = datasetService.getObjectType(aclKey)) {
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