/*
 * Copyright (C) 2020. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.organizations

import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.Ace
import com.openlattice.authorization.Acl
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.Permission
import com.openlattice.authorization.PrincipalsMapManager
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.data.DataDeletionManager
import com.openlattice.data.DataGraphManager
import com.openlattice.data.DeleteType
import com.openlattice.data.EntityKey
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.ExternalColumn
import com.openlattice.organization.ExternalTable
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.processors.OrganizationReadEntryProcessor
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.stereotype.Service
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Service
class OrganizationMetadataEntitySetsService(
    hazelcastInstance: HazelcastInstance,
    private val edmService: EdmManager,
    private val principalsMapManager: PrincipalsMapManager,
    private val authorizationManager: AuthorizationManager
) {

    protected val organizations = HazelcastMap.ORGANIZATIONS.getMap(hazelcastInstance)

    lateinit var dataDeletionService: DataDeletionManager
    lateinit var dataGraphManager: DataGraphManager
    lateinit var entitySetsManager: EntitySetManager
    lateinit var organizationService: HazelcastOrganizationService

    private lateinit var organizationMetadataEntityTypeId: UUID
    private lateinit var datasetEntityTypeId: UUID
    private lateinit var columnsEntityTypeId: UUID
    private lateinit var omAuthorizedPropertyTypes: Map<UUID, PropertyType>
    private lateinit var datasetsAuthorizedPropertyTypes: Map<UUID, PropertyType>
    private lateinit var columnAuthorizedPropertyTypes: Map<UUID, PropertyType>
    private lateinit var propertyTypes: Map<String, PropertyType>

    /**
     * This is done this way, so that service can startup even on an empty stack for now.
     *
     * In the future we will enforce creation of all necessary edm elements at startup.
     */
    private fun initializeFields() {
        if (!this::organizationMetadataEntityTypeId.isInitialized) {
            val om = edmService.getEntityType(ORGANIZATION_METADATA_ET)
            organizationMetadataEntityTypeId = om.id
            omAuthorizedPropertyTypes = edmService.getPropertyTypesAsMap(om.properties)

        }
        if (!this::datasetEntityTypeId.isInitialized) {
            val ds = edmService.getEntityType(DATASETS_ET)
            datasetEntityTypeId = ds.id
            datasetsAuthorizedPropertyTypes = edmService.getPropertyTypesAsMap(ds.properties)
        }
        if (!this::columnsEntityTypeId.isInitialized) {
            val c = edmService.getEntityType(COLUMNS_ET)
            columnsEntityTypeId = c.id
            columnAuthorizedPropertyTypes = edmService.getPropertyTypesAsMap(c.properties)
        }
        if (!this::propertyTypes.isInitialized) {
            propertyTypes = (omAuthorizedPropertyTypes.values + datasetsAuthorizedPropertyTypes.values + columnAuthorizedPropertyTypes.values)
                    .associateBy { it.type.fullQualifiedNameAsString }
        }
    }

    fun isFullyInitialized(): Boolean = this::organizationMetadataEntityTypeId.isInitialized
            && this::datasetEntityTypeId.isInitialized
            && this::columnsEntityTypeId.isInitialized
            && this::omAuthorizedPropertyTypes.isInitialized
            && this::datasetsAuthorizedPropertyTypes.isInitialized
            && this::columnAuthorizedPropertyTypes.isInitialized
            && this::propertyTypes.isInitialized

    fun initializeOrganizationMetadataEntitySets(organizationId: UUID) {
        val adminRoleAclKey = organizations.executeOnKey(
            organizationId,
            OrganizationReadEntryProcessor { it.adminRoleAclKey }
        ) as AclKey

        initializeOrganizationMetadataEntitySets(principalsMapManager.lookupRole(adminRoleAclKey))
    }

    fun ensureOrganizationMetadataEntitySetIdsFullyInitialized(organizationId: UUID) {
        val fullyInitialized = organizations.executeOnKey(
            organizationId,
            OrganizationReadEntryProcessor { it.organizationMetadataEntitySetIds.isInitialized }) as Boolean

        if (!fullyInitialized) {
            initializeOrganizationMetadataEntitySets(organizationId)
        }
    }

    fun initializeOrganizationMetadataEntitySets(adminRole: Role) {
        initializeFields()
        if (!isFullyInitialized()) {
            return
        }

        val organizationId = adminRole.organizationId

        val organizationMetadataEntitySetIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)
        val createdEntitySets = mutableSetOf<UUID>()
        val orgAclKeyGrants = mutableListOf<AclKey>()

        val organizationMetadataEntitySetId = if (organizationMetadataEntitySetIds.organization == UNINITIALIZED_METADATA_ENTITY_SET_ID) {
            val organizationMetadataEntitySet = buildOrganizationMetadataEntitySet(organizationId)
            val id = entitySetsManager.createEntitySet(adminRole.principal, organizationMetadataEntitySet)
            createdEntitySets.add(id)
            orgAclKeyGrants.add(AclKey(id))
            omAuthorizedPropertyTypes.keys.forEach { orgAclKeyGrants.add(AclKey(id, it)) }
            id
        } else {
            organizationMetadataEntitySetIds.organization
        }

        val datasetsEntitySetId = if (organizationMetadataEntitySetIds.datasets == UNINITIALIZED_METADATA_ENTITY_SET_ID) {
            val datasetsEntitySet = buildDatasetsEntitySet(organizationId)
            val id = entitySetsManager.createEntitySet(adminRole.principal, datasetsEntitySet)
            createdEntitySets.add(id)
            orgAclKeyGrants.add(AclKey(id))
            datasetsAuthorizedPropertyTypes.keys.forEach { orgAclKeyGrants.add(AclKey(id, it)) }
            id
        } else {
            organizationMetadataEntitySetIds.datasets
        }

        val columnsEntitySetId = if (organizationMetadataEntitySetIds.columns == UNINITIALIZED_METADATA_ENTITY_SET_ID) {
            val columnsEntitySet = buildColumnEntitySet(organizationId)
            val id = entitySetsManager.createEntitySet(adminRole.principal, columnsEntitySet)
            createdEntitySets.add(id)
            orgAclKeyGrants.add(AclKey(id))
            columnAuthorizedPropertyTypes.keys.forEach { orgAclKeyGrants.add(AclKey(id, it)) }
            id
        } else {
            organizationMetadataEntitySetIds.columns
        }

        if (createdEntitySets.isNotEmpty()) {
            organizationService.setOrganizationMetadataEntitySetIds(
                    organizationId,
                    OrganizationMetadataEntitySetIds(
                            organizationMetadataEntitySetId,
                            datasetsEntitySetId,
                            columnsEntitySetId
                    )
            )

            entitySetsManager.getEntitySetsAsMap(createdEntitySets).values.forEach {
                entitySetsManager.setupOrganizationMetadataAndAuditEntitySets(it)
            }

            val orgPrincipal = organizationService.getOrganizationPrincipal(organizationId)!!.principal
            val orgPrincipalAce = Ace(orgPrincipal, EnumSet.of(Permission.READ))

            authorizationManager.addPermissions(orgAclKeyGrants.map { Acl(it, setOf(orgPrincipalAce)) })
        }
    }

    fun addDatasetsAndColumns(
        entitySets: Collection<EntitySet>,
        propertyTypesByEntitySet: Map<UUID, Collection<PropertyType>>
    ) {
        initializeFields()
        if (!isFullyInitialized() || entitySets.isEmpty()) {
            return
        }

        entitySets.groupBy { it.organizationId }.forEach { (organizationId, orgEntitySets) ->
            ensureOrganizationMetadataEntitySetIdsFullyInitialized(organizationId)
            val organizationMetadataEntitySetIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)
            val datasetEntityKeyIds = getDatasetEntityKeyIds(
                organizationMetadataEntitySetIds,
                orgEntitySets.map { it.id }
            )
            val columnEntityKeyIds = getColumnEntityKeyIds(
                    organizationMetadataEntitySetIds,
                    entitySets.associate {
                        it.id to propertyTypesByEntitySet.getOrDefault(it.id, listOf()).map { pt -> pt.id }
                    }
            )

            val datasetEntities = mutableMapOf<UUID, MutableMap<UUID, Set<Any>>>()
            val columnEntities = mutableMapOf<UUID, MutableMap<UUID, Set<Any>>>()

            orgEntitySets.forEach { entitySet ->
                val entitySetPropertyTypeEntities = propertyTypesByEntitySet.getOrDefault(entitySet.id, listOf())
                    .withIndex()
                    .associate { (index, propertyType) ->
                        columnEntityKeyIds.getValue(
                            AclKey(entitySet.id, propertyType.id)
                        ) to buildColumnEntity(organizationId, entitySet, propertyType, index)
                }
                columnEntities.putAll(entitySetPropertyTypeEntities)

                val datasetEKID = datasetEntityKeyIds.getValue(entitySet.id)
                datasetEntities[datasetEKID] = buildDatasetEntity(organizationId, entitySet)
            }

            dataGraphManager.partialReplaceEntities(
                    organizationMetadataEntitySetIds.datasets,
                    datasetEntities,
                    datasetsAuthorizedPropertyTypes
            )

            dataGraphManager.partialReplaceEntities(
                    organizationMetadataEntitySetIds.columns,
                    columnEntities,
                    columnAuthorizedPropertyTypes
            )
        }
    }

    fun addDatasetsAndColumns(
            organizationId: UUID,
            tables: Collection<ExternalTable>,
            columnsByTableId: Map<UUID, Collection<ExternalColumn>>
    ) {
        initializeFields()
        if (!isFullyInitialized() || tables.isEmpty()) {
            return
        }
        ensureOrganizationMetadataEntitySetIdsFullyInitialized(organizationId)
        val organizationMetadataEntitySetIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)
        val datasetEntityKeyIds = getDatasetEntityKeyIds(
            organizationMetadataEntitySetIds,
            tables.map { it.id }
        )
        val columnEntityKeyIds = getColumnEntityKeyIds(
                organizationMetadataEntitySetIds,
                tables.associate { it.id to columnsByTableId.getOrDefault(it.id, listOf()).map { c -> c.id } }
        )

        val datasetEntities = mutableMapOf<UUID, MutableMap<UUID, Set<Any>>>()
        val columnEntities = mutableMapOf<UUID, MutableMap<UUID, Set<Any>>>()

        tables.forEach { table ->
            val tableColumnEntities = columnsByTableId
                .getOrDefault(table.id, listOf())
                .associate { column ->
                    columnEntityKeyIds.getValue(AclKey(table.id, column.id)) to buildColumnEntity(
                        organizationId,
                        table,
                        column
                    )
                }
            columnEntities.putAll(tableColumnEntities)
            datasetEntities[datasetEntityKeyIds.getValue(table.id)] = buildDatasetEntity(organizationId, table)
        }

        dataGraphManager.partialReplaceEntities(
                organizationMetadataEntitySetIds.datasets,
                datasetEntities,
                datasetsAuthorizedPropertyTypes
        )

        dataGraphManager.partialReplaceEntities(
                organizationMetadataEntitySetIds.columns,
                columnEntities,
                columnAuthorizedPropertyTypes
        )
    }

    fun addDataset(organizationId: UUID, table: ExternalTable) {
        initializeFields()
        if (!isFullyInitialized()) {
            return
        }
        ensureOrganizationMetadataEntitySetIdsFullyInitialized(organizationId)
        val organizationMetadataEntitySetIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)

        val datasetEntity = buildDatasetEntity(organizationId, table)
        val datasetEntityKeyId = getDatasetEntityKeyId(organizationMetadataEntitySetIds, table.id)

        dataGraphManager.partialReplaceEntities(
                organizationMetadataEntitySetIds.datasets,
                mapOf(datasetEntityKeyId to datasetEntity),
                datasetsAuthorizedPropertyTypes
        )
    }

    fun addDatasetColumns(
            organizationId: UUID,
            table: ExternalTable,
            columns: Collection<ExternalColumn>
    ) {
        initializeFields()
        if (!isFullyInitialized()) {
            return
        }
        ensureOrganizationMetadataEntitySetIdsFullyInitialized(organizationId)
        val organizationMetadataEntitySetIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)

        val columnEntities = columns.associate { column ->
            getColumnEntityKeyId(organizationMetadataEntitySetIds, column.tableId, column) to buildColumnEntity(
                organizationId,
                table,
                column
            )
        }

        dataGraphManager.partialReplaceEntities(
                organizationMetadataEntitySetIds.columns,
                columnEntities,
                columnAuthorizedPropertyTypes
        )
    }

    fun deleteDatasets(organizationId: UUID, tableIdsToDelete: Set<UUID>) {

        initializeFields()
        if (!isFullyInitialized() || tableIdsToDelete.isEmpty()) {
            return
        }

        ensureOrganizationMetadataEntitySetIdsFullyInitialized(organizationId)

        val metadataIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)
        val dataSetEntityKeyIds = getDatasetEntityKeyIds(metadataIds, tableIdsToDelete.toList())
        dataDeletionService.clearOrDeleteEntities(
            metadataIds.datasets,
            dataSetEntityKeyIds.values.toMutableSet(),
            DeleteType.Soft
        )
    }

    fun deleteDatasetColumns(organizationId: UUID, columnIdsToDelete: Map<UUID, Set<UUID>>) {

        initializeFields()
        if (!isFullyInitialized() || columnIdsToDelete.isEmpty()) {
            return
        }

        ensureOrganizationMetadataEntitySetIdsFullyInitialized(organizationId)

        val metadataIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)
        val dataSetColumnEntityKeyIds = getColumnEntityKeyIds(metadataIds, columnIdsToDelete)
        dataDeletionService.clearOrDeleteEntities(
            metadataIds.columns,
            dataSetColumnEntityKeyIds.values.toMutableSet(),
            DeleteType.Soft
        )
    }

    private fun getDatasetEntityKeyId(
            organizationMetadataEntitySetIds: OrganizationMetadataEntitySetIds,
            datasetId: UUID
    ): UUID {
        return getDatasetEntityKeyIds(organizationMetadataEntitySetIds, listOf(datasetId)).getValue(datasetId)
    }

    private fun getDatasetEntityKeyIds(
            metadataIds: OrganizationMetadataEntitySetIds,
            datasetIds: List<UUID>
    ): Map<UUID, UUID> {
        val entityKeys = datasetIds.map { EntityKey(metadataIds.datasets, it.toString()) }.toSet()
        val entityKeyIds = dataGraphManager.getEntityKeyIds(entityKeys)
        return datasetIds.zip(entityKeyIds).toMap()
    }

    private fun getColumnEntityKeyId(
            organizationMetadataEntitySetIds: OrganizationMetadataEntitySetIds,
            datasetId: UUID,
            column: AbstractSecurableObject
    ): UUID {
        return getColumnEntityKeyIds(organizationMetadataEntitySetIds, mapOf(datasetId to listOf(column.id)))
            .getValue(AclKey(datasetId, column.id))
    }

    private fun getColumnEntityKeyIds(
            organizationMetadataEntitySetIds: OrganizationMetadataEntitySetIds,
            datasetIdToColumnIds: Map<UUID, Collection<UUID>>
    ): Map<AclKey, UUID> {

        val aclKeys = datasetIdToColumnIds.flatMap { it.value.map { colId -> AclKey(it.key, colId) } }

        val entityKeyIds = dataGraphManager.getEntityKeyIds(
                aclKeys.map { EntityKey(organizationMetadataEntitySetIds.columns, "${it[0]}.${it[1]}") }.toSet()
        )

        return aclKeys.zip(entityKeyIds).toMap()
    }

    private fun buildOrganizationMetadataEntitySet(organizationId: UUID): EntitySet = EntitySet(
            organizationId = organizationId,
            entityTypeId = organizationMetadataEntityTypeId,
            name = buildOrganizationMetadataEntitySetName(organizationId),
            _title = "Organization metadata for $organizationId",
            _description = "Organization metadata for $organizationId",
            contacts = mutableSetOf(),
            flags = EnumSet.of(EntitySetFlag.METADATA)
    )

    private fun buildDatasetsEntitySet(organizationId: UUID): EntitySet = EntitySet(
            organizationId = organizationId,
            entityTypeId = datasetEntityTypeId,
            name = buildDatasetsEntitySetName(organizationId),
            _title = "Datasets for $organizationId",
            _description = "Datasets for $organizationId",
            contacts = mutableSetOf(),
            flags = EnumSet.of(EntitySetFlag.METADATA)
    )

    private fun buildColumnEntitySet(organizationId: UUID): EntitySet = EntitySet(
            organizationId = organizationId,
            entityTypeId = columnsEntityTypeId,
            name = buildColumnEntitySetName(organizationId),
            _title = "Dataset columns for $organizationId",
            _description = "Dataset columns for $organizationId",
            contacts = mutableSetOf(),
            flags = EnumSet.of(EntitySetFlag.METADATA)
    )

    private fun buildOrganizationMetadataEntitySetName(organizationId: UUID): String = "org-metadata-$organizationId"
    private fun buildDatasetsEntitySetName(organizationId: UUID): String = "datasets-$organizationId"
    private fun buildColumnEntitySetName(organizationId: UUID): String = "columns-$organizationId"

    private fun buildDatasetEntity(
            organizationId: UUID,
            table: ExternalTable
    ): MutableMap<UUID, Set<Any>> {
        return mutableMapOf(
            propertyTypes.getValue(DATASET_NAME).id to setOf(table.name),
            propertyTypes.getValue(DESCRIPTION).id to setOf(table.description),
            propertyTypes.getValue(ID).id to setOf(table.id.toString()),
            propertyTypes.getValue(ORG_ID).id to setOf(organizationId),
            propertyTypes.getValue(PG_OID).id to setOf(table.oid),
            propertyTypes.getValue(STANDARDIZED).id to setOf(false),
            propertyTypes.getValue(TITLE).id to setOf(table.title)
        )
    }

    private fun buildDatasetEntity(
        organizationId: UUID,
        entitySet: EntitySet
    ): MutableMap<UUID, Set<Any>> {
        return mutableMapOf(
            propertyTypes.getValue(CONTACT).id to entitySet.contacts,
            propertyTypes.getValue(DATASET_NAME).id to setOf(entitySet.name),
            propertyTypes.getValue(DESCRIPTION).id to setOf(entitySet.description),
            propertyTypes.getValue(FLAGS).id to entitySet.flags.map { flag -> flag.name }.toSet(),
            propertyTypes.getValue(ID).id to setOf(entitySet.id.toString()),
            propertyTypes.getValue(ORG_ID).id to setOf(organizationId),
            propertyTypes.getValue(STANDARDIZED).id to setOf(true),
            propertyTypes.getValue(TITLE).id to setOf(entitySet.title)
        )
    }

    private fun buildColumnEntity(
            organizationId: UUID,
            table: ExternalTable,
            column: ExternalColumn
    ): MutableMap<UUID, Set<Any>> {
        return mutableMapOf(
            propertyTypes.getValue(COL_NAME).id to setOf(column.name),
            propertyTypes.getValue(DATASET_ID).id to setOf(table.id),
            propertyTypes.getValue(DATASET_NAME).id to setOf(table.name),
            propertyTypes.getValue(DATA_TYPE).id to setOf(column.dataType.toString()),
            propertyTypes.getValue(DESCRIPTION).id to setOf(column.description),
            propertyTypes.getValue(ID).id to setOf(column.id.toString()),
            propertyTypes.getValue(INDEX).id to setOf(column.ordinalPosition),
            propertyTypes.getValue(ORG_ID).id to setOf(organizationId),
            propertyTypes.getValue(TITLE).id to setOf(column.title)
        )
    }
    private fun buildColumnEntity(
        organizationId: UUID,
        entitySet: EntitySet,
        propertyType: PropertyType,
        index: Int
    ): MutableMap<UUID, Set<Any>> {
        return mutableMapOf(
            propertyTypes.getValue(COL_NAME).id to setOf(propertyType.type.toString()),
            propertyTypes.getValue(DATASET_ID).id to setOf(entitySet.id),
            propertyTypes.getValue(DATASET_NAME).id to setOf(entitySet.name),
            propertyTypes.getValue(DATA_TYPE).id to setOf(propertyType.datatype.toString()),
            propertyTypes.getValue(DESCRIPTION).id to setOf(propertyType.description),
            propertyTypes.getValue(ID).id to setOf(propertyType.id.toString()),
            propertyTypes.getValue(INDEX).id to setOf(index),
            propertyTypes.getValue(ORG_ID).id to setOf(organizationId),
            propertyTypes.getValue(TITLE).id to setOf(propertyType.title),
            propertyTypes.getValue(TYPE).id to setOf(propertyType.type.toString())
        )
    }

    companion object {
        private val ORGANIZATION_METADATA_ET = FullQualifiedName("ol.organization_metadata")
        private val DATASETS_ET = FullQualifiedName("ol.dataset")
        private val COLUMNS_ET = FullQualifiedName("ol.column")

        private const val COL_NAME = "ol.column_name"
        private const val CONTACT = "contact.Email"
        private const val DATASET_ID = "ol.datasetid"
        private const val DATASET_NAME = "ol.dataset_name"
        private const val DATA_TYPE = "ol.datatype"
        private const val DESCRIPTION = "ol.description"
        private const val FLAGS = "ol.flags"
        private const val ID = "ol.id"
        private const val INDEX = "ol.index"
        private const val ORG_ID = "ol.organization_id"
        private const val PG_OID = "ol.pgoid"
        private const val STANDARDIZED = "ol.standardized"
        private const val TITLE = "ol.title"
        private const val TYPE = "ol.type"
    }
}
