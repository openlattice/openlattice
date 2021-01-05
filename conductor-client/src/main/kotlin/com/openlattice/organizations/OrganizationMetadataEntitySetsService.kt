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

import com.dataloom.mappers.ObjectMappers
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.data.DataGraphManager
import com.openlattice.data.EntityKey
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.PropertyType
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organization.roles.Role
import com.openlattice.postgres.DataTables.quote
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.stereotype.Service
import java.util.*

private val ORGANIZATION_METADATA_ET = FullQualifiedName("ol.organization_metadata")
private val DATASETS_ET = FullQualifiedName("ol.dataset")
private val COLUMNS_ET = FullQualifiedName("ol.column")

private const val PGOID = "ol.pgoid"
private const val ID = "ol.id"
private const val COL_INFO = "ol.columninfo"
private const val DATASET_NAME = "ol.dataset_name"
private const val ORG_ID = "ol.organization_id"
private const val STANDARDIZED = "ol.standardized"
private const val TYPE = "ol.type"
private const val COL_NAME = "ol.column_name"
private const val CONTACT = "contact.Email"
private const val DESCRIPTION = "ol.description"


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Service
class OrganizationMetadataEntitySetsService(
        private val edmService: EdmManager,
        private val authorizationManager: AuthorizationManager
) {
    private val mapper = ObjectMappers.newJsonMapper()


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

    fun isFullyInitialized(): Boolean = this::organizationMetadataEntityTypeId.isInitialized &&
            this::datasetEntityTypeId.isInitialized && this::columnsEntityTypeId.isInitialized &&
            this::omAuthorizedPropertyTypes.isInitialized && this::datasetsAuthorizedPropertyTypes.isInitialized &&
            this::columnAuthorizedPropertyTypes.isInitialized && this::propertyTypes.isInitialized

    fun initializeOrganizationMetadataEntitySets(adminRole: Role) {
        initializeFields()
        if (!isFullyInitialized()) {
            return
        }

        val organizationId = adminRole.organizationId

        val organizationMetadataEntitySetIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)
        val createdEntitySets = mutableSetOf<UUID>()

        val organizationMetadataEntitySetId = if (organizationMetadataEntitySetIds.organization == UNINITIALIZED_METADATA_ENTITY_SET_ID) {
            val organizationMetadataEntitySet = buildOrganizationMetadataEntitySet(organizationId)
            val id = entitySetsManager.createEntitySet(adminRole.principal, organizationMetadataEntitySet)
            createdEntitySets.add(id)
            id
        } else {
            organizationMetadataEntitySetIds.organization
        }

        val datasetsEntitySetId = if (organizationMetadataEntitySetIds.datasets == UNINITIALIZED_METADATA_ENTITY_SET_ID) {
            val datasetsEntitySet = buildDatasetsEntitySet(organizationId)
            val id = entitySetsManager.createEntitySet(adminRole.principal, datasetsEntitySet)
            createdEntitySets.add(id)
            id
        } else {
            organizationMetadataEntitySetIds.datasets
        }

        val columnsEntitySetId = if (organizationMetadataEntitySetIds.columns == UNINITIALIZED_METADATA_ENTITY_SET_ID) {
            val columnsEntitySet = buildColumnEntitySet(organizationId)
            val id = entitySetsManager.createEntitySet(adminRole.principal, columnsEntitySet)
            createdEntitySets.add(id)
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

            authorizationManager.addPermissions(createdEntitySets.map { Acl(AclKey(it), setOf(orgPrincipalAce)) })
        }
    }

    fun addDatasetsAndColumns(entitySets: Collection<EntitySet>, propertyTypesByEntitySet: Map<UUID, Collection<PropertyType>>) {
        initializeFields()
        if (!isFullyInitialized() || entitySets.isEmpty()) {
            return
        }

        entitySets.groupBy { it.organizationId }.forEach { (organizationId, orgEntitySets) ->
            val organizationMetadataEntitySetIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)

            val datasetEntityKeyIds = getDatasetEntityKeyIds(organizationMetadataEntitySetIds, orgEntitySets.map { it.id })
            val columnEntityKeyIds = getColumnEntityKeyIds(organizationMetadataEntitySetIds, propertyTypesByEntitySet.mapValues { it.value.map { pt -> pt.id } })

            val datasetEntities = mutableMapOf<UUID, MutableMap<UUID, Set<Any>>>()
            val columnEntities = mutableMapOf<UUID, MutableMap<UUID, Set<Any>>>()

            orgEntitySets.forEach { entitySet ->

                val entitySetPropertyTypeEntities = propertyTypesByEntitySet.getOrDefault(entitySet.id, listOf()).associate { propertyType ->
                    columnEntityKeyIds.getValue(AclKey(entitySet.id, propertyType.id)) to mutableMapOf<UUID, Set<Any>>(
                            propertyTypes.getValue(ID).id to setOf(propertyType.id.toString()),
                            propertyTypes.getValue(DATASET_NAME).id to setOf(entitySet.name),
                            propertyTypes.getValue(COL_NAME).id to setOf(propertyType.type.fullQualifiedNameAsString),
                            propertyTypes.getValue(ORG_ID).id to setOf(organizationId.toString()),
                            propertyTypes.getValue(TYPE).id to setOf(propertyType.datatype.toString()),
                            propertyTypes.getValue(DESCRIPTION).id to setOf(propertyType.description)
                    )
                }

                columnEntities.putAll(entitySetPropertyTypeEntities)

                datasetEntities[datasetEntityKeyIds.getValue(entitySet.id)] = mutableMapOf(
                        propertyTypes.getValue(ID).id to setOf(entitySet.id.toString()),
                        propertyTypes.getValue(DATASET_NAME).id to setOf(entitySet.name),
                        propertyTypes.getValue(CONTACT).id to entitySet.contacts,
                        propertyTypes.getValue(STANDARDIZED).id to setOf(true),
                        propertyTypes.getValue(COL_INFO).id to setOf(mapper.writeValueAsString(entitySetPropertyTypeEntities.values))
                )

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
            tables: Collection<OrganizationExternalDatabaseTable>,
            columnsByTableId: Map<UUID, Collection<OrganizationExternalDatabaseColumn>>
    ) {
        initializeFields()
        if (!isFullyInitialized() || tables.isEmpty()) {
            return
        }

        val organizationMetadataEntitySetIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)

        val datasetEntityKeyIds = getDatasetEntityKeyIds(organizationMetadataEntitySetIds, tables.map { it.id })
        val columnEntityKeyIds = getColumnEntityKeyIds(organizationMetadataEntitySetIds, columnsByTableId.mapValues { it.value.map { c -> c.id } })

        val datasetEntities = mutableMapOf<UUID, MutableMap<UUID, Set<Any>>>()
        val columnEntities = mutableMapOf<UUID, MutableMap<UUID, Set<Any>>>()

        tables.forEach { table ->

            val tableColumnEntities = columnsByTableId.getOrDefault(table.id, listOf()).associate { column ->
                columnEntityKeyIds.getValue(AclKey(table.id, column.id)) to mutableMapOf<UUID, Set<Any>>(
                        propertyTypes.getValue(ID).id to setOf(column.id.toString()),
                        propertyTypes.getValue(DATASET_NAME).id to setOf(table.name),
                        propertyTypes.getValue(COL_NAME).id to setOf(column.name),
                        propertyTypes.getValue(ORG_ID).id to setOf(organizationId.toString()),
                        propertyTypes.getValue(TYPE).id to setOf(column.dataType.toString()),
                        propertyTypes.getValue(DESCRIPTION).id to setOf(column.description)
                )
            }

            columnEntities.putAll(tableColumnEntities)

            datasetEntities[datasetEntityKeyIds.getValue(table.id)] = mutableMapOf(
                    propertyTypes.getValue(PGOID).id to setOf(table.oid),
                    propertyTypes.getValue(ID).id to setOf(table.id.toString()),
                    propertyTypes.getValue(DATASET_NAME).id to setOf(table.name),
                    propertyTypes.getValue(STANDARDIZED).id to setOf(false),
                    propertyTypes.getValue(COL_INFO).id to setOf(mapper.writeValueAsString(tableColumnEntities.values))
            )

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

    fun addDataset(organizationId: UUID, table: OrganizationExternalDatabaseTable) {
        initializeFields()
        if (!isFullyInitialized()) {
            return
        }
        val organizationMetadataEntitySetIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)

        val datasetEntity = mutableMapOf<UUID, Set<Any>>(
                propertyTypes.getValue(PGOID).id to setOf(table.oid),
                propertyTypes.getValue(ID).id to setOf(table.id.toString()),
                propertyTypes.getValue(DATASET_NAME).id to setOf(table.name),
                propertyTypes.getValue(STANDARDIZED).id to setOf(false)
        )

        val datasetEntityKeyId = getDatasetEntityKeyId(organizationMetadataEntitySetIds, table.oid)

        dataGraphManager.partialReplaceEntities(
                organizationMetadataEntitySetIds.datasets,
                mapOf(datasetEntityKeyId to datasetEntity),
                datasetsAuthorizedPropertyTypes
        )
    }

    fun addDatasetColumns(organizationId: UUID, table: OrganizationExternalDatabaseTable, columns: Collection<OrganizationExternalDatabaseColumn>) {
        initializeFields()
        if (!isFullyInitialized()) {
            return
        }
        val organizationMetadataEntitySetIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)

        val columnEntities = columns.associate { column ->
            getColumnEntityKeyId(organizationMetadataEntitySetIds, column.tableId, column) to mutableMapOf<UUID, Set<Any>>(
                    propertyTypes.getValue(ID).id to setOf(column.id.toString()),
                    propertyTypes.getValue(DATASET_NAME).id to setOf(table.name),
                    propertyTypes.getValue(COL_NAME).id to setOf(column.name),
                    propertyTypes.getValue(ORG_ID).id to setOf(organizationId.toString()),
                    propertyTypes.getValue(TYPE).id to setOf(column.dataType.toString()),
                    propertyTypes.getValue(DESCRIPTION).id to setOf(column.description)
            )
        }

        dataGraphManager.partialReplaceEntities(
                organizationMetadataEntitySetIds.columns,
                columnEntities,
                columnAuthorizedPropertyTypes
        )

        val datasetEntityKeyId = getDatasetEntityKeyId(organizationMetadataEntitySetIds, table.id)
        val datasetColumnEntity = mutableMapOf<UUID, Set<Any>>(
                propertyTypes.getValue(COL_INFO).id to setOf(mapper.writeValueAsString(columnEntities))
        )

        dataGraphManager.partialReplaceEntities(
                organizationMetadataEntitySetIds.datasets,
                mapOf(datasetEntityKeyId to datasetColumnEntity),
                datasetsAuthorizedPropertyTypes
        )
    }

    private fun getDatasetEntityKeyId(
            organizationMetadataEntitySetIds: OrganizationMetadataEntitySetIds,
            datasetId: Any
    ): UUID {
        return getDatasetEntityKeyIds(organizationMetadataEntitySetIds, listOf(datasetId)).getValue(datasetId)
    }

    private fun getDatasetEntityKeyIds(
            organizationMetadataEntitySetIds: OrganizationMetadataEntitySetIds,
            datasetIds: List<Any>
    ): Map<Any, UUID> {
        val entityKeyIds = dataGraphManager.getEntityKeyIds(
                datasetIds.map { EntityKey(organizationMetadataEntitySetIds.datasets, it.toString()) }.toSet()
        )

        return datasetIds.zip(entityKeyIds).toMap()

    }

    private fun getColumnEntityKeyId(
            organizationMetadataEntitySetIds: OrganizationMetadataEntitySetIds,
            datasetId: UUID,
            column: AbstractSecurableObject

    ): UUID {
        return getColumnEntityKeyIds(organizationMetadataEntitySetIds, mapOf(datasetId to listOf(column.id))).getValue(AclKey(datasetId, column.id))
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
            _title = "Organization Metadata for $organizationId",
            _description = "Organization Metadata for $organizationId",
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
            _title = "Datasets for $organizationId",
            _description = "Datasets for $organizationId",
            contacts = mutableSetOf(),
            flags = EnumSet.of(EntitySetFlag.METADATA)
    )

    private fun buildOrganizationMetadataEntitySetName(organizationId: UUID): String = quote(
            "org-metadata-$organizationId"
    )

    private fun buildDatasetsEntitySetName(organizationId: UUID): String = quote("datasets-$organizationId")
    private fun buildColumnEntitySetName(organizationId: UUID): String = quote("column-$organizationId")
}