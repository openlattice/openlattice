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
import com.openlattice.data.DataGraphManager
import com.openlattice.data.EntityKey
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
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
class OrganizationMetadataEntitySetsService(private val edmService: EdmManager) {
    private val mapper = ObjectMappers.newJsonMapper()


    lateinit var dataGraphManager: DataGraphManager
    lateinit var entitySetsManager: EntitySetManager
    lateinit var organizationService: HazelcastOrganizationService


    private lateinit var organizationMetadataEntityTypeId: UUID
    private lateinit var datasetEntityTypeId: UUID
    private lateinit var columnsEntityTypeId: UUID
    private lateinit var omAuthorizedPropertyTypes: Map<UUID, PropertyType>
    private lateinit var datasetsAuthorizedPropertTypes: Map<UUID, PropertyType>
    private lateinit var columnAuthorizedPropertTypes: Map<UUID, PropertyType>
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
            datasetsAuthorizedPropertTypes = edmService.getPropertyTypesAsMap(ds.properties)
        }
        if (!this::columnsEntityTypeId.isInitialized) {
            val c = edmService.getEntityType(COLUMNS_ET)
            columnsEntityTypeId = c.id
            columnAuthorizedPropertTypes = edmService.getPropertyTypesAsMap(c.properties)
        }
        if (!this::propertyTypes.isInitialized) {
            propertyTypes = (omAuthorizedPropertyTypes.values + datasetsAuthorizedPropertTypes.values + columnAuthorizedPropertTypes.values)
                    .associateBy { it.type.fullQualifiedNameAsString }
        }
    }

    fun isFullyInitialized(): Boolean = this::organizationMetadataEntityTypeId.isInitialized &&
            this::datasetEntityTypeId.isInitialized && this::columnsEntityTypeId.isInitialized &&
            this::omAuthorizedPropertyTypes.isInitialized && this::datasetsAuthorizedPropertTypes.isInitialized &&
            this::columnAuthorizedPropertTypes.isInitialized && this::propertyTypes.isInitialized

    fun initializeOrganizationMetadataEntitySets(organizationId: UUID) {
        initializeFields()
        if (!isFullyInitialized()) {
            return
        }
        val organizationMetadataEntitySetIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)
        val organizationPrincipal = organizationService.getOrganizationPrincipal(organizationId)!!
        var updated = false

        val organizationMetadataEntitySetId = if (organizationMetadataEntitySetIds.organization == UNINITIALIZED_METADATA_ENTITY_SET_ID) {
            updated = true
            val organizationMetadataEntitySet = buildOrganizationMetadataEntitySet(organizationId)
            entitySetsManager.createEntitySet(organizationPrincipal.principal, organizationMetadataEntitySet)
        } else {
            organizationMetadataEntitySetIds.organization
        }

        val datasetsEntitySetId = if (organizationMetadataEntitySetIds.datasets == UNINITIALIZED_METADATA_ENTITY_SET_ID) {
            updated = true
            val datasetsEntitySet = buildDatasetsEntitySet(organizationId)
            entitySetsManager.createEntitySet(organizationPrincipal.principal, datasetsEntitySet)
        } else {
            organizationMetadataEntitySetIds.datasets
        }

        val columnsEntitySetId = if (organizationMetadataEntitySetIds.columns == UNINITIALIZED_METADATA_ENTITY_SET_ID) {
            updated = true
            val columnsEntitySet = buildColumnEntitySet(organizationId)
            entitySetsManager.createEntitySet(organizationPrincipal.principal, columnsEntitySet)
        } else {
            organizationMetadataEntitySetIds.columns
        }

        if (updated) {
            organizationService.setOrganizationMetadataEntitySetIds(
                    organizationId,
                    OrganizationMetadataEntitySetIds(
                            organizationMetadataEntitySetId,
                            datasetsEntitySetId,
                            columnsEntitySetId
                    )
            )
        }
    }

    fun addDataset(entitySet: EntitySet) {
        initializeFields()
        if (!isFullyInitialized()) {
            return
        }
        val organizationId = entitySet.organizationId
        val organizationMetadataEntitySetIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)
        val datasetEntity = mutableMapOf<UUID, Set<Any>>(
                propertyTypes.getValue(ID).id to setOf(entitySet.id.toString()),
                propertyTypes.getValue(DATASET_NAME).id to setOf(entitySet.name),
                propertyTypes.getValue(CONTACT).id to entitySet.contacts,
                propertyTypes.getValue(STANDARDIZED).id to setOf(true)
        )

        val datasetEntityKeyId = dataGraphManager.getEntityKeyIds(
                setOf(EntityKey(organizationMetadataEntitySetIds.datasets, entitySet.id.toString()))
        ).first()


        dataGraphManager.partialReplaceEntities(
                organizationMetadataEntitySetIds.datasets,
                mapOf(datasetEntityKeyId to datasetEntity),
                datasetsAuthorizedPropertTypes
        )
    }

    fun addDataset(organizationId: UUID, table: OrganizationExternalDatabaseTable) {
        addDataset(organizationId, table.oid, table.id, table.name)
    }

    fun addDataset(organizationId: UUID, oid: Int, id: UUID, name: String) {
        initializeFields()
        if (!isFullyInitialized()) {
            return
        }
        val organizationMetadataEntitySetIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)

        val datasetEntity = mutableMapOf<UUID, Set<Any>>(
                propertyTypes.getValue(PGOID).id to setOf(oid),
                propertyTypes.getValue(ID).id to setOf(id.toString()),
                propertyTypes.getValue(DATASET_NAME).id to setOf(name),
                propertyTypes.getValue(STANDARDIZED).id to setOf(false)
        )

        val datasetEntityKeyId = dataGraphManager.getEntityKeyIds(
                setOf(EntityKey(organizationMetadataEntitySetIds.datasets, oid.toString()))
        ).first()

        dataGraphManager.partialReplaceEntities(
                organizationMetadataEntitySetIds.datasets,
                mapOf(datasetEntityKeyId to datasetEntity),
                datasetsAuthorizedPropertTypes
        )
    }

    fun addDatasetColumn(entitySet: EntitySet, propertyType: PropertyType) {
        initializeFields()
        if (!isFullyInitialized()) {
            return
        }
        val organizationId = entitySet.organizationId
        val organizationMetadataEntitySetIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)
        val columnEntity = mutableMapOf<UUID, Set<Any>>(
                propertyTypes.getValue(ID).id to setOf(propertyType.id.toString()),
                propertyTypes.getValue(DATASET_NAME).id to setOf(entitySet.name),
                propertyTypes.getValue(COL_NAME).id to setOf(propertyType.type.fullQualifiedNameAsString),
                propertyTypes.getValue(ORG_ID).id to setOf(organizationId.toString()),
                propertyTypes.getValue(TYPE).id to setOf(propertyType.datatype.toString()),
                propertyTypes.getValue(DESCRIPTION).id to setOf(propertyType.description)
        )
        val datasetColumnEntity = mutableMapOf<UUID, Set<Any>>(
                propertyTypes.getValue(COL_INFO).id to setOf(mapper.writeValueAsString(propertyType))
        )

        val columnEntityKeyId = dataGraphManager.getEntityKeyIds(
                setOf(EntityKey(organizationMetadataEntitySetIds.columns, "${entitySet.id}.${propertyType.id}"))
        ).first()

        val datasetEntityKeyId = dataGraphManager.getEntityKeyIds(
                setOf(EntityKey(organizationMetadataEntitySetIds.datasets, entitySet.id.toString()))
        ).first()


        dataGraphManager.partialReplaceEntities(
                organizationMetadataEntitySetIds.columns,
                mapOf(columnEntityKeyId to columnEntity),
                columnAuthorizedPropertTypes
        )

        dataGraphManager.partialReplaceEntities(
                organizationMetadataEntitySetIds.datasets,
                mapOf(datasetEntityKeyId to datasetColumnEntity),
                datasetsAuthorizedPropertTypes
        )
    }

    fun addDatasetColumn(organizationId: UUID, table: OrganizationExternalDatabaseTable, column: OrganizationExternalDatabaseColumn) {
        initializeFields()
        if (!isFullyInitialized()) {
            return
        }
        val organizationMetadataEntitySetIds = organizationService.getOrganizationMetadataEntitySetIds(organizationId)

        val columnEntity = mutableMapOf<UUID, Set<Any>>(
                propertyTypes.getValue(ID).id to setOf(column.id.toString()),
                propertyTypes.getValue(DATASET_NAME).id to setOf(table.name),
                propertyTypes.getValue(COL_NAME).id to setOf(column.name),
                propertyTypes.getValue(ORG_ID).id to setOf(organizationId.toString()),
                propertyTypes.getValue(TYPE).id to setOf(column.dataType.toString()),
                propertyTypes.getValue(DESCRIPTION).id to setOf(column.description)
        )

        val datasetColumnEntity = mutableMapOf<UUID, Set<Any>>(
                propertyTypes.getValue(COL_INFO).id to setOf(mapper.writeValueAsString(column))
        )

        val columnEntityKeyId = dataGraphManager.getEntityKeyIds(
                setOf(EntityKey(organizationMetadataEntitySetIds.columns, "${column.tableId}.${column.id}"))
        ).first()


        val datasetEntityKeyId = dataGraphManager.getEntityKeyIds(
                setOf(EntityKey(organizationMetadataEntitySetIds.datasets, table.id.toString())) //TODO: Consider using OID
        ).first()

        dataGraphManager.partialReplaceEntities(
                organizationMetadataEntitySetIds.columns,
                mapOf(columnEntityKeyId to columnEntity),
                columnAuthorizedPropertTypes
        )

        dataGraphManager.partialReplaceEntities(
                organizationMetadataEntitySetIds.datasets,
                mapOf(datasetEntityKeyId to datasetColumnEntity),
                datasetsAuthorizedPropertTypes
        )
    }

    private fun buildOrganizationMetadataEntitySet(organizationId: UUID): EntitySet = EntitySet(
            organizationId = organizationId,
            entityTypeId = organizationMetadataEntityTypeId,
            name = buildOrganizationMetadataEntitySetName(organizationId),
            _title = "Organization Metadata for $organizationId",
            _description = "Organization Metadata for $organizationId",
            contacts = mutableSetOf()
    )

    private fun buildDatasetsEntitySet(organizationId: UUID): EntitySet = EntitySet(
            organizationId = organizationId,
            entityTypeId = datasetEntityTypeId,
            name = buildDatasetsEntitySetName(organizationId),
            _title = "Datasets for $organizationId",
            _description = "Datasets for $organizationId",
            contacts = mutableSetOf()
    )

    private fun buildColumnEntitySet(organizationId: UUID): EntitySet = EntitySet(
            organizationId = organizationId,
            entityTypeId = columnsEntityTypeId,
            name = buildColumnEntitySetName(organizationId),
            _title = "Datasets for $organizationId",
            _description = "Datasets for $organizationId",
            contacts = mutableSetOf()
    )

    private fun buildOrganizationMetadataEntitySetName(organizationId: UUID): String = quote(
            "org-metadata-$organizationId"
    )

    private fun buildDatasetsEntitySetName(organizationId: UUID): String = quote("datasets-$organizationId")
    private fun buildColumnEntitySetName(organizationId: UUID): String = quote("column-$organizationId")
}