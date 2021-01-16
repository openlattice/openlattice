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
import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.AbstractSecurableObject
import com.openlattice.controllers.exceptions.ResourceNotFoundException
import com.openlattice.data.DataGraphManager
import com.openlattice.data.EntityKey
import com.openlattice.datastore.services.EdmManager
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.HazelcastOrganizationService.Companion.getOrganizationPrincipal
import com.openlattice.organizations.processors.OrganizationEntryProcessor
import com.openlattice.organizations.processors.OrganizationReadEntryProcessor
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.postgres.DataTables.quote
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.stereotype.Service
import java.util.*


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Service
class OrganizationEntitySetsService(
        hazelcastInstance: HazelcastInstance,
        private val edmService: EdmManager,
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val authorizationManager: AuthorizationManager
) {
    private val mapper = ObjectMappers.newJsonMapper()

    protected val organizations = HazelcastMap.ORGANIZATIONS.getMap(hazelcastInstance)

    lateinit var dataGraphManager: DataGraphManager
    lateinit var entitySetsManager: EntitySetManager

    private lateinit var organizationMetadataEntityTypeId: UUID
    private lateinit var datasetEntityTypeId: UUID
    private lateinit var columnsEntityTypeId: UUID
    private lateinit var schemaEntityTypeId: UUID
    private lateinit var viewEntityTypeId: UUID
    private lateinit var accessRequestsEntityTypeId: UUID
    private lateinit var omAuthorizedPropertyTypes: Map<UUID, PropertyType>
    private lateinit var datasetsAuthorizedPropertyTypes: Map<UUID, PropertyType>
    private lateinit var columnAuthorizedPropertyTypes: Map<UUID, PropertyType>
    private lateinit var schemaAuthorizedPropertyTypes: Map<UUID, PropertyType>
    private lateinit var viewAuthorizedPropertyTypes: Map<UUID, PropertyType>
    private lateinit var accessRequestAuthorizedPropertyTypes: Map<UUID, PropertyType>

    private lateinit var propertyTypes: Map<String, PropertyType>

    /**
     * This is done this way, so that service can startup even on an empty stack.
     *
     * As the stack takes dependencies on the EDM, we really should take a json export from prod that can be used to
     * initialize a stack and check it in to git.
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
        if (!this::schemaEntityTypeId.isInitialized) {
            val et = edmService.getEntityType(SCHEMAS_ET)
            schemaEntityTypeId = et.id
            schemaAuthorizedPropertyTypes = edmService.getPropertyTypesAsMap(et.properties)
        }
        if (!this::viewEntityTypeId.isInitialized) {
            val et = edmService.getEntityType(VIEWS_ET)
            viewEntityTypeId = et.id
            viewAuthorizedPropertyTypes = edmService.getPropertyTypesAsMap(et.properties)
        }
        if (!this::accessRequestsEntityTypeId.isInitialized) {
            val et = edmService.getEntityType(ACCESS_REQUEST_ET)
            accessRequestsEntityTypeId = et.id
            accessRequestAuthorizedPropertyTypes = edmService.getPropertyTypesAsMap(et.properties)
        }
        if (!this::propertyTypes.isInitialized) {
            propertyTypes = listOf(
                    omAuthorizedPropertyTypes.values,
                    datasetsAuthorizedPropertyTypes.values,
                    columnAuthorizedPropertyTypes.values,
                    schemaAuthorizedPropertyTypes.values,
                    viewAuthorizedPropertyTypes.values,
                    accessRequestAuthorizedPropertyTypes.values
            )
                    .flatten()
                    .associateBy { it.type.fullQualifiedNameAsString }
        }
    }

    fun isFullyInitialized(): Boolean = this::organizationMetadataEntityTypeId.isInitialized &&
            this::datasetEntityTypeId.isInitialized && this::columnsEntityTypeId.isInitialized &&
            this::omAuthorizedPropertyTypes.isInitialized && this::datasetsAuthorizedPropertyTypes.isInitialized &&
            this::columnAuthorizedPropertyTypes.isInitialized && this::propertyTypes.isInitialized

    fun initializeOrganizationMetadataEntitySets(organizationId: UUID) {
        val adminAclKey = organizations.executeOnKey(
                organizationId,
                OrganizationReadEntryProcessor { it.adminRoleAclKey }
        ) as AclKey

        initializeOrganizationMetadataEntitySets(securePrincipalsManager.getRole(organizationId, adminAclKey[1]))
    }

    fun ensureOrganizationMetadataEntitySetIdsFullyInitialized(organizationId: UUID) {
        val fullyInitialized = organizations
                .executeOnKey(organizationId, OrganizationReadEntryProcessor { org ->
                    with(org.organizationMetadataEntitySetIds) {
                        listOf(
                                columns,
                                datasets,
                                organization,
                                schemas,
                                views,
                                accessRequests
                        ).any { it == UNINITIALIZED_METADATA_ENTITY_SET_ID }
                    }
                }) as Boolean
        if( !fullyInitialized ) {
            initializeOrganizationMetadataEntitySets(organizationId)
        }
    }

    fun initializeOrganizationMetadataEntitySets(adminRole: Role) {
        initializeFields()
        if (!isFullyInitialized()) {
            return
        }

        val organizationId = adminRole.organizationId

        val organizationMetadataEntitySetIds = getOrganizationMetadataEntitySetIds(organizationId)
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

        val schemaEntitySetId = if (organizationMetadataEntitySetIds.schemas == UNINITIALIZED_METADATA_ENTITY_SET_ID) {
            val schemasEntitySet = buildColumnEntitySet(organizationId)
            val id = entitySetsManager.createEntitySet(adminRole.principal, schemasEntitySet)
            createdEntitySets.add(id)
            id
        } else {
            organizationMetadataEntitySetIds.schemas
        }

        val viewsEntitySetId = if (organizationMetadataEntitySetIds.views == UNINITIALIZED_METADATA_ENTITY_SET_ID) {
            val viewsEntitySet = buildViewsEntitySet(organizationId)
            val id = entitySetsManager.createEntitySet(adminRole.principal, viewsEntitySet)
            createdEntitySets.add(id)
            id
        } else {
            organizationMetadataEntitySetIds.views
        }

        val accessRequestsEntitySetId = if (organizationMetadataEntitySetIds.accessRequests == UNINITIALIZED_METADATA_ENTITY_SET_ID) {
            val accessRequestsEntitySet = buildViewsEntitySet(organizationId)
            val id = entitySetsManager.createEntitySet(adminRole.principal, accessRequestsEntitySet)
            createdEntitySets.add(id)
            id
        } else {
            organizationMetadataEntitySetIds.accessRequests
        }

        if (createdEntitySets.isNotEmpty()) {
            setOrganizationMetadataEntitySetIds(
                    organizationId,
                    OrganizationMetadataEntitySetIds(
                            organizationMetadataEntitySetId,
                            datasetsEntitySetId,
                            columnsEntitySetId,
                            schemaEntitySetId,
                            viewsEntitySetId,
                            accessRequestsEntitySetId
                    )
            )

            entitySetsManager.getEntitySetsAsMap(createdEntitySets).values.forEach {
                entitySetsManager.setupOrganizationMetadataAndAuditEntitySets(it)
            }

            val orgPrincipal = getOrganizationPrincipal(securePrincipalsManager, organizationId)!!.principal
            val orgPrincipalAce = Ace(orgPrincipal, EnumSet.of(Permission.READ))

            authorizationManager.addPermissions(createdEntitySets.map { Acl(AclKey(it), setOf(orgPrincipalAce)) })
        }
    }

    fun addDatasetsAndColumns(
            entitySets: Collection<EntitySet>, propertyTypesByEntitySet: Map<UUID, Collection<PropertyType>>
    ) {
        initializeFields()
        if (!isFullyInitialized() || entitySets.isEmpty()) {
            return
        }


        entitySets.groupBy { it.organizationId }.forEach { (organizationId, orgEntitySets) ->
            ensureOrganizationMetadataEntitySetIdsFullyInitialized(organizationId)
            val organizationMetadataEntitySetIds = getOrganizationMetadataEntitySetIds(organizationId)

            val datasetEntityKeyIds = getDatasetEntityKeyIds(
                    organizationMetadataEntitySetIds, orgEntitySets.map { it.id })
            val columnEntityKeyIds = getColumnEntityKeyIds(
                    organizationMetadataEntitySetIds,
                    entitySets.associate {
                        it.id to propertyTypesByEntitySet.getOrDefault(
                                it.id, listOf()
                        ).map { pt -> pt.id }
                    }
            )

            val datasetEntities = mutableMapOf<UUID, MutableMap<UUID, Set<Any>>>()
            val columnEntities = mutableMapOf<UUID, MutableMap<UUID, Set<Any>>>()

            orgEntitySets.forEach { entitySet ->

                val entitySetPropertyTypeEntities = propertyTypesByEntitySet.getOrDefault(
                        entitySet.id, listOf()
                ).associate { propertyType ->
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
                        propertyTypes.getValue(COL_INFO).id to setOf(
                                mapper.writeValueAsString(entitySetPropertyTypeEntities.values)
                        )
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
        ensureOrganizationMetadataEntitySetIdsFullyInitialized(organizationId)
        val organizationMetadataEntitySetIds = getOrganizationMetadataEntitySetIds(organizationId)

        val datasetEntityKeyIds = getDatasetEntityKeyIds(organizationMetadataEntitySetIds, tables.map { it.id })
        val columnEntityKeyIds = getColumnEntityKeyIds(
                organizationMetadataEntitySetIds,
                tables.associate { it.id to columnsByTableId.getOrDefault(it.id, listOf()).map { c -> c.id } }
        )

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
                    propertyTypes.getValue(EXTERNAL_ID).id to setOf(table.externalId),
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
        ensureOrganizationMetadataEntitySetIdsFullyInitialized(organizationId)
        val organizationMetadataEntitySetIds = getOrganizationMetadataEntitySetIds(organizationId)

        val datasetEntity = mutableMapOf<UUID, Set<Any>>(
                propertyTypes.getValue(EXTERNAL_ID).id to setOf(table.externalId),
                propertyTypes.getValue(ID).id to setOf(table.id.toString()),
                propertyTypes.getValue(DATASET_NAME).id to setOf(table.name),
                propertyTypes.getValue(STANDARDIZED).id to setOf(false)
        )

        val datasetEntityKeyId = getDatasetEntityKeyId(organizationMetadataEntitySetIds, table.externalId)

        dataGraphManager.partialReplaceEntities(
                organizationMetadataEntitySetIds.datasets,
                mapOf(datasetEntityKeyId to datasetEntity),
                datasetsAuthorizedPropertyTypes
        )
    }

    fun addDatasetColumns(
            organizationId: UUID,
            table: OrganizationExternalDatabaseTable,
            columns: Collection<OrganizationExternalDatabaseColumn>
    ) {
        initializeFields()
        if (!isFullyInitialized()) {
            return
        }

        ensureOrganizationMetadataEntitySetIdsFullyInitialized(organizationId)
        val organizationMetadataEntitySetIds = getOrganizationMetadataEntitySetIds(organizationId)

        val columnEntities = columns.associate { column ->
            getColumnEntityKeyId(
                    organizationMetadataEntitySetIds, column.tableId, column
            ) to mutableMapOf<UUID, Set<Any>>(
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

    fun getOrganizationMetadataEntitySetIds(organizationId: UUID): OrganizationMetadataEntitySetIds {
        return organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            OrganizationEntryProcessor.Result(it.organizationMetadataEntitySetIds, false)
        }) as OrganizationMetadataEntitySetIds? ?: throw ResourceNotFoundException(
                "Unable able to resolve organization $organizationId"
        )
    }

    fun setOrganizationMetadataEntitySetIds(
            organizationId: UUID,
            organizationMetadataEntitySetIds: OrganizationMetadataEntitySetIds
    ) {
        organizations.executeOnKey(organizationId, OrganizationEntryProcessor {
            it.organizationMetadataEntitySetIds = organizationMetadataEntitySetIds
            OrganizationEntryProcessor.Result(null, true)
        })
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
        return getColumnEntityKeyIds(organizationMetadataEntitySetIds, mapOf(datasetId to listOf(column.id))).getValue(
                AclKey(datasetId, column.id)
        )
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

    private fun buildSchemaEntitySet(organizationId: UUID): EntitySet = EntitySet(
            organizationId = organizationId,
            entityTypeId = schemaEntityTypeId,
            name = buildSchemasEntitySetName(organizationId),
            _title = "Schemas for $organizationId",
            _description = "Schemas for $organizationId",
            contacts = mutableSetOf(),
            flags = EnumSet.of(EntitySetFlag.METADATA)
    )

    private fun buildViewsEntitySet(organizationId: UUID): EntitySet = EntitySet(
            organizationId = organizationId,
            entityTypeId = viewEntityTypeId,
            name = buildViewsEntitySetName(organizationId),
            _title = "Views for $organizationId",
            _description = "Views for $organizationId",
            contacts = mutableSetOf(),
            flags = EnumSet.of(EntitySetFlag.METADATA)
    )

    private fun buildAccessRequestEntitySet(organizationId: UUID): EntitySet = EntitySet(
            organizationId = organizationId,
            entityTypeId = accessRequestsEntityTypeId,
            name = buildAccessRequestsEntitySetName(organizationId),
            _title = "Access Requests for $organizationId",
            _description = "Access Requests for $organizationId",
            contacts = mutableSetOf(),
            flags = EnumSet.of(EntitySetFlag.METADATA)
    )
    private fun buildOrganizationMetadataEntitySetName(organizationId: UUID): String = quote(
            "org-metadata-$organizationId"
    )

    private fun buildDatasetsEntitySetName(organizationId: UUID): String = quote("datasets-$organizationId")
    private fun buildColumnEntitySetName(organizationId: UUID): String = quote("column-$organizationId")
    private fun buildSchemasEntitySetName(organizationId: UUID): String = quote("schemas-$organizationId")
    private fun buildViewsEntitySetName(organizationId: UUID): String = quote("views-$organizationId")
    private fun buildAccessRequestsEntitySetName(organizationId: UUID): String = quote("access-requests-$organizationId")

    companion object {
        private val ORGANIZATION_METADATA_ET = FullQualifiedName("ol.organization_metadata")
        private val DATASETS_ET = FullQualifiedName("ol.dataset")
        private val COLUMNS_ET = FullQualifiedName("ol.column")
        private val SCHEMAS_ET = FullQualifiedName("ol.dbschema")
        private val VIEWS_ET = FullQualifiedName("ol.views")
        private val ACCESS_REQUEST_ET = FullQualifiedName("ol.accessrequest")

        private const val EXTERNAL_ID = "ol.externalid"
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
        private const val SQL = "ol.sql"
        private const val OWNER = "ol.owner" // TODO: figure out clean way of exposing owner of an object as owner may not be in OL system

        private const val TEXT = "ol.text"
        private const val PERMISSIONS = "ol.permissions"
        private const val ACLKEY = "ol.aclkey"
        private const val STATUS = "ol.status"
        private const val REQUEST_PRINCIPAL_ID = "ol.requestprincipalid"
        private const val RESPONSE_PRINCIPAL_ID = "ol.responseprincipalid"
        private const val REQUEST_DATETIME = "ol.requestdatetime"
        private const val RESPONSE_DATETIME = "ol.responsedatetime"
    }
}