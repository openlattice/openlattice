package com.openlattice.datasets

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.edm.EntitySet
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.ExternalTable
import com.openlattice.organizations.mapstores.ExternalTablesMapstore
import com.openlattice.postgres.mapstores.EntitySetMapstore
import com.openlattice.search.SearchService
import org.springframework.stereotype.Service
import java.util.*

@Service
class DataSetService(
    hazelcast: HazelcastInstance,
    val searchService: SearchService
) {

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap(hazelcast)
    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(hazelcast)
    private val externalTables = HazelcastMap.EXTERNAL_TABLES.getMap(hazelcast)
    private val externalColumns = HazelcastMap.EXTERNAL_COLUMNS.getMap(hazelcast)
    private val securableObjectTypes = HazelcastMap.SECURABLE_OBJECT_TYPES.getMap(hazelcast)
    private val objectMetadata = HazelcastMap.OBJECT_METADATA.getMap(hazelcast)

    fun getObjectMetadata(aclKey: AclKey): SecurableObjectMetadata {
        return objectMetadata.getValue(aclKey)
    }

    fun getObjectMetadata(aclKeys: Set<AclKey>): Map<AclKey, SecurableObjectMetadata> {
        return objectMetadata.getAll(aclKeys)
    }

    fun initializeMetadata(aclKey: AclKey, metadata: SecurableObjectMetadata) {
        objectMetadata[aclKey] = metadata
    }

    fun initializeMetadata(metadata: Map<AclKey, SecurableObjectMetadata>) {
        objectMetadata.putAll(metadata)
    }

    fun updateObjectMetadata(aclKey: AclKey, update: SecurableObjectMetadataUpdate): Boolean {
        val success = objectMetadata.executeOnKey(aclKey, SecurableObjectMetadataUpdateEntryProcessor(update))
        updateIndexedObject(aclKey)
        return success
    }

    fun deleteObjectMetadata(aclKey: AclKey) {
        objectMetadata.delete(aclKey)
        deleteIndexedObject(aclKey)
    }

    fun deleteObjectMetadataForRootObject(id: UUID) {
        objectMetadata.removeAll(Predicates.equal(ObjectMetadataMapstore.ROOT_OBJECT_INDEX, id))
    }

    fun getDataset(id: UUID): DataSet {
        return getDatasets(setOf(id)).getValue(id)
    }

    fun getDatasets(ids: Set<UUID>): Map<UUID, DataSet> {
        val datasetsAsMap = mutableMapOf<UUID, DataSet>()
        val aclKeys = ids.mapTo(mutableSetOf()) { AclKey(it) }

        val metadata = objectMetadata.getAll(aclKeys)
        val types = securableObjectTypes.getAll(aclKeys)

        val typeToId = types.entries.groupBy { it.value }.mapValues { it.value.mapTo(mutableSetOf()) { e -> e.key[0] } }

        typeToId[SecurableObjectType.EntitySet]?.let {
            entitySets.getAll(it).forEach { (id, entitySet) ->
                datasetsAsMap[id] = DataSet.fromEntitySet(entitySet, metadata.getValue(AclKey(id)))
            }
        }

        typeToId[SecurableObjectType.OrganizationExternalDatabaseTable]?.let {
            externalTables.getAll(it).forEach { (id, table) ->
                datasetsAsMap[id] = DataSet.fromExternalTable(table, metadata.getValue(AclKey(id)))
            }
        }

        return datasetsAsMap
    }

    fun getOrganizationDataSetIds(organizationId: UUID): Set<UUID> {
        val entitySetIds = entitySets.keySet(
            Predicates.equal(EntitySetMapstore.ORGANIZATION_INDEX, organizationId)
        )
        val tableIds = externalTables.keySet(
            Predicates.equal(ExternalTablesMapstore.ORGANIZATION_ID_INDEX, organizationId)
        )
        return entitySetIds + tableIds
    }

    fun getDatasetColumn(aclKey: AclKey): DataSetColumn {
        return getDatasetColumns(setOf(aclKey)).getValue(aclKey)
    }

    fun getDatasetColumns(aclKeys: Set<AclKey>): Map<AclKey, DataSetColumn> {
        val columnsAsMap = mutableMapOf<AclKey, DataSetColumn>()

        val metadataMap = objectMetadata.getAll(aclKeys)
        val types = securableObjectTypes.getAll(aclKeys)

        val typeToId = types.entries.groupBy { it.value }.mapValues { it.value.mapTo(mutableSetOf()) { e -> e.key } }

        typeToId[SecurableObjectType.PropertyTypeInEntitySet]?.let {
            val relevantEntitySets = entitySets.getAll(it.mapTo(mutableSetOf()) { ak -> ak[0] })
            val relevantPropertyTypes = propertyTypes.getAll(it.mapTo(mutableSetOf()) { ak -> ak[1] })
            it.forEach { aclKey ->
                val entitySet = relevantEntitySets.getValue(aclKey[0])
                val propertyType = relevantPropertyTypes.getValue(aclKey[1])
                val metadata = metadataMap.getValue(aclKey)
                columnsAsMap[aclKey] = DataSetColumn.fromPropertyType(entitySet, propertyType, metadata)
            }
        }

        typeToId[SecurableObjectType.OrganizationExternalDatabaseColumn]?.let {
            val tables = externalTables.getAll(it.mapTo(mutableSetOf()) { ak -> ak[0] })
            val columns = externalColumns.getAll(it.mapTo(mutableSetOf()) { ak -> ak[1] })

            it.forEach { aclKey ->
                val table = tables.getValue(aclKey[0])
                val column = columns.getValue(aclKey[1])
                val metadata = metadataMap.getValue(aclKey)
                columnsAsMap[aclKey] = DataSetColumn.fromExternalColumn(table, column, metadata)
            }
        }

        return columnsAsMap
    }

    fun getColumnsInDataset(datasetId: UUID): List<DataSetColumn> {
        return getColumnsInDatasets(setOf(datasetId)).getValue(datasetId).toList()
    }

    fun getColumnsInDatasets(datasetIds: Set<UUID>): Map<UUID, Collection<DataSetColumn>> {
        val columnKeys = objectMetadata.keySet(
                Predicates.`in`(ObjectMetadataMapstore.ROOT_OBJECT_INDEX, *datasetIds.toTypedArray())
        ).filter { it.size > 1 }.toSet()

        val columns = getDatasetColumns(columnKeys)

        val existingDatasetToColumns = columnKeys.groupBy { it.root }.mapValues { it.value.map { aclKey -> columns.getValue(aclKey) } }
        return datasetIds.associateWith { existingDatasetToColumns[it] ?: listOf() }
    }

    fun getObjectType(aclKey: AclKey): SecurableObjectType {
        return securableObjectTypes.getValue(aclKey)
    }

    fun filterDatasetIdsByOrganizations(datasetIds: Collection<UUID>, organizationIds: Collection<UUID>): Set<UUID> {
        val entitySetIds = entitySets.keySet(
            Predicates.and(
                Predicates.`in`<UUID, EntitySet>(
                    EntitySetMapstore.ORGANIZATION_INDEX,
                    *organizationIds.toTypedArray()
                ),
                Predicates.`in`<UUID, EntitySet>(
                    EntitySetMapstore.ID_INDEX,
                    *datasetIds.toTypedArray()
                )
            )
        )

        val tableIds = externalTables.keySet(
            Predicates.and(
                Predicates.`in`<UUID, ExternalTable>(
                    ExternalTablesMapstore.ORGANIZATION_ID_INDEX,
                    *organizationIds.toTypedArray()
                ),
                Predicates.`in`<UUID, ExternalTable>(
                    ExternalTablesMapstore.ID_INDEX,
                    *datasetIds.toTypedArray()
                )
            )
        )

        return entitySetIds + tableIds
    }

    fun getExternalTables(): Iterable<ExternalTable> {
        return externalTables.values
    }


    private fun deleteIndexedObject(aclKey: AclKey) {
        if (aclKey.size != 1) {
            return
        }
        searchService.deleteIndexedDataSet(aclKey.first())
    }

    private fun updateIndexedObject(aclKey: AclKey) {
        val rootObjectId = aclKey.first()
        if (aclKey.size == 1) {
            searchService.updateIndexedDataSet(rootObjectId)
        } else {
            searchService.updateIndexedDataSetColumns(rootObjectId)
        }
    }
}
