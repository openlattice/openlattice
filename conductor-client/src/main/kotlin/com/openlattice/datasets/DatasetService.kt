package com.openlattice.datasets

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.hazelcast.HazelcastMap
import java.util.*

class DatasetService(val hazelcast: HazelcastInstance) {

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

    fun updateObjectMetadata(aclKey: AclKey, update: SecurableObjectMetadataUpdate): Boolean {
        return objectMetadata.executeOnKey(aclKey, SecurableObjectMetadataUpdateEntryProcessor(update))
    }

    fun deleteObjectMetadata(aclKey: AclKey) {
        objectMetadata.delete(aclKey)
    }

    fun deleteObjectMetadataForRootObject(id: UUID) {
        objectMetadata.removeAll(Predicates.equal(ObjectMetadataMapstore.ROOT_OBJECT_INDEX, id))
    }

    fun getDataset(id: UUID): Dataset {
        return getDatasets(setOf(id)).getValue(id)
    }

    fun getDatasets(ids: Set<UUID>): Map<UUID, Dataset> {
        val datasetsAsMap = mutableMapOf<UUID, Dataset>()
        val aclKeys = ids.mapTo(mutableSetOf()) { AclKey(it) }

        val metadata = objectMetadata.getAll(aclKeys)
        val types = securableObjectTypes.getAll(aclKeys)

        val typeToId = types.entries.groupBy { it.value }.mapValues { it.value.mapTo(mutableSetOf()) { e -> e.key[0] } }

        typeToId[SecurableObjectType.EntitySet]?.let {
            entitySets.getAll(it).forEach { (id, entitySet) ->
                datasetsAsMap[id] = Dataset.fromEntitySet(entitySet, metadata.getValue(AclKey(id)))
            }
        }

        typeToId[SecurableObjectType.OrganizationExternalDatabaseTable]?.let {
            externalTables.getAll(it).forEach { (id, table) ->
                datasetsAsMap[id] = Dataset.fromExternalTable(table, metadata.getValue(AclKey(id)))
            }
        }

        return datasetsAsMap
    }

    fun getDatasetColumn(aclKey: AclKey): DatasetColumn {
        return getDatasetColumns(setOf(aclKey)).getValue(aclKey)
    }

    fun getDatasetColumns(aclKeys: Set<AclKey>): Map<AclKey, DatasetColumn> {
        val columnsAsMap = mutableMapOf<AclKey, DatasetColumn>()

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
                columnsAsMap[aclKey] = DatasetColumn.fromPropertyType(entitySet, propertyType, metadata)
            }
        }

        typeToId[SecurableObjectType.OrganizationExternalDatabaseColumn]?.let {
            val tables = externalTables.getAll(it.mapTo(mutableSetOf()) { ak -> ak[0] })
            val columns = externalColumns.getAll(it.mapTo(mutableSetOf()) { ak -> ak[1] })

            it.forEach { aclKey ->
                val table = tables.getValue(aclKey[0])
                val column = columns.getValue(aclKey[1])
                val metadata = metadataMap.getValue(aclKey)
                columnsAsMap[aclKey] = DatasetColumn.fromExternalColumn(table, column, metadata)
            }
        }

        return columnsAsMap
    }
}