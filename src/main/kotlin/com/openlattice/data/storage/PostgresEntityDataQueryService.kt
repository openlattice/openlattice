package com.openlattice.data.storage

import com.google.common.collect.Multimaps
import com.google.common.collect.SetMultimap
import com.openlattice.analysis.SqlBindInfo
import com.openlattice.analysis.requests.Filter
import com.openlattice.data.WriteEvent
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.data.storage.partitions.PartitionsInfo
import com.openlattice.data.util.PostgresDataHasher
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.*
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.lang3.NotImplementedException
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.security.InvalidParameterException
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*
import kotlin.streams.asStream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresEntityDataQueryService(
        private val hds: HikariDataSource,
        private val byteBlobDataManager: ByteBlobDataManager,
        private val partitionManager: PartitionManager
) {
    companion object {
        private val logger = LoggerFactory.getLogger(PostgresEntityDataQueryService::class.java)
    }

    @JvmOverloads
    fun getEntityKeyIdsInEntitySet(
            entitySetId: UUID,
            version: Optional<Long> = Optional.empty()
    ): BasePostgresIterable<UUID> {
        return if (version.isPresent) {
            throw NotImplementedException("BLAME MTR. Not yet implemented.")
        } else {
            BasePostgresIterable(
                    PreparedStatementHolderSupplier(
                            hds,
                            "SELECT ${ID_VALUE.name} FROM ${IDS.name} WHERE ${ENTITY_SET_ID.name} = ? AND ${VERSION.name} > 0",
                            FETCH_SIZE
                    ) { ps -> ps.setObject(1, entitySetId) }
            ) { rs -> ResultSetAdapters.id(rs) }
        }
    }


    @JvmOverloads
    fun getEntitiesWithPropertyTypeIds(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>> = mapOf(),
            metadataOptions: Set<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            version: Optional<Long> = Optional.empty(),
            linking: Boolean = false
    ): Map<UUID, MutableMap<UUID, MutableSet<Any>>> {
        return getEntitySetIterable(
                entityKeyIds,
                authorizedPropertyTypes,
                propertyTypeFilters,
                metadataOptions,
                version,
                linking
        ) { rs -> getEntityPropertiesByPropertyTypeId(rs, authorizedPropertyTypes, byteBlobDataManager) }.toMap()
    }

    @JvmOverloads
    fun getEntitiesWithPropertyTypeFqns(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>> = mapOf(),
            metadataOptions: Set<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            version: Optional<Long> = Optional.empty(),
            linking: Boolean = false
    ): Map<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>> {
        return getEntitySetIterable(
                entityKeyIds,
                authorizedPropertyTypes,
                propertyTypeFilters,
                metadataOptions,
                version,
                linking
        ) { rs -> getEntityPropertiesByFullQualifiedName(rs, authorizedPropertyTypes, byteBlobDataManager) }.toMap()
    }


    /**
     * Note: for linking queries, linking id and entity set id will be returned, thus data won't be merged by linking id
     */
    // todo: linking queries: do we return linking entity set id or normal? If linking -> query can only be done for 1 linking entityset yet
    private fun <T> getEntitySetSequence(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>> = mapOf(),
            metadataOptions: Set<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            version: Optional<Long> = Optional.empty(),
            linking: Boolean = false,
            adapter: (ResultSet) -> T
    ): Sequence<T> {
        val propertyTypes = authorizedPropertyTypes.values.flatMap { it.values }.associateBy { it.id }
        val entitySetIds = entityKeyIds.keys
        val ids = entityKeyIds.values.flatMap { it.orElse(emptySet()) }.toSet()
        val partitions = entityKeyIds.flatMap { (entitySetId, maybeEntityKeyIds) ->
            maybeEntityKeyIds.map {
                getPartitionsInfo(it, partitionManager.getEntitySetPartitionsInfo(entitySetId).partitions.toList())
            }.orElse(emptyList())
        }
        val (sql, binders) = if (linking) {
            buildPreparableFiltersClauseForLinkedEntities(propertyTypes, propertyTypeFilters)
        } else {
            buildPreparableFiltersSqlForEntities(propertyTypes, propertyTypeFilters)
        }


        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, sql, FETCH_SIZE) { ps ->
            (linkedSetOf(
                    SqlBinder(SqlBindInfo(1, PostgresArrays.createUuidArray(ps.connection, entitySetIds)), ::doBind),
                    // TODO with empty ids it won't return any rows
                    SqlBinder(SqlBindInfo(2, PostgresArrays.createUuidArray(ps.connection, ids)), ::doBind),
                    SqlBinder(SqlBindInfo(3, PostgresArrays.createIntArray(ps.connection, partitions)), ::doBind)
            ) + binders).forEach { it.bind(ps) }
        }, adapter).asSequence()
    }

    fun getEntitySetWithPropertyTypeIdsIterable(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java)
    ) : BasePostgresIterable<Pair<UUID, Map<UUID, Set<Any>>>> {
        return getEntitySetIterable(entityKeyIds, authorizedPropertyTypes,mapOf(), metadataOptions) { rs ->
            getEntityPropertiesByPropertyTypeId(rs, authorizedPropertyTypes, byteBlobDataManager)
        }
    }
    /**
     * Note: for linking queries, linking id and entity set id will be returned, thus data won't be merged by linking id
     */
    // todo: linking queries: do we return linking entity set id or normal? If linking -> query can only be done for 1 linking entityset yet
    private fun <T> getEntitySetIterable(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>> = mapOf(),
            metadataOptions: Set<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            version: Optional<Long> = Optional.empty(),
            linking: Boolean = false,
            adapter: (ResultSet) -> T
    ): BasePostgresIterable<T> {
        val propertyTypes = authorizedPropertyTypes.values.flatMap { it.values }.associateBy { it.id }
        val entitySetIds = entityKeyIds.keys
        val ids = entityKeyIds.values.flatMap { it.orElse(emptySet()) }.toSet()
        val partitions = entityKeyIds.flatMap { (entitySetId, maybeEntityKeyIds) ->
            maybeEntityKeyIds.map {
                getPartitionsInfo(it, partitionManager.getEntitySetPartitionsInfo(entitySetId).partitions.toList())
            }.orElse(emptyList())
        }
        val (sql, binders) = if (linking) {
            buildPreparableFiltersClauseForLinkedEntities(propertyTypes, propertyTypeFilters)
        } else {
            buildPreparableFiltersSqlForEntities(propertyTypes, propertyTypeFilters)
        }


        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, sql, FETCH_SIZE) { ps ->
            (linkedSetOf(
                    SqlBinder(SqlBindInfo(1, PostgresArrays.createUuidArray(ps.connection, entitySetIds)), ::doBind),
                    // TODO with empty ids it won't return any rows
                    SqlBinder(SqlBindInfo(2, PostgresArrays.createUuidArray(ps.connection, ids)), ::doBind),
                    SqlBinder(SqlBindInfo(3, PostgresArrays.createIntArray(ps.connection, partitions)), ::doBind)
            ) + binders).forEach { it.bind(ps) }
        }, adapter)
    }

    /**
     * Updates or insert entities.
     * @param entitySetId The entity set id for which to insert entities for.
     * @param entities The entites to update or insert.
     * @param authorizedPropertyTypes The authorized property types for the insertion.
     *
     * @return A write event summarizing the results of performing this operation.
     */
    fun upsertEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        return upsertEntities(entitySetId, entities, authorizedPropertyTypes, false)
    }

    /**
     * Updates or insert entities.
     * @param entitySetId The entity set id for which to insert entities for.
     * @param entities The entites to update or insert.
     * @param authorizedPropertyTypes The authorized property types for the insertion.
     * @param awsPassthrough True if the data will be stored directly in AWS via another means and all that is being
     * provided is the s3 prefix and key.
     *
     * @return A write event summarizing the results of performing this operation.
     */
    fun upsertEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            awsPassthrough: Boolean
    ): WriteEvent {
        val version = System.currentTimeMillis()
        val partitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
        val partitions = partitionsInfo.partitions.toList()
        //Update the versions of all entities.
        val (updatedEntityCount, updatedPropertyCounts) = hds.connection.use { connection ->
            connection.autoCommit = false
            val entityKeyIdsArr = PostgresArrays.createUuidArray(connection, entities.keys)
            val versionsArrays = PostgresArrays.createLongArray(connection, arrayOf(version))

            /*
             * Our approach is to use entity level locking that takes advantage of the router executor to avoid deadlocks.
             *
             * If performance becomes an issue, we can break this is up into individual transactions at the risk of
             * ending up with partitial property right and decoupled metadata updates.
             */

            //Acquire entity key id locks
            val rowLocks = connection.prepareStatement(lockEntitiesSql)
            rowLocks.setObject(1, entitySetId)
            rowLocks.setObject(2, entities.keys)
            rowLocks.setObject(3, entityKeyIdsArr)
            rowLocks.execute()

            //Update metadata
            val upsertEntities = connection.prepareStatement(upsertEntitiesSql)
            upsertEntities.setObject(1, version)
            upsertEntities.setObject(2, version)
            upsertEntities.setObject(3, version)
            upsertEntities.setObject(4, entitySetId)
            upsertEntities.setObject(5, entityKeyIdsArr)
            val updatedEntityCount = upsertEntities.executeUpdate()

            //Basic validation.
            if (updatedEntityCount != entities.size) {
                logger.warn(
                        "Update $updatedEntityCount entities. Expect to update ${entities.size} for entity set $entitySetId."
                )
                logger.debug("Entity key ids: {}", entities.keys)
            }

            //Update property values. We use multiple prepared statements in batch while re-using ARRAY[version].
            val upsertPropertyValues = mutableMapOf<UUID, PreparedStatement>()
            val updatedPropertyCounts = entities.entries.map { (entityKeyId, rawValue) ->
                val entityData = if (awsPassthrough) {
                    rawValue
                } else {
                    Multimaps.asMap(JsonDeserializer
                                            .validateFormatAndNormalize(rawValue, authorizedPropertyTypes)
                                            { "Entity set $entitySetId with entity key id $entityKeyId" })
                }

                entityData.map { (propertyTypeId, values) ->
                    val upsertPropertyValue = upsertPropertyValues.getOrPut(propertyTypeId) {
                        val pt = authorizedPropertyTypes[propertyTypeId] ?: abortInsert(entitySetId, entityKeyId)
                        connection.prepareStatement(upsertPropertyValueSql(pt))
                    }

                    //TODO: Keep track of collisions here. We can detect when hashes collide for an entity
                    //and read the existing value to determine which colliding values need to be assigned new
                    // hashes. This is fine because hashes are immutable and the front-end always requests them
                    // from the backend before performing operations.

                    values.map { value ->
                        //Binary data types get stored in S3 bucket
                        val (propertyHash, insertValue) =
                                if (authorizedPropertyTypes
                                                .getValue(propertyTypeId).datatype == EdmPrimitiveTypeKind.Binary) {
                                    if (awsPassthrough) {
                                        //Data is being stored in AWS directly the value will be the url fragment
                                        //of where the data will be stored in AWS.
                                        PostgresDataHasher.hashObject(
                                                value,
                                                EdmPrimitiveTypeKind.String
                                        ) to value
                                    } else {
                                        //Data is expected to be of a specific type so that it can be stored in
                                        //s3 bucket

                                        val binaryData = value as BinaryDataWithContentType

                                        val digest = PostgresDataHasher
                                                .hashObjectToHex(binaryData.data, EdmPrimitiveTypeKind.Binary)
                                        //store entity set id/entity key id/property type id/property hash as key in S3
                                        val s3Key = "$entitySetId/$entityKeyId/$propertyTypeId/$digest"
                                        byteBlobDataManager
                                                .putObject(s3Key, binaryData.data, binaryData.contentType)
                                        PostgresDataHasher
                                                .hashObject(s3Key, EdmPrimitiveTypeKind.String) to s3Key
                                    }
                                } else {
                                    PostgresDataHasher.hashObject(
                                            value,
                                            authorizedPropertyTypes.getValue(propertyTypeId).datatype
                                    ) to value

                                }

                        upsertPropertyValue.setObject(1, entitySetId)
                        upsertPropertyValue.setObject(2, entityKeyId)
                        upsertPropertyValue.setObject(3, getPartition(entityKeyId, partitions))
                        upsertPropertyValue.setObject(4, propertyTypeId)
                        upsertPropertyValue.setObject(5, propertyHash)
                        upsertPropertyValue.setObject(6, version)
                        upsertPropertyValue.setObject(7, versionsArrays)
                        upsertPropertyValue.setObject(8, insertValue)
                        upsertPropertyValue.setObject(9, partitionsInfo.partitionsVersion)
                        upsertPropertyValue.addBatch()
                    }
                }
                upsertPropertyValues.values.map { it.executeBatch().sum() }.sum()
            }.sum()
            connection.commit()
            updatedEntityCount to updatedPropertyCounts
        }


        logger.debug("Updated $updatedEntityCount entities and $updatedPropertyCounts properties")

        return WriteEvent(version, updatedEntityCount)
    }

    fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>): WriteEvent {
        TODO("Not implemented")
    }

    fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>): WriteEvent {
        TODO("Not implemented")
    }

    fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties: Map<UUID, SetMultimap<UUID, Map<ByteBuffer, Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>): WriteEvent {
        TODO("Not implemented")
    }

    /**
     * Tombstones all entities in an entity set.
     */
    private fun tombstone(conn: Connection, entitySetId: UUID): WriteEvent {
        val connection = hds.connection
        val tombstoneVersion = -System.currentTimeMillis()
        val autoCommit = conn.autoCommit

        conn.autoCommit = false

        val numUpdated = conn.prepareStatement(updateVersionsForEntitySet).use { ps ->
            ps.setObject(1, tombstoneVersion)
            ps.setObject(2, tombstoneVersion)
            ps.setObject(3, tombstoneVersion)
            ps.setObject(4, entitySetId)
            ps.executeUpdate()
        }

        //We don't count property type updates.
        conn.prepareStatement(updateVersionsForPropertiesInEntitySet).use { ps ->
            ps.setObject(1, tombstoneVersion)
            ps.setObject(2, tombstoneVersion)
            ps.setObject(3, tombstoneVersion)
            ps.setObject(4, entitySetId)
            ps.executeUpdate()
        }

        conn.commit()
        conn.autoCommit = autoCommit
        numUpdated


        return WriteEvent(tombstoneVersion, numUpdated)
    }

    fun clearEntitySet(entitySetId: UUID, authorizedPropertyTypes: Map<UUID, PropertyType>): WriteEvent {
        TODO("Not implemented")
    }

    /**
     * Tombstones (writes a negative version) for the provided entities.
     * @param entitySetId The entity set to operate on.
     * @param entityKeyIds The entity key ids to tombstone.
     * @param authorizedPropertyTypes The property types the user is allowed to tombstone. We assume that authorization
     * checks are enforced at a higher level and that this just streamlines issuing the necessary queries.
     */
    fun clearEntities(
            entitySetId: UUID, entityKeyIds: Set<UUID>, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        return hds.connection.use { conn ->
            tombstone(conn, entitySetId, entityKeyIds, authorizedPropertyTypes.values)
            tombstone(conn, entitySetId, entityKeyIds)
        }
    }

    /**
     * Tombstones (writes a negative version) for the provided entity properties.
     * @param entitySetId The entity set to operate on.
     * @param entityKeyIds The entity key ids to tombstone.
     * @param authorizedPropertyTypes The property types the user is requested and is allowed to tombstone. We assume
     * that authorization checks are enforced at a higher level and that this just streamlines issuing the necessary
     * queries.
     */
    fun clearEntityData(
            entitySetId: UUID, entityKeyIds: Set<UUID>, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        return hds.connection.use { conn ->
            tombstone(conn, entitySetId, entityKeyIds, authorizedPropertyTypes.values)
        }
    }

    fun deleteEntityData(
            entitySetId: UUID, entityKeyIds: Set<UUID>, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        val connection = hds.connection
        val numUpdates = connection.use {
            authorizedPropertyTypes
                    .map { property ->
                        if (property.value.datatype == EdmPrimitiveTypeKind.Binary) {
                            deletePropertyOfEntityFromS3(entitySetId, entityKeyIds, property.key)
                        }

                        val ps = it.prepareStatement(deletePropertiesOfEntities(entitySetId, property.key))
                        val arr = PostgresArrays.createUuidArray(it, entityKeyIds)
                        ps.setArray(1, arr)

                        val count = ps.executeUpdate()
                        ps.close()
                        count
                    }
                    .sum()
        }

        return WriteEvent(System.currentTimeMillis(), numUpdates)
    }

    fun deleteEntitySetData(entitySetId: UUID, propertyTypes: Map<UUID, PropertyType>): WriteEvent {
        val numUpdates = hds.connection.use { connection ->
            propertyTypes
                    .map {
                        if (it.value.datatype == EdmPrimitiveTypeKind.Binary) {
                            deletePropertiesInEntitySetFromS3(entitySetId, it.key)
                        }
                        connection.createStatement().use { stmt ->
                            stmt.executeUpdate(deletePropertiesInEntitySet(entitySetId, it.key))
                        }
                    }
                    .sum()
        }

        return WriteEvent(System.currentTimeMillis(), numUpdates)
    }

    fun deletePropertyOfEntityFromS3(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            propertyTypeId: UUID
    ) {
        BasePostgresIterable<String>(
                PreparedStatementHolderSupplier(hds, selectEntitySetTextProperties, FETCH_SIZE) { ps ->
                    val connection = ps.connection
                    val ps = connection.prepareStatement(selectEntitiesTextProperties)
                    val entitySetIdsArr = PostgresArrays.createUuidArray(connection, setOf(entitySetId))
                    val propertyTypeIdsArr = PostgresArrays.createUuidArray(connection, setOf(propertyTypeId))
                    val entityKeyIdsArr = PostgresArrays.createUuidArray(connection, entityKeyIds)
                    ps.setObject(1, entitySetIdsArr)
                    ps.setArray(2, propertyTypeIdsArr)
                    ps.setArray(3, entityKeyIdsArr)
                }
        ) { rs ->
            rs.getString(getDataColumnName(PostgresDatatype.TEXT))
        }.asSequence().chunked(10000).asStream().parallel().forEach { byteBlobDataManager.deleteObjects(it) }
    }


    fun deletePropertiesInEntitySetFromS3(entitySetId: UUID, propertyTypeId: UUID) {
        BasePostgresIterable<String>(
                PreparedStatementHolderSupplier(hds, selectEntitySetTextProperties, FETCH_SIZE) { ps ->
                    val connection = ps.connection
                    val ps = connection.prepareStatement(selectEntitySetTextProperties)
                    val entitySetIdsArr = PostgresArrays.createUuidArray(connection, setOf(entitySetId))
                    val propertyTypeIdsArr = PostgresArrays.createUuidArray(connection, setOf(propertyTypeId))
                    ps.setObject(1, entitySetIdsArr)
                    ps.setArray(2, propertyTypeIdsArr)
                }
        ) { rs ->
            rs.getString(getDataColumnName(PostgresDatatype.TEXT))
        }.asSequence().chunked(10000).asStream().parallel().forEach { byteBlobDataManager.deleteObjects(it) }
    }

    fun deleteEntitySet(entitySetId: UUID): WriteEvent {
        val numUpdates = hds.connection.use {
            val s = it.prepareStatement(deleteEntitySetEntityKeys(entitySetId))
            s.executeUpdate()
        }

        return WriteEvent(System.currentTimeMillis(), numUpdates)
    }

    fun deleteEntities(entitySetId: UUID, entityKeyIds: Set<UUID>): WriteEvent {
        val numUpdates = hds.connection.use {
            val s = it.prepareStatement(deleteEntityKeys(entitySetId))
            val arr = PostgresArrays.createUuidArray(it, entityKeyIds)
            s.setArray(1, arr)
            s.executeUpdate()
        }

        return WriteEvent(System.currentTimeMillis(), numUpdates)
    }

    /**
     * Tombstones the provided set of property types for each provided entity key.
     *
     * This version of tombstone only operates on the [PostgresTable.DATA] table and does not change the version of
     * entities in the [PostgresTable.ENTITY_KEY_IDS] table
     *
     * @param conn A valid JDBC connection, ideally with autocommit disabled.
     * @param entitySetId The entity set id for which to tombstone entries
     * @param propertyTypesToTombstone A collection of property types to tombstone
     *
     * @return A write event object containing a summary of the operation useful for auditing purposes.
     *
     */
    private fun tombstone(
            conn: Connection,
            entitySetId: UUID,
            propertyTypesToTombstone: Collection<PropertyType>
    ): WriteEvent {
        val tombstoneVersion = -System.currentTimeMillis()
        val propertyTypeIdsArr = PostgresArrays.createUuidArray(conn, propertyTypesToTombstone.map { it.id })

        val numUpdated = conn.prepareStatement(updateVersionsForPropertyTypesInEntitySet).use { ps ->
            ps.setObject(1, tombstoneVersion)
            ps.setObject(2, tombstoneVersion)
            ps.setObject(3, tombstoneVersion)
            ps.setObject(4, entitySetId)
            ps.setObject(5, propertyTypeIdsArr)
            ps.executeUpdate()
        }

        return WriteEvent(tombstoneVersion, numUpdated)
    }

    /**
     * Tombstones entities in the [PostgresTable.ENTITY_KEY_IDS] table.
     *
     * @param conn A valid JDBC connection, ideally with autocommit disabled.
     * @param entitySetId The entity set id for which to tombstone entries.
     * @param entityKeyIds The entity key ids for which to tombstone entries.
     * @param partitionsInfo Contains the partition info for
     */
    private fun tombstone(
            conn: Connection,
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            partitionsInfo: PartitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
    ): WriteEvent {
        val tombstoneVersion = -System.currentTimeMillis()
        val entityKeyIdsArr = PostgresArrays.createUuidArray(conn, entityKeyIds)
        val partitionsArr = PostgresArrays.createIntArray(conn, partitionsInfo.partitions)

        val numUpdated = conn.prepareStatement(updateVersionsForEntitiesInEntitySet).use { ps ->
            ps.setObject(1, tombstoneVersion)
            ps.setObject(2, tombstoneVersion)
            ps.setObject(3, tombstoneVersion)
            ps.setObject(4, entitySetId)
            ps.setObject(5, entityKeyIdsArr)
            ps.setObject(6, partitionsArr)
            ps.executeUpdate()
        }

        return WriteEvent(tombstoneVersion, numUpdated)
    }

    /**
     * Tombstones the provided set of property types for each provided entity key.
     *
     * This version of tombstone only operates on the [PostgresTable.DATA] table and does not change the version of
     * entities in the [PostgresTable.ENTITY_KEY_IDS] table
     *
     * @param conn A valid JDBC connection, ideally with autocommit disabled.
     * @param entitySetId The entity set id for which to tombstone entries
     * @param entityKeyIds The entity key ids for which to tombstone entries.
     * @param propertyTypesToTombstone A collection of property types to tombstone
     *
     * @return A write event object containing a summary of the operation useful for auditing purposes.
     *
     */
    private fun tombstone(
            conn: Connection,
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            propertyTypesToTombstone: Collection<PropertyType>
    ): WriteEvent {
        val tombstoneVersion = -System.currentTimeMillis()
        val propertyTypeIdsArr = PostgresArrays.createUuidArray(conn, propertyTypesToTombstone.map { it.id })
        val entityKeyIdsArr = PostgresArrays.createUuidArray(conn, entityKeyIds)
        val partitionsArr = PostgresArrays.createIntArray(conn, entityKeyIds.map { it.leastSignificantBits.toInt() })
        val numUpdated = conn.prepareStatement(updateVersionsForPropertyTypesInEntitiesInEntitySet).use { ps ->
            ps.setObject(1, tombstoneVersion)
            ps.setObject(2, tombstoneVersion)
            ps.setObject(3, tombstoneVersion)
            ps.setObject(4, entitySetId)
            ps.setObject(5, entityKeyIdsArr)
            ps.setObject(6, partitionsArr)
            ps.setObject(5, propertyTypeIdsArr)
            ps.executeUpdate()
        }

        return WriteEvent(tombstoneVersion, numUpdated)
    }
}


private fun abortInsert(entitySetId: UUID, entityKeyId: UUID): Nothing {
    throw InvalidParameterException(
            "Cannot insert property type not in authorized property types for entity $entityKeyId from entity set $entitySetId."
    )
}