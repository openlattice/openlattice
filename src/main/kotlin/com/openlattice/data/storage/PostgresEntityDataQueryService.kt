package com.openlattice.data.storage

import com.google.common.collect.Multimaps
import com.openlattice.IdConstants
import com.openlattice.analysis.SqlBindInfo
import com.openlattice.analysis.requests.Filter
import com.openlattice.data.DeleteType
import com.openlattice.data.WriteEvent
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.data.storage.partitions.PartitionsInfo
import com.openlattice.data.util.PostgresDataHasher
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.*
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.openlattice.postgres.streams.StatementHolder
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
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Function
import java.util.function.Supplier
import kotlin.streams.asStream

const val S3_DELETE_BATCH_SIZE = 10_000
const val DEFAULT_BATCH_SIZE = 128_000

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
        if (version.isPresent) {
            throw NotImplementedException("BLAME MTR. Not yet implemented.")
        }

        return BasePostgresIterable(
                PreparedStatementHolderSupplier(
                        hds,
                        "SELECT ${ID_VALUE.name} FROM ${IDS.name} WHERE ${ENTITY_SET_ID.name} = ? AND ${VERSION.name} > 0",
                        FETCH_SIZE
                ) { ps -> ps.setObject(1, entitySetId) }
        ) { rs -> ResultSetAdapters.id(rs) }
    }

    @JvmOverloads
    fun getEntitiesWithPropertyTypeIds(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>> = mapOf(),
            metadataOptions: Set<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            version: Optional<Long> = Optional.empty(),
            linking: Boolean = false
    ): BasePostgresIterable<Pair<UUID, Map<UUID, Set<Any>>>> {
        return getEntitySetIterable(
                entityKeyIds,
                authorizedPropertyTypes,
                propertyTypeFilters,
                metadataOptions,
                version,
                linking
        ) { rs ->
            getEntityPropertiesByPropertyTypeId(rs, authorizedPropertyTypes, metadataOptions, byteBlobDataManager)
        }
    }

    fun getEntitySetWithPropertyTypeIdsIterable(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java)
    ): BasePostgresIterable<Pair<UUID, Map<UUID, Set<Any>>>> {
        return getEntitiesWithPropertyTypeIds(entityKeyIds, authorizedPropertyTypes, mapOf(), metadataOptions)
    }

    @JvmOverloads
    fun getLinkedEntitiesByEntitySetIdWithOriginIds(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            extraMetadataOptions: EnumSet<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            propertyTypeFilters: Map<UUID, Set<Filter>> = mapOf(),
            version: Optional<Long> = Optional.empty()
    ): BasePostgresIterable<Pair<UUID, Pair<UUID, Pair<UUID, MutableMap<UUID, MutableSet<Any>>>>>> {
        val metadataOptions = extraMetadataOptions + MetadataOption.ORIGIN_IDS
        return getEntitySetIterable(
                entityKeyIds,
                authorizedPropertyTypes,
                propertyTypeFilters,
                metadataOptions,
                version,
                true
        ) { rs ->
            getEntityPropertiesByEntitySetIdOriginIdAndPropertyTypeId(
                    rs, authorizedPropertyTypes, metadataOptions, byteBlobDataManager
            )
        }
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
        ) { rs ->
            getEntityPropertiesByFullQualifiedName(
                    rs,
                    authorizedPropertyTypes,
                    metadataOptions,
                    byteBlobDataManager
            )
        }.toMap()
    }


    /**
     * Note: for linking queries, linking id and entity set id will be returned, thus data won't be merged by linking id
     */
    private fun <T> getEntitySetSequence(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>> = mapOf(),
            metadataOptions: Set<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            version: Optional<Long> = Optional.empty(),
            linking: Boolean = false,
            adapter: (ResultSet) -> T
    ): Sequence<T> {
        return getEntitySetIterable(
                entityKeyIds,
                authorizedPropertyTypes,
                propertyTypeFilters,
                metadataOptions,
                version,
                linking,
                adapter
        ).asSequence()
    }

    /**
     * Note: for linking queries, linking id and entity set id will be returned, thus data won't be merged by linking id
     */
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
            val entitySetPartitions = if (linking) partitionManager.getAllPartitions() else partitionManager.getEntitySetPartitionsInfo(
                    entitySetId
            ).partitions.toList()
            maybeEntityKeyIds.map {
                getPartitionsInfo(it, entitySetPartitions)
            }.orElse(entitySetPartitions)
        }.toSet()
        var startIndex = 2
        if (ids.isNotEmpty()) {
            startIndex++
        }
        if (partitions.isNotEmpty()) {
            startIndex++
        }

        val (sql, binders) = buildPreparableFiltersSql(
                startIndex,
                propertyTypes,
                propertyTypeFilters,
                metadataOptions,
                linking,
                ids.isNotEmpty(),
                partitions.isNotEmpty()
        )

        return BasePostgresIterable(PreparedStatementHolderSupplier(hds, sql, FETCH_SIZE) { ps ->
            val metaBinders = linkedSetOf<SqlBinder>()
            var bindIndex = 1
            metaBinders.add(
                    SqlBinder(
                            SqlBindInfo(bindIndex++, PostgresArrays.createUuidArray(ps.connection, entitySetIds)),
                            ::doBind
                    )
            )
            if (ids.isNotEmpty()) {
                metaBinders.add(
                        SqlBinder(
                                SqlBindInfo(bindIndex++, PostgresArrays.createUuidArray(ps.connection, ids)), ::doBind
                        )
                )
            }

            if (partitions.isNotEmpty()) {
                metaBinders.add(
                        SqlBinder(
                                SqlBindInfo(bindIndex++, PostgresArrays.createIntArray(ps.connection, partitions)),
                                ::doBind
                        )
                )
            }
            (metaBinders + binders).forEach { it.bind(ps) }
        }, adapter)
    }

    /**
     * Updates or insert entities.
     * @param entitySetId The entity set id for which to insert entities for.
     * @param entities The entites to update or insert.
     * @param authorizedPropertyTypes The authorized property types for the insertion.
     * @param awsPassthrough True if the data will be stored directly in AWS via another means and all that is being
     * provided is the s3 prefix and key.
     * @param partitionsInfo Contains the partition information for the requested entity set.
     *
     * @return A write event summarizing the results of performing this operation.
     */
    fun upsertEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            awsPassthrough: Boolean = false,
            partitionsInfo: PartitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
    ): WriteEvent {
        val version = System.currentTimeMillis()

        val tombstoneFn = { _: Long, _: Map<UUID, Map<UUID, Set<Any>>> -> }

        return upsertEntities(
                entitySetId,
                tombstoneFn,
                entities,
                authorizedPropertyTypes,
                version,
                partitionsInfo,
                awsPassthrough
        )
    }

    /**
     * Updates or insert entities.
     * @param entitySetId The entity set id for which to insert entities.
     * @param tombstoneFn A function that may tombstone values before performing the upsert.
     * @param entities The entities to update or insert.
     * @param authorizedPropertyTypes The authorized property types for the insertion.
     * @param version The version to use for upserting.
     * @param partitionsInfo Contains the partition information for the requested entity set.
     * @param awsPassthrough True if the data will be stored directly in AWS via another means and all that is being
     * provided is the s3 prefix and key.
     *
     * @return A write event summarizing the results of performing this operation.
     */
    private fun upsertEntities(
            entitySetId: UUID,
            tombstoneFn: (version: Long, entityBatch: Map<UUID, Map<UUID, Set<Any>>>) -> Unit,
            entities: Map<UUID, Map<UUID, Set<Any>>>, // ekids ->
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            version: Long,
            partitionsInfo: PartitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId),
            awsPassthrough: Boolean = false
    ): WriteEvent {
        var updatedEntityCount = 0
        var updatedPropertyCounts = 0
        val partitions = partitionsInfo.partitions.toList()

        entities.entries
                .groupBy({ getPartition(it.key, partitions) }, { it.toPair() })
                .mapValues { it.value.toMap() }
                .asSequence().asStream().parallel()
                .forEach { (partition, rawEntityBatch) ->

                    val entityBatch = rawEntityBatch.mapValues { (entityKeyId, rawValue) ->
                        return@mapValues if (awsPassthrough) {
                            rawValue
                        } else {
                            Multimaps.asMap(JsonDeserializer
                                                    .validateFormatAndNormalize(rawValue, authorizedPropertyTypes)
                                                    { "Entity set $entitySetId with entity key id $entityKeyId" })
                        }
                    }

                    tombstoneFn(version, entityBatch)
                    val upc = upsertEntities(
                            entitySetId,
                            entityBatch,
                            authorizedPropertyTypes,
                            version,
                            partitionsInfo.partitionsVersion,
                            partition,
                            awsPassthrough
                    )
                    //For now we can't track how many entities were updated in a call transactionally.
                    //If we want to check how many entities were written at a specific version that is possible but
                    //expensive.
                    updatedEntityCount += entities.size
                    updatedPropertyCounts += upc
                }

        logger.debug("Updated $updatedEntityCount entities and $updatedPropertyCounts properties")

        return WriteEvent(version, updatedEntityCount)
    }

    private fun upsertEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            version: Long,
            partitionsVersion: Int,
            partition: Int,
            awsPassthrough: Boolean
    ): Int {
        return hds.connection.use { connection ->
            //Update the versions of all entities.
            val entityKeyIdsArr = PostgresArrays.createUuidArray(connection, entities.keys)
            val versionsArrays = PostgresArrays.createLongArray(connection, version)

            /*
             * We do not need entity level locking as our version field ensures that data is consistent even across
             * transactions in all cases, except deletes (clear is fine) as long entity version is not bumped until
             * all properties are written.
             *
             */

            //Update property values. We use multiple prepared statements in batch while re-using ARRAY[version].
            val upsertPropertyValues = mutableMapOf<UUID, PreparedStatement>()
            val updatedPropertyCounts = entities.entries.map { (entityKeyId, entityData) ->

                entityData.map { (propertyTypeId, values) ->
                    val upsertPropertyValue = upsertPropertyValues.getOrPut(propertyTypeId) {
                        val pt = authorizedPropertyTypes[propertyTypeId] ?: abortInsert(entitySetId, entityKeyId)
                        connection.prepareStatement(upsertPropertyValueSql(pt))
                    }

                    values.map { value ->
                        val dataType = authorizedPropertyTypes.getValue(propertyTypeId).datatype

                        val (propertyHash, insertValue) = getPropertyHash(
                                entitySetId, entityKeyId, propertyTypeId,
                                value, dataType, awsPassthrough
                        )

                        upsertPropertyValue.setObject(1, entitySetId)
                        upsertPropertyValue.setObject(2, entityKeyId)
                        upsertPropertyValue.setInt(3, partition)
                        upsertPropertyValue.setObject(4, propertyTypeId)
                        upsertPropertyValue.setObject(5, propertyHash)
                        upsertPropertyValue.setObject(6, version)
                        upsertPropertyValue.setArray(7, versionsArrays)
                        upsertPropertyValue.setInt(8, partitionsVersion)
                        upsertPropertyValue.setObject(9, insertValue)
                        upsertPropertyValue.addBatch()
                    }
                }
                upsertPropertyValues.values.map { it.executeBatch().sum() }.sum()
            }.sum()

            //Make data visible by marking new version in ids table.
            val upsertEntities = connection.prepareStatement(buildUpsertEntitiesAndLinkedData())

            upsertEntities.setObject(1, versionsArrays)
            upsertEntities.setObject(2, version)
            upsertEntities.setObject(3, version)
            upsertEntities.setObject(4, entitySetId)
            upsertEntities.setArray(5, entityKeyIdsArr)
            upsertEntities.setInt(6, partition)
            upsertEntities.setInt(7, partition)
            upsertEntities.setLong(8, version)


            val updatedLinkedEntities = upsertEntities.executeUpdate()
            logger.debug("Updated $updatedLinkedEntities linked entities as part of insert.")
            updatedPropertyCounts
        }

    }

    private fun getPropertyHash(
            entitySetId: UUID,
            entityKeyId: UUID,
            propertyTypeId: UUID,
            value: Any,
            dataType: EdmPrimitiveTypeKind,
            awsPassthrough: Boolean
    ): Pair<ByteArray, Any> {
        if (dataType != EdmPrimitiveTypeKind.Binary) {
            return PostgresDataHasher.hashObject(value, dataType) to value
        }
        //Binary data types get stored in S3 bucket:
        if (awsPassthrough) {
            //Data is being stored in AWS directly the value will be the url fragment
            //of where the data will be stored in AWS.
            return PostgresDataHasher.hashObject(value, EdmPrimitiveTypeKind.String) to value
        }

        //Data is expected to be of a specific type so that it can be stored in s3 bucket
        val binaryData = value as BinaryDataWithContentType
        val digest = PostgresDataHasher.hashObjectToHex(binaryData.data, EdmPrimitiveTypeKind.Binary)

        //store entity set id/entity key id/property type id/property hash as key in S3
        val s3Key = "$entitySetId/$entityKeyId/$propertyTypeId/$digest"

        byteBlobDataManager.putObject(s3Key, binaryData.data, binaryData.contentType)
        return PostgresDataHasher.hashObject(s3Key, EdmPrimitiveTypeKind.String) to s3Key
    }

    fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            partitionsInfo: PartitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
    ): WriteEvent {
        val entityKeyIdsToLinkingIds = getLinkingIdsOfEntityKeyIds(entities.keys)

        val propertyTypes = authorizedPropertyTypes.values

        val tombstoneFn: (Long, Map<UUID, Map<UUID, Set<Any>>>) ->
        Unit = { version: Long,
                 entityBatch: Map<UUID, Map<UUID, Set<Any>>> ->
            tombstone(
                    entitySetId,
                    entityBatch.keys,
                    entityKeyIdsToLinkingIds,
                    propertyTypes,
                    version,
                    partitionsInfo
            )
        }

        return upsertEntities(
                entitySetId,
                tombstoneFn,
                entities,
                authorizedPropertyTypes,
                System.currentTimeMillis(),
                partitionsInfo
        )
    }

    fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            partitionsInfo: PartitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
    ): WriteEvent {
        val entityKeyIdsToLinkingIds = getLinkingIdsOfEntityKeyIds(entities.keys)

        // Is the overhead from including irrelevant property types in a bulk delete really worse than performing individual queries? :thinking-face:
        val tombstoneFn =
                { version: Long,
                  entityBatch: Map<UUID, Map<UUID, Set<Any>>> ->
                    entityBatch.forEach { (entityKeyId, entity) ->
                        //Implied access enforcement as it will raise exception if lacking permission
                        tombstone(
                                entitySetId,
                                setOf(entityKeyId),
                                entityKeyIdsToLinkingIds,
                                entity.keys.map { authorizedPropertyTypes.getValue(it) }.toSet(),
                                version,
                                partitionsInfo
                        )
                    }
                }

        return upsertEntities(
                entitySetId,
                tombstoneFn,
                entities,
                authorizedPropertyTypes,
                System.currentTimeMillis(),
                partitionsInfo
        )
    }

    fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties: Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>, // ekid -> ptid -> hashes -> shit
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        //We expect controller to have performed access control checks upstream.
        val entityKeyIdsToLinkingIds = getLinkingIdsOfEntityKeyIds(replacementProperties.keys)
        val partitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)

        val tombstoneFn: (Long, Map<UUID, Map<UUID, Set<Any>>>) -> Unit =
                { version: Long, entityBatch: Map<UUID, Map<UUID, Set<Any>>> ->
                    val ids = entityBatch.keys
                    tombstone(
                            entitySetId,
                            replacementProperties.filter { ids.contains(it.key) },
                            entityKeyIdsToLinkingIds,
                            version,
                            partitionsInfo
                    )
                }

        //This performs unnecessary copies and we should fix at some point
        val replacementValues = replacementProperties.asSequence().map {
            it.key to extractValues(
                    it.value
            )
        }.toMap()

        return upsertEntities(
                entitySetId,
                tombstoneFn,
                replacementValues,
                authorizedPropertyTypes,
                System.currentTimeMillis(),
                partitionsInfo
        )

    }

    private fun extractValues(propertyValues: Map<UUID, Set<Map<ByteBuffer, Any>>>): Map<UUID, Set<Any>> {
        return propertyValues.mapValues { (_, replacements) -> replacements.flatMap { it.values }.toSet() }
    }

    /**
     * Tombstones all entities in an entity set in both [IDS] and [DATA] table.
     */
    private fun tombstone(conn: Connection, entitySetId: UUID, version: Long): WriteEvent {
        check(!conn.autoCommit) { "Connection auto-commit must be disabled" }

        val numUpdated = conn.prepareStatement(updateVersionsForEntitySet).use { ps ->
            ps.setLong(1, -version)
            ps.setLong(2, -version)
            ps.setLong(3, -version)
            ps.setObject(4, entitySetId)
            ps.executeUpdate()
        }

        //We don't count property type updates.
        conn.prepareStatement(updateVersionsForPropertiesInEntitySet).use { ps ->
            ps.setLong(1, -version)
            ps.setLong(2, -version)
            ps.setLong(3, -version)
            ps.setObject(4, entitySetId)
            ps.executeUpdate()
        }

        return WriteEvent(version, numUpdated)
    }

    /**
     * Tombstones all data from authorizedPropertyTypes for an entity set in both [IDS] and [DATA] table.
     *
     * NOTE: this function commits the tombstone transactions.
     */
    fun clearEntitySet(entitySetId: UUID, authorizedPropertyTypes: Map<UUID, PropertyType>): WriteEvent {
        return hds.connection.use { conn ->
            conn.autoCommit = false
            val version = System.currentTimeMillis()
            tombstone(conn, entitySetId, authorizedPropertyTypes.values, version)
            val event = tombstone(conn, entitySetId, version)
            conn.autoCommit = true
            event
        }
    }

    /**
     * Tombstones (writes a negative version) for the provided entities.
     * @param entitySetId The entity set to operate on.
     * @param entityKeyIds The entity key ids to tombstone.
     * @param authorizedPropertyTypes The property types the user is allowed to tombstone. We assume that authorization
     * checks are enforced at a higher level and that this just streamlines issuing the necessary queries.
     * @param partitionsInfo Contains the partition information for the requested entity set.
     */
    fun clearEntities(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            partitionsInfo: PartitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
    ): WriteEvent {
        val entityKeyIdsToLinkingIds = getLinkingIdsOfEntityKeyIds(entityKeyIds)

        val version = System.currentTimeMillis()

        return hds.connection.use { conn ->
            tombstone(
                    entitySetId,
                    entityKeyIds,
                    entityKeyIdsToLinkingIds,
                    authorizedPropertyTypes.values,
                    version,
                    partitionsInfo
            )
            tombstoneIdsTable(conn, entitySetId, entityKeyIds, version, partitionsInfo)
        }
    }

    /**
     * Tombstones (writes a negative version) for the provided entity properties.
     * @param entitySetId The entity set to operate on.
     * @param entityKeyIds The entity key ids to tombstone.
     * @param authorizedPropertyTypes The property types the user is requested and is allowed to tombstone. We assume
     * that authorization checks are enforced at a higher level and that this just streamlines issuing the necessary
     * queries.
     * @param partitionsInfo Contains the partition information for the requested entity set.
     */
    fun clearEntityData(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            partitionsInfo: PartitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
    ): WriteEvent {
        val version = System.currentTimeMillis()
        val entityKeyIdsToLinkingIds = getLinkingIdsOfEntityKeyIds(entityKeyIds)

        return hds.connection.use { conn ->
            val writeEvent = tombstone(
                    entitySetId,
                    entityKeyIds,
                    entityKeyIdsToLinkingIds,
                    authorizedPropertyTypes.values,
                    version,
                    partitionsInfo
            )
            return@use writeEvent
        }
    }

    /**
     * Deletes properties of entities in entity set from [DATA] table.
     */
    fun deleteEntityData(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            partitionsInfo: PartitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
    ): WriteEvent {
        // TODO same as deleteEntityDataAndEntities?
        // Delete properties from S3
        authorizedPropertyTypes.map { property ->
            if (property.value.datatype == EdmPrimitiveTypeKind.Binary) {
                deletePropertyOfEntityFromS3(entitySetId, entityKeyIds, property.key)
            }
        }

        val partitions = partitionsInfo.partitions.toList()
        val partitionVersion = partitionsInfo.partitionsVersion
        val numUpdates = entityKeyIds
                .groupBy { getPartition(it, partitions) }
                .map { (partition, entities) ->
                    deletePropertiesFromEntities(
                            entitySetId, entities, authorizedPropertyTypes, partition, partitionVersion
                    )
                }.sum()

        return WriteEvent(System.currentTimeMillis(), numUpdates)
    }

    /**
     * Deletes properties of entities in entity set from [DATA] table.
     */
    private fun deletePropertiesFromEntities(
            entitySetId: UUID,
            entities: Collection<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            partition: Int,
            partitionVersion: Int
    ): Int {
        // TODO delete also linking entity entries
        return hds.connection.use { connection ->
            connection.autoCommit = false

            val propertyTypesArr = PostgresArrays.createUuidArray(connection, authorizedPropertyTypes.keys)

            val idsArr = PostgresArrays.createUuidArray(connection, entities)

            lockEntitiesForUpdate(connection, idsArr, partition, partitionVersion)

            // Delete entity properties from data table
            val ps = connection.prepareStatement(deletePropertiesOfEntitiesInEntitySet)
            ps.setObject(1, entitySetId)
            ps.setArray(2, idsArr)
            ps.setInt(3, partition)
            ps.setInt(4, partitionVersion)
            ps.setArray(5, propertyTypesArr)

            val count = ps.executeUpdate()
            connection.commit()
            count
        }
    }

    /**
     * Deletes properties of entities in entity set from [DATA] table.
     */
    fun deleteEntityDataAndEntities(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            partitionsInfo: PartitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
    ): WriteEvent {
        // Delete properties from S3
        authorizedPropertyTypes.map { property ->
            if (property.value.datatype == EdmPrimitiveTypeKind.Binary) {
                deletePropertyOfEntityFromS3(entitySetId, entityKeyIds, property.key)
            }
        }

        val partitions = partitionsInfo.partitions.toList()
        val partitionVersion = partitionsInfo.partitionsVersion
        val numUpdates = entityKeyIds
                .groupBy { getPartition(it, partitions) }
                .map { (partition, entities) ->
                    deleteEntities(entitySetId, entities, partition, partitionVersion)
                }.sum()

        return WriteEvent(System.currentTimeMillis(), numUpdates)
    }

    /**
     * Deletes entities from [DATA] table.
     */
    private fun deleteEntities(
            entitySetId: UUID,
            entities: Collection<UUID>,
            partition: Int,
            partitionVersion: Int
    ): Int {
        // TODO also delete linking entities
        return hds.connection.use { connection ->
            connection.autoCommit = false

            val idsArr = PostgresArrays.createUuidArray(connection, entities)

            lockEntitiesForUpdate(connection, idsArr, partition, partitionVersion)

            // Delete entity properties from data table
            val ps = connection.prepareStatement(deleteEntitiesInEntitySet)
            ps.setObject(1, entitySetId)
            ps.setArray(2, idsArr)
            ps.setInt(3, partition)
            ps.setInt(4, partitionVersion)

            val count = ps.executeUpdate()
            connection.commit()
            count
        }
    }

    /**
     * Deletes property types of entity set from [DATA] table.
     */
    fun deleteEntitySetData(entitySetId: UUID, propertyTypes: Map<UUID, PropertyType>): WriteEvent {
        val numUpdates = hds.connection.use { connection ->
            val ps = connection.prepareStatement(deletePropertyInEntitySet)

            propertyTypes
                    .map { propertyType ->
                        if (propertyType.value.datatype == EdmPrimitiveTypeKind.Binary) {
                            deletePropertiesInEntitySetFromS3(entitySetId, propertyType.key)
                        }

                        ps.setObject(1, entitySetId)
                        ps.setObject(2, propertyType.key)
                        ps.addBatch()
                    }
            ps.executeBatch().sum()
        }

        return WriteEvent(System.currentTimeMillis(), numUpdates)
    }

    private fun deletePropertyOfEntityFromS3(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            propertyTypeId: UUID
    ) {
        val count = AtomicLong()
        BasePostgresIterable<String>(
                PreparedStatementHolderSupplier(hds, selectEntitiesTextProperties, FETCH_SIZE) { ps ->
                    val connection = ps.connection
                    val entitySetIdsArr = PostgresArrays.createUuidArray(connection, setOf(entitySetId))
                    val propertyTypeIdsArr = PostgresArrays.createUuidArray(connection, setOf(propertyTypeId))
                    val entityKeyIdsArr = PostgresArrays.createUuidArray(connection, entityKeyIds)
                    ps.setArray(1, entitySetIdsArr)
                    ps.setArray(2, propertyTypeIdsArr)
                    ps.setArray(3, entityKeyIdsArr)
                }
        ) { rs ->
            rs.getString(getMergedDataColumnName(PostgresDatatype.TEXT))
        }.asSequence().chunked(S3_DELETE_BATCH_SIZE).asStream().parallel().forEach { s3Keys ->
            byteBlobDataManager.deleteObjects(s3Keys)
            count.addAndGet(s3Keys.size.toLong())
        }
    }


    private fun deletePropertiesInEntitySetFromS3(entitySetId: UUID, propertyTypeId: UUID): Long {
        val count = AtomicLong()
        BasePostgresIterable<String>(
                PreparedStatementHolderSupplier(hds, selectEntitySetTextProperties, FETCH_SIZE) { ps ->
                    val entitySetIdsArr = PostgresArrays.createUuidArray(ps.connection, setOf(entitySetId))
                    val propertyTypeIdsArr = PostgresArrays.createUuidArray(ps.connection, setOf(propertyTypeId))
                    ps.setArray(1, entitySetIdsArr)
                    ps.setArray(2, propertyTypeIdsArr)
                }
        ) { rs ->
            rs.getString(getMergedDataColumnName(PostgresDatatype.TEXT))
        }.asSequence().chunked(S3_DELETE_BATCH_SIZE).asStream().parallel().forEach { s3Keys ->
            byteBlobDataManager.deleteObjects(s3Keys)
            count.addAndGet(s3Keys.size.toLong())
        }
        return count.get()
    }

    /**
     * Tombstones (to version = 0) entity key ids belonging to the requested entity set in [IDS] table.
     */
    fun tombstoneDeletedEntitySet(entitySetId: UUID): WriteEvent {
        val partitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)

        val numUpdates = hds.connection.use {
            val ps = it.prepareStatement(zeroVersionsForEntitySet)
            val partitionsArr = PostgresArrays.createIntArray(it, partitionsInfo.partitions)

            ps.setObject(1, entitySetId)
            ps.setArray(2, partitionsArr)
            ps.setInt(3, partitionsInfo.partitionsVersion)
            ps.executeUpdate()
        }

        return WriteEvent(System.currentTimeMillis(), numUpdates)
    }

    /**
     * Deletes entities from [IDS] table.
     */
    fun deleteEntities(entitySetId: UUID, entityKeyIds: Set<UUID>): WriteEvent {
        val partitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
        val partitions = partitionsInfo.partitions.toList()
        val partitionsVersion = partitionsInfo.partitionsVersion

        val numUpdates = hds.connection.use { conn ->
            val ps = conn.prepareStatement(deleteEntityKeys)
            entityKeyIds
                    .groupBy { getPartition(it, partitions) }
                    .forEach { (partition, rawEntityBatch) ->
                        val arr = PostgresArrays.createUuidArray(conn, rawEntityBatch)
                        ps.setObject(1, entitySetId)
                        ps.setArray(2, arr)
                        ps.setInt(3, partition)
                        ps.setInt(4, partitionsVersion)

                        ps.addBatch()
                    }
            ps.executeBatch()
        }.sum()

        return WriteEvent(System.currentTimeMillis(), numUpdates)
    }

    /**
     * Tombstones (to version = 0) entities in [IDS] table.
     */
    fun tombstoneDeletedEntities(entitySetId: UUID, entityKeyIds: Set<UUID>): WriteEvent {
        val partitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
        val partitions = partitionsInfo.partitions.toList()
        val partitionsVersion = partitionsInfo.partitionsVersion

        val numUpdates = hds.connection.use { conn ->
            val ps = conn.prepareStatement(zeroVersionsForEntitiesInEntitySet)

            entityKeyIds.groupBy { getPartition(it, partitions) }
                    .forEach { (partition, rawEntityBatch) ->
                        val partitionsArr = PostgresArrays.createIntArray(conn, listOf(partition))
                        val idsArr = PostgresArrays.createUuidArray(conn, rawEntityBatch)

                        ps.setObject(1, entitySetId)
                        ps.setArray(2, partitionsArr)
                        ps.setInt(3, partitionsVersion)
                        ps.setArray(4, idsArr)

                        ps.addBatch()
                    }

            ps.executeBatch()
        }.sum()

        return WriteEvent(System.currentTimeMillis(), numUpdates)
    }

    private fun lockEntitiesForUpdate(
            connection: Connection,
            idsArr: java.sql.Array,
            partition: Int,
            partitionVersion: Int
    ) {

        check(!connection.autoCommit) { "Connection auto-commit must be disabled" }

        // Acquire entity key id locks
        val rowLocks = connection.prepareStatement(lockEntitiesSql)
        rowLocks.setArray(1, idsArr)
        rowLocks.setInt(2, partition)
        rowLocks.setInt(3, partitionVersion)
        rowLocks.executeQuery()
    }

    /**
     * Tombstones the provided set of property types for each provided entity key.
     *
     * This version of tombstone only operates on the [DATA] table and does not change the version of
     * entities in the [IDS] table
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
            propertyTypesToTombstone: Collection<PropertyType>,
            version: Long
    ): WriteEvent {
        val propertyTypeIdsArr = PostgresArrays.createUuidArray(conn, propertyTypesToTombstone.map { it.id })

        val numUpdated = conn.prepareStatement(updateVersionsForPropertyTypesInEntitySet).use { ps ->
            ps.setLong(1, -version)
            ps.setLong(2, -version)
            ps.setLong(3, -version)
            ps.setObject(4, entitySetId)
            ps.setArray(5, propertyTypeIdsArr)
            ps.executeUpdate()
        }

        return WriteEvent(version, numUpdated)
    }

    /**
     * Tombstones entities in the [PostgresTable.IDS] table.
     *
     * @param conn A valid JDBC connection, ideally with autocommit disabled.
     * @param entitySetId The entity set id for which to tombstone entries.
     * @param entityKeyIds The entity key ids for which to tombstone entries.
     * @param version Version to be used for tombstoning.
     * @param partitionsInfo Contains the partition information for the requested entity set.
     */
    private fun tombstoneIdsTable(
            conn: Connection,
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            version: Long,
            partitionsInfo: PartitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
    ): WriteEvent {
        val partitions = partitionsInfo.partitions.toList()
        val entityKeyIdsArr = PostgresArrays.createUuidArray(conn, entityKeyIds)

        val partitionsVersion = partitionsInfo.partitionsVersion
        val partitionsArr = PostgresArrays.createIntArray(conn, entityKeyIds.map { getPartition(it, partitions) })

        val numUpdated = conn.prepareStatement(updateVersionsForEntitiesInEntitySet).use { ps ->
            ps.setLong(1, -version)
            ps.setLong(2, -version)
            ps.setLong(3, -version)
            ps.setObject(4, entitySetId)
            ps.setArray(5, entityKeyIdsArr)
            ps.setArray(6, partitionsArr)
            ps.setInt(7, partitionsVersion)
            ps.executeUpdate()
        }

        return WriteEvent(version, numUpdated)
    }

    /**
     * Tombstones the provided set of property types for each provided entity key.
     *
     * This version of tombstone only operates on the [DATA] table and does not change the version of
     * entities in the [IDS] table
     *
     * @param entitySetId The entity set id for which to tombstone entries
     * @param entityKeyIds The entity key ids for which to tombstone entries.
     * @param propertyTypesToTombstone A collection of property types to tombstone
     * @param version Version to be used for tombstoning.
     * @param partitionsInfo Contains the partition info for the requested entity set.
     *
     * @return A write event object containing a summary of the operation useful for auditing purposes.
     */
    private fun tombstone(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            entityKeyIdsToLinkingIds: Map<UUID, UUID>,
            propertyTypesToTombstone: Collection<PropertyType>,
            version: Long,
            partitionsInfo: PartitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
    ): WriteEvent {
        val linkingIds = entityKeyIdsToLinkingIds.values
        return hds.connection.use { conn ->
            val propertyTypeIdsArr = PostgresArrays.createUuidArray(conn, propertyTypesToTombstone.map { it.id })
            val entityKeyIdsArr = PostgresArrays.createUuidArray(conn, entityKeyIds)
            val partitions = partitionsInfo.partitions.toList()
            val partitionsArr = PostgresArrays.createIntArray(conn, entityKeyIds.map { getPartition(it, partitions) })
            val linkingPartitionsArr = PostgresArrays.createIntArray(
                    conn, linkingIds.map { getPartition(it, partitions) })

            val numUpdated = conn.prepareStatement(updateVersionsForPropertyTypesInEntitiesInEntitySet()).use { ps ->
                ps.setLong(1, -version)
                ps.setLong(2, -version)
                ps.setLong(3, -version)
                ps.setObject(4, entitySetId)
                ps.setArray(5, propertyTypeIdsArr)
                ps.setArray(6, entityKeyIdsArr)
                ps.setArray(7, partitionsArr)
                ps.setInt(8, partitionsInfo.partitionsVersion)
                ps.executeUpdate()
            }

            val linksTombstoned = conn.prepareStatement(
                    updateVersionsForPropertyTypesInEntitiesInEntitySet(linking = true)
            ).use { ps ->
                ps.setLong(1, -version)
                ps.setLong(2, -version)
                ps.setLong(3, -version)
                ps.setObject(4, entitySetId)
                ps.setArray(5, propertyTypeIdsArr)
                ps.setArray(6, entityKeyIdsArr)
                ps.setArray(7, linkingPartitionsArr)
                ps.setInt(8, partitionsInfo.partitionsVersion)
                ps.executeUpdate()
            }

            WriteEvent(version, numUpdated + linksTombstoned)
        }
    }

    /**
     *
     * Tombstones the provided set of property type hash values for each provided entity key.
     *
     * This version of tombstone only operates on the [DATA] table and does not change the version of
     * entities in the [IDS] table
     *
     * @param entitySetId The entity set id for which to tombstone entries
     * @param entities The entities with their properties for which to tombstone entries.
     * @param entityKeyIdsToLinkingIds Linking ids mapped by their origin ids.
     * @param version The version to use to tombstone.
     * @param partitionsInfo Contains the partition info for the entity set of the entities.
     *
     * @return A write event object containing a summary of the operation useful for auditing purposes.
     *
     */
    private fun tombstone(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>,
            entityKeyIdsToLinkingIds: Map<UUID, UUID>,
            version: Long,
            partitionsInfo: PartitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
    ): WriteEvent {
        return hds.connection.use { conn ->
            val entityKeyIds = entities.keys
            val linkingIds = entityKeyIdsToLinkingIds.values
            val partitions = partitionsInfo.partitions.toList()
            val entityKeyIdsArr = PostgresArrays.createUuidArray(conn, entityKeyIds)
            val partitionsVersion = partitionsInfo.partitionsVersion
            val partitionsArr = PostgresArrays.createIntArray(
                    conn, entities.keys.map {
                getPartition(it, partitions)
            })
            val linkingPartitionsArr = PostgresArrays.createIntArray(
                    conn,
                    linkingIds.map { getPartition(it, partitions) }
            )

            val updatePropertyValueVersion = conn.prepareStatement(
                    updateVersionsForPropertyValuesInEntitiesInEntitySet()
            )
            val tombstoneLinks = conn.prepareStatement(
                    updateVersionsForPropertyValuesInEntitiesInEntitySet(linking = true)
            )

            entities.forEach { (_, entity) ->
                entity.forEach { (propertyTypeId, updates) ->
                    val propertyTypeIdsArr = PostgresArrays.createUuidArray(conn, propertyTypeId)
                    //TODO: https://github.com/pgjdbc/pgjdbc/issues/936
                    //TODO: https://github.com/pgjdbc/pgjdbc/pull/1194
                    //TODO: https://github.com/pgjdbc/pgjdbc/pull/1044
                    //Once above issues are resolved this can be done as a single query WHERE HASH = ANY(?)

                    updates
                            .flatMap { it.keys }
                            .forEach { update ->
                                updatePropertyValueVersion.setLong(1, -version)
                                updatePropertyValueVersion.setLong(2, -version)
                                updatePropertyValueVersion.setLong(3, -version)
                                updatePropertyValueVersion.setObject(4, entitySetId)
                                updatePropertyValueVersion.setArray(5, propertyTypeIdsArr)
                                updatePropertyValueVersion.setArray(6, entityKeyIdsArr)
                                updatePropertyValueVersion.setArray(7, partitionsArr)
                                updatePropertyValueVersion.setInt(8, partitionsVersion)
                                updatePropertyValueVersion.setBytes(9, update.array())
                                updatePropertyValueVersion.addBatch()

                                tombstoneLinks.setLong(1, -version)
                                tombstoneLinks.setLong(2, -version)
                                tombstoneLinks.setLong(3, -version)
                                tombstoneLinks.setObject(4, entitySetId)
                                tombstoneLinks.setArray(5, propertyTypeIdsArr)
                                tombstoneLinks.setArray(6, entityKeyIdsArr)
                                tombstoneLinks.setArray(7, linkingPartitionsArr)
                                tombstoneLinks.setInt(8, partitionsVersion)
                                tombstoneLinks.setBytes(9, update.array())
                                tombstoneLinks.addBatch()
                            }
                }
            }
            val numUpdated = updatePropertyValueVersion.executeUpdate()
            val linksUpdated = tombstoneLinks.executeUpdate()

            WriteEvent(version, numUpdated + linksUpdated)
        }

    }

    private fun getLinkingIdsOfEntityKeyIds(entityKeyIds: Set<UUID>): Map<UUID, UUID> {
        val sql = "SELECT ${ID.name}, ${LINKING_ID.name} FROM ${IDS.name} WHERE ${ID.name} = ANY(?) AND ${LINKING_ID.name} IS NOT NULL"

        return PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    val stmt = connection.prepareStatement(sql)

                    stmt.setArray(1, PostgresArrays.createUuidArray(connection, entityKeyIds))
                    val rs = stmt.executeQuery()
                    StatementHolder(connection, stmt, rs)
                },
                Function<ResultSet, Pair<UUID, UUID>> { rs ->
                    val entityKeyId = ResultSetAdapters.id(rs)
                    val linkingId = ResultSetAdapters.linkingId(rs)

                    entityKeyId to linkingId
                }
        ).toMap()
    }

    fun getExpiringEntitiesFromEntitySet(
            entitySetId: UUID,
            expirationBaseColumn: String,
            formattedDateMinusTTE: Any,
            sqlFormat: Int,
            deleteType: DeleteType
    ): BasePostgresIterable<UUID> {
        val partitionsInfo: PartitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
        val partitions = PostgresArrays.createIntArray(hds.connection, partitionsInfo.partitions)
        val partitionVersion = partitionsInfo.partitionsVersion
        return BasePostgresIterable(
                PreparedStatementHolderSupplier(
                        hds,
                        getExpiringEntitiesQuery(expirationBaseColumn, deleteType),
                        FETCH_SIZE,
                        false
                ) { stmt ->
                    stmt.setObject(1, entitySetId)
                    stmt.setArray(2, partitions)
                    stmt.setInt(3, partitionVersion)
                    stmt.setObject(4, IdConstants.ID_ID.id)
                    stmt.setObject(5, formattedDateMinusTTE, sqlFormat)
                }
        ) { rs -> ResultSetAdapters.id(rs) }
    }

    private fun getExpiringEntitiesQuery(expirationBaseColumn: String, deleteType: DeleteType): String {
        var ignoredClearedEntitiesClause = ""
        if (deleteType == DeleteType.Soft) {
            ignoredClearedEntitiesClause = "AND ${VERSION.name} >= 0 "
        }
        return "SELECT ${ID.name} FROM ${PostgresTable.DATA.name} " +
                "WHERE ${ENTITY_SET_ID.name} = ? " +
                "AND ${PARTITION.name} = ANY(?) " +
                "AND ${PARTITIONS_VERSION.name} = ? " +
                "AND ${PROPERTY_TYPE_ID.name} != ? " +
                "AND $expirationBaseColumn <= ? " +
                ignoredClearedEntitiesClause // this clause ignores entities that have already been cleared
    }
}

private fun abortInsert(entitySetId: UUID, entityKeyId: UUID): Nothing {
    throw InvalidParameterException(
            "Cannot insert property type not in authorized property types for entity $entityKeyId from entity set $entitySetId."
    )
}
