package com.openlattice.data.storage

import com.google.common.collect.Multimaps
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
import java.util.concurrent.atomic.AtomicLong
import kotlin.streams.asStream

const val S3_DELETE_BATCH_SIZE = 10000

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

    /**
     *
     */
    @JvmOverloads
    fun getEntitiesWithPropertyTypeIds(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>> = mapOf(),
            metadataOptions: Set<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java),
            version: Optional<Long> = Optional.empty(),
            linking: Boolean = false
    ): BasePostgresIterable<Pair<UUID, MutableMap<UUID, MutableSet<Any>>>> {
        return getEntitySetIterable(
                entityKeyIds,
                authorizedPropertyTypes,
                propertyTypeFilters,
                metadataOptions,
                version,
                linking
        ) { rs -> getEntityPropertiesByPropertyTypeId2(rs, authorizedPropertyTypes, byteBlobDataManager) }
    }

    @JvmOverloads
    fun getEntitiesWithPropertyTypeIdsOld(
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

    fun getEntitySetWithPropertyTypeIdsIterable(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption> = EnumSet.noneOf(MetadataOption::class.java)
    ): BasePostgresIterable<Pair<UUID, Map<UUID, Set<Any>>>> {
        return getEntitySetIterable(entityKeyIds, authorizedPropertyTypes, mapOf(), metadataOptions) { rs ->
            getEntityPropertiesByPropertyTypeId2(rs, authorizedPropertyTypes,byteBlobDataManager )
//            getEntityPropertiesByPropertyTypeId(rs, authorizedPropertyTypes, byteBlobDataManager)
        }
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
            val entitySetPartitions = partitionManager.getEntitySetPartitionsInfo(entitySetId).partitions.toList()
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

        val (sql, binders) = if (linking) {
            buildPreparableFiltersSqlForLinkedEntities(
                    startIndex, propertyTypes, propertyTypeFilters, ids.isNotEmpty(), partitions.isNotEmpty()
            )
        } else {
            buildPreparableFiltersSqlForEntities(
                    startIndex, propertyTypes, propertyTypeFilters, ids.isNotEmpty(), partitions.isNotEmpty()
            )
        }

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
     *
     * @return A write event summarizing the results of performing this operation.
     */
    fun upsertEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            awsPassthrough: Boolean = false
    ): WriteEvent {
        return hds.connection.use { connection ->
            val writeEvent = upsertEntities(connection, entitySetId, entities, authorizedPropertyTypes, awsPassthrough)
//            connection.commit()
//            connection.autoCommit = true
            return@use writeEvent
        }
    }

    /**
     * Updates or insert entities.
     * @param entitySetId The entity set id for which to insert entities for.
     * @param entities The entities to update or insert.
     * @param authorizedPropertyTypes The authorized property types for the insertion.
     * @param awsPassthrough True if the data will be stored directly in AWS via another means and all that is being
     * provided is the s3 prefix and key.
     *
     * @return A write event summarizing the results of performing this operation.
     */
    private fun upsertEntities(
            connection: Connection,
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            awsPassthrough: Boolean = false
    ): WriteEvent {
        connection.autoCommit = true
//        check(!connection.autoCommit) { "Connection must not be in autocommit mode." }
        val version = System.currentTimeMillis()
        val partitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
        val partitions = partitionsInfo.partitions.toList()

        var updatedEntityCount = 0
        var updatedPropertyCounts = 0

        entities.entries
                .groupBy({ getPartition(it.key, partitions) }, { it.toPair() })
                .mapValues { it.value.toMap() }
                .forEach { (partition, entities) ->
                    val (uec, upc) = upsertEntities(
                            connection,
                            entitySetId,
                            entities,
                            authorizedPropertyTypes,
                            version,
                            partitionsInfo,
                            partition,
                            awsPassthrough
                    )
                    updatedEntityCount += uec
                    updatedPropertyCounts += upc
                }

        logger.debug("Updated $updatedEntityCount entities and $updatedPropertyCounts properties")

        return WriteEvent(version, updatedEntityCount)
    }

    private fun upsertEntities(
            connection: Connection,
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            version: Long,
            partitionsInfo: PartitionsInfo,
            partition: Int,
            awsPassthrough: Boolean,
            linkingId: UUID?=null
    ): Pair<Int, Int> {

        //Update the versions of all entities.

        connection.autoCommit = true
        val entityKeyIdsArr = PostgresArrays.createUuidArray(connection, entities.keys)
        val versionsArrays = PostgresArrays.createLongArray(connection, arrayOf(version))

        /*
         * Our approach is to use entity level locking that takes advantage of the router executor to avoid deadlocks.
         *
         * If performance becomes an issue, we can break this is up into individual transactions at the risk of
         * ending up with partial property right and decoupled metadata updates.
         */

//        //Acquire entity key id locks
//        val rowLocks = connection.prepareStatement(lockEntitiesSql)
//        rowLocks.setArray(1, entityKeyIdsArr)
//        rowLocks.setInt(2, partition)
//        rowLocks.executeQuery()

        //Update metadata
        val upsertEntities = connection.prepareStatement(upsertEntitiesSql)

        upsertEntities.setArray(1, versionsArrays)
        upsertEntities.setLong(2, version)
        upsertEntities.setLong(3, version)
        upsertEntities.setObject(4, entitySetId)
        upsertEntities.setArray(5, entityKeyIdsArr)
        upsertEntities.setInt(6, partition)

        val updatedEntityCount = upsertEntities.executeUpdate()

        //Basic validation.
        if (updatedEntityCount != entities.size) {
            logger.warn(
                    "Update $updatedEntityCount entities. Expect to update ${entities.size} for entity set $entitySetId."
            )
            logger.debug("Entity key ids: {}", entities.keys)
        }

        val updatedProperties = upsertPropertyValues( connection, entitySetId, entities, authorizedPropertyTypes,
                partitionsInfo, partition, awsPassthrough, version, versionsArrays )

        val updatedLinkedProperties = if ( linkingId == null ){
            0
        } else {
            upsertPropertyValues( connection, entitySetId, entities, authorizedPropertyTypes, partitionsInfo, partition,
                    awsPassthrough, version, versionsArrays, linkingId )
        }

        return updatedEntityCount to ( updatedProperties + updatedLinkedProperties )
    }

    private fun upsertPropertyValues(
            connection: Connection,
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            partitionsInfo: PartitionsInfo,
            partition: Int,
            awsPassthrough: Boolean,
            version: Long,
            versionsArrays: java.sql.Array,
            linkingId: UUID?=null
    ): Int {
        //Update property values. We use multiple prepared statements in batch while re-using ARRAY[version].
        val upsertPropertyValues = mutableMapOf<UUID, PreparedStatement>()
        val isLinking = linkingId != null
        return entities.entries.map { (entityKeyId, rawValue) ->
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
                    val sql = if ( isLinking ) {
                        upsertLinkedEntityPropertyValueSql(pt)
                    } else {
                        upsertPropertyValueSql(pt)
                    }
                    connection.prepareStatement( sql )
                }

                //TODO: Keep track of collisions here. We can detect when hashes collide for an entity
                //and read the existing value to determine which colliding values need to be assigned new
                // hashes. This is fine because hashes are immutable and the front-end always requests them
                // from the backend before performing operations.

                values.map { value ->
                    //Binary data types get stored in S3 bucket
                    val (propertyHash, insertValue) =
                        if (authorizedPropertyTypes.getValue(propertyTypeId).datatype == EdmPrimitiveTypeKind.Binary) {
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
                    if ( isLinking ){
                        upsertPropertyValue.setObject(2, linkingId) // ID_VALUE
                    } else {
                        upsertPropertyValue.setObject(2, entityKeyId)
                    }
                    upsertPropertyValue.setInt(3, partition)
                    upsertPropertyValue.setObject(4, propertyTypeId)
                    upsertPropertyValue.setObject(5, propertyHash)
                    upsertPropertyValue.setLong(6, version)
                    upsertPropertyValue.setArray(7, versionsArrays)
                    upsertPropertyValue.setInt(8, partitionsInfo.partitionsVersion)
                    upsertPropertyValue.setObject(9, insertValue)
                    if ( isLinking ){
                        upsertPropertyValue.setObject(10, entityKeyId) // ORIGIN_ID
                    }
                    upsertPropertyValue.addBatch()
                }
            }
            upsertPropertyValues.values.map { it.executeBatch().sum() }.sum()
        }.sum()
    }


    fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        return hds.connection.use { connection ->
            connection.autoCommit = false
            tombstone(connection, entitySetId, entities.keys, authorizedPropertyTypes.values)
            val event = upsertEntities(connection, entitySetId, entities, authorizedPropertyTypes)
            connection.autoCommit = true
            event
        }
    }

    fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        //Only tombstone properties being replaced.
        return hds.connection.use { connection ->
            connection.autoCommit = false
            entities.forEach { (entityKeyId, entity) ->
                //Implied access enforcement as it will raise exception if lacking permission
                tombstone(
                        connection,
                        entitySetId,
                        setOf(entityKeyId),
                        entity.keys.map { authorizedPropertyTypes.getValue(it) }.toSet()
                )
            }
            val event = upsertEntities(connection, entitySetId, entities, authorizedPropertyTypes)
            connection.autoCommit = true
            event
        }
    }

    fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties: Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): WriteEvent {
        //We expect controller to have performed access control checks upstream.

        return hds.connection.use { connection ->
            connection.autoCommit = false
            tombstone(connection, entitySetId, replacementProperties)
            //This performs unnecessary copies and we should fix at some point
            val replacementValues = replacementProperties.asSequence().map {
                it.key to extractValues(
                        it.value
                )
            }.toMap()
            val event = upsertEntities(entitySetId, replacementValues, authorizedPropertyTypes)
            connection.autoCommit = true
            event
        }
    }

    private fun extractValues(propertyValues: Map<UUID, Set<Map<ByteBuffer, Any>>>): Map<UUID, Set<Any>> {
        return propertyValues.mapValues { (entityKeyId, replacements) -> replacements.flatMap { it.values }.toSet() }
    }

    /**
     * Tombstones all entities in an entity set.
     */
    private fun tombstone(conn: Connection, entitySetId: UUID): WriteEvent {
        check(!conn.autoCommit) { "Connection auto-commit must be disabled" }
        val tombstoneVersion = -System.currentTimeMillis()


        val numUpdated = conn.prepareStatement(updateVersionsForEntitySet).use { ps ->
            ps.setLong(1, tombstoneVersion)
            ps.setLong(2, tombstoneVersion)
            ps.setLong(3, tombstoneVersion)
            ps.setObject(4, entitySetId)
            ps.executeUpdate()
        }

        //We don't count property type updates.
        conn.prepareStatement(updateVersionsForPropertiesInEntitySet).use { ps ->
            ps.setLong(1, tombstoneVersion)
            ps.setLong(2, tombstoneVersion)
            ps.setLong(3, tombstoneVersion)
            ps.setObject(4, entitySetId)
            ps.executeUpdate()
        }

        return WriteEvent(tombstoneVersion, numUpdated)
    }

    /**
     * Tombstones all data from authorizedPropertyTypes for an entity set.
     *
     * NOTE: this function commits the tombstone transactions.
     */
    fun clearEntitySet(entitySetId: UUID, authorizedPropertyTypes: Map<UUID, PropertyType>): WriteEvent {
        return hds.connection.use { conn ->
            conn.autoCommit = false
            tombstone(conn, entitySetId, authorizedPropertyTypes.values)
            val event = tombstone(conn, entitySetId)
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
                    deleteEntities(entitySetId, entities, authorizedPropertyTypes, partition, partitionVersion)
                }.sum()

        return WriteEvent(System.currentTimeMillis(), numUpdates)
    }

    private fun deleteEntities(
            entitySetId: UUID,
            entities: Collection<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            partition: Int,
            partitionVersion: Int
    ): Int {
        return hds.connection.use { connection ->
            connection.autoCommit = false

            val propertyTypesArr = PostgresArrays.createUuidArray(connection, authorizedPropertyTypes.keys)

            val idsArr = PostgresArrays.createUuidArray(connection, entities)

            // Acquire entity key id locks
            val rowLocks = connection.prepareStatement(lockEntitiesSql)
            rowLocks.setArray(1, idsArr)
            rowLocks.setInt(2, partition)
            rowLocks.executeQuery()

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

    fun deletePropertyOfEntityFromS3(
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


    fun deletePropertiesInEntitySetFromS3(entitySetId: UUID, propertyTypeId: UUID): Long {
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

    fun deleteEntitySet(entitySetId: UUID): WriteEvent {
        val numUpdates = hds.connection.use {
            val ps = it.prepareStatement(deleteEntitySetEntityKeys)
            ps.setObject(1, entitySetId)
            ps.executeUpdate()
        }

        return WriteEvent(System.currentTimeMillis(), numUpdates)
    }

    fun deleteEntities(entitySetId: UUID, entityKeyIds: Set<UUID>): WriteEvent {
        val numUpdates = hds.connection.use {
            val ps = it.prepareStatement(deleteEntityKeys)
            val arr = PostgresArrays.createUuidArray(it, entityKeyIds)
            ps.setObject(1, entitySetId)
            ps.setArray(2, arr)

            ps.executeUpdate()
        }

        return WriteEvent(System.currentTimeMillis(), numUpdates)
    }

    /**
     * Tombstones the provided set of property types for each provided entity key.
     *
     * This version of tombstone only operates on the [PostgresTable.DATA] table and does not change the version of
     * entities in the [PostgresTable.IDS] table
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
            ps.setLong(1, tombstoneVersion)
            ps.setLong(2, tombstoneVersion)
            ps.setLong(3, tombstoneVersion)
            ps.setObject(4, entitySetId)
            ps.setArray(5, propertyTypeIdsArr)
            ps.executeUpdate()
        }

        return WriteEvent(tombstoneVersion, numUpdated)
    }

    /**
     * Tombstones entities in the [PostgresTable.IDS] table.
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
        val partitions = partitionsInfo.partitions.toList()
        val tombstoneVersion = -System.currentTimeMillis()
        val entityKeyIdsArr = PostgresArrays.createUuidArray(conn, entityKeyIds)
        val partitionsVersion = partitionsInfo.partitionsVersion
        val partitionsArr = PostgresArrays.createIntArray(conn, entityKeyIds.map { getPartition(it, partitions) })

        val numUpdated = conn.prepareStatement(updateVersionsForEntitiesInEntitySet).use { ps ->
            ps.setLong(1, tombstoneVersion)
            ps.setLong(2, tombstoneVersion)
            ps.setLong(3, tombstoneVersion)
            ps.setObject(4, entitySetId)
            ps.setArray(5, entityKeyIdsArr)
            ps.setArray(6, partitionsArr)
            ps.setInt(7, partitionsVersion)
            ps.executeUpdate()
        }

        return WriteEvent(tombstoneVersion, numUpdated)
    }

    /**
     * Tombstones the provided set of property types for each provided entity key.
     *
     * This version of tombstone only operates on the [PostgresTable.DATA] table and does not change the version of
     * entities in the [PostgresTable.IDS] table
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
            propertyTypesToTombstone: Collection<PropertyType>,
            partitionsInfo: PartitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
    ): WriteEvent {
        val partitions = partitionsInfo.partitions.toList()
        val tombstoneVersion = -System.currentTimeMillis()
        val propertyTypeIdsArr = PostgresArrays.createUuidArray(conn, propertyTypesToTombstone.map { it.id })
        val entityKeyIdsArr = PostgresArrays.createUuidArray(conn, entityKeyIds)
        val partitionsArr = PostgresArrays.createIntArray(conn, entityKeyIds.map { getPartition(it, partitions) })
        val partitionsVersion = partitionsInfo.partitionsVersion
        val numUpdated = conn.prepareStatement(updateVersionsForPropertyTypesInEntitiesInEntitySet).use { ps ->
            ps.setLong(1, tombstoneVersion)
            ps.setLong(2, tombstoneVersion)
            ps.setLong(3, tombstoneVersion)
            ps.setObject(4, entitySetId)
            ps.setArray(5, entityKeyIdsArr)
            ps.setArray(6, partitionsArr)
            ps.setInt(7, partitionsVersion)
            ps.setArray(8, propertyTypeIdsArr)
            ps.executeUpdate()
        }

        return WriteEvent(tombstoneVersion, numUpdated)
    }


    /**
     *
     * Tombstones the provided set of property type hash values for each provided entity key.
     *
     * This version of tombstone only operates on the [PostgresTable.DATA] table and does not change the version of
     * entities in the [PostgresTable.IDS] table
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
            connection: Connection,
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>,
            partitionsInfo: PartitionsInfo = partitionManager.getEntitySetPartitionsInfo(entitySetId)
    ): WriteEvent {
        val entityKeyIds = entities.keys
        val partitions = partitionsInfo.partitions.toList()
        val tombstoneVersion = -System.currentTimeMillis()
        val entityKeyIdsArr = PostgresArrays.createUuidArray(connection, entityKeyIds)
        val partitionsArr = PostgresArrays.createIntArray(connection, entityKeyIds.map { getPartition(it, partitions) })

        val updatePropertyValueVersion = connection.prepareStatement(
                updateVersionsForPropertyValuesInEntitiesInEntitySet
        )

        entities.forEach { (_, entity) ->
            entity.forEach { (propertyTypeId, updates) ->
                //TODO: https://github.com/pgjdbc/pgjdbc/issues/936
                //TODO: https://github.com/pgjdbc/pgjdbc/pull/1194
                //TODO: https://github.com/pgjdbc/pgjdbc/pull/1044
                //Once above issues are resolved this can be done as a single query WHERE HASH = ANY(?)
                updates
                        .flatMap { it.keys }
                        .forEach { update ->
                            updatePropertyValueVersion.setLong(1, tombstoneVersion)
                            updatePropertyValueVersion.setLong(2, tombstoneVersion)
                            updatePropertyValueVersion.setLong(3, tombstoneVersion)
                            updatePropertyValueVersion.setObject(4, entitySetId)
                            updatePropertyValueVersion.setArray(5, entityKeyIdsArr)
                            updatePropertyValueVersion.setArray(6, partitionsArr)
                            updatePropertyValueVersion.setInt(7, partitionsInfo.partitionsVersion)
                            updatePropertyValueVersion.setObject(8, propertyTypeId)
                            updatePropertyValueVersion.setBytes(9, update.array())
                            updatePropertyValueVersion.addBatch()
                        }


            }
        }
        val numUpdated = updatePropertyValueVersion.executeUpdate()




        return WriteEvent(tombstoneVersion, numUpdated)
    }
}


private fun abortInsert(entitySetId: UUID, entityKeyId: UUID): Nothing {
    throw InvalidParameterException(
            "Cannot insert property type not in authorized property types for entity $entityKeyId from entity set $entitySetId."
    )
}
