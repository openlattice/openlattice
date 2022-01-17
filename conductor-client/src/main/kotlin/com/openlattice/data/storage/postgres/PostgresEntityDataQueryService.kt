package com.openlattice.data.storage.postgres

import com.codahale.metrics.annotation.Timed
import com.geekbeast.postgres.PostgresArrays
import com.geekbeast.postgres.PostgresDatatype
import com.openlattice.analysis.requests.Filter
import com.openlattice.data.*
import com.openlattice.data.storage.*
import com.openlattice.data.storage.PostgresEntitySetSizesInitializationTask.Companion.ENTITY_SET_SIZES_VIEW
import com.openlattice.data.util.PostgresDataHasher
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.ExpirationBase
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.*
import com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID
import com.openlattice.postgres.PostgresColumn.ID
import com.openlattice.postgres.PostgresColumn.PROPERTY_TYPE_ID
import com.openlattice.postgres.PostgresColumn.VERSION
import com.openlattice.postgres.PostgresColumn.VERSIONS
import com.openlattice.postgres.PostgresTable.DATA
import com.openlattice.postgres.PostgresTable.IDS
import com.geekbeast.postgres.streams.BasePostgresIterable
import com.geekbeast.postgres.streams.PreparedStatementHolderSupplier
import com.geekbeast.postgres.streams.StatementHolderSupplier
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.nio.ByteBuffer
import java.security.InvalidParameterException
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.streams.asStream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Service
class PostgresEntityDataQueryService(
        private val dataSourceResolver: DataSourceResolver,
        private val byteBlobDataManager: ByteBlobDataManager,
) : EntityDataQueryService {
    companion object {
        private val logger = LoggerFactory.getLogger(PostgresEntityDataQueryService::class.java)

        private const val S3_DELETE_BATCH_SIZE = 10_000
        private const val EXPIRED_DATA_BATCH_SIZE = 10_000
    }

    override fun getEntitySetCounts(): Map<UUID, Long> {
        return BasePostgresIterable(
                StatementHolderSupplier(
                        dataSourceResolver.getDataSource(EntitySet.DEFAULT_DATASOURCE),
                        "SELECT * FROM $ENTITY_SET_SIZES_VIEW"
                )
        ) { ResultSetAdapters.entitySetId(it) to ResultSetAdapters.count(it) }.toMap()
    }

    @JvmOverloads
    override fun getEntitiesWithPropertyTypeIds(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long>
    ): Iterable<Pair<UUID, MutableMap<UUID, MutableSet<Any>>>> {
        return getEntitySetIterable(
                entityKeyIds,
                authorizedPropertyTypes,
                propertyTypeFilters,
                metadataOptions,
                version
        ) { rs ->
            getEntityPropertiesByPropertyTypeId(rs, authorizedPropertyTypes, metadataOptions, byteBlobDataManager)
        }
    }

    @JvmOverloads
    override fun getLinkedEntitiesWithPropertyTypeIds(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long>
    ): Iterable<Pair<UUID, MutableMap<UUID, MutableSet<Any>>>> {
        return getEntitySetIterable(
                entityKeyIds,
                authorizedPropertyTypes,
                propertyTypeFilters,
                metadataOptions,
                version,
                linking = true
        ) { rs ->
            getEntityPropertiesByPropertyTypeId(rs, authorizedPropertyTypes, metadataOptions, byteBlobDataManager)
        }
    }

    override fun getEntitySetWithPropertyTypeIdsIterable(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption>
    ): Iterable<Pair<UUID, MutableMap<UUID, MutableSet<Any>>>> {
        return getEntitiesWithPropertyTypeIds(entityKeyIds, authorizedPropertyTypes, mapOf(), metadataOptions)
    }

    /**
     * Returns linked entity set data detailed in a Map mapped by linking id, (normal) entity set id, origin id,
     * property type id and values respectively.
     */
    @JvmOverloads
    override fun getLinkedEntitiesByEntitySetIdWithOriginIds(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>,
            propertyTypeFilters: Map<UUID, Set<Filter>>,
            version: Optional<Long>
    ): Iterable<Pair<UUID, Pair<UUID, Map<UUID, MutableMap<UUID, MutableSet<Any>>>>>> {
        return getEntitySetIterable(
                entityKeyIds,
                authorizedPropertyTypes,
                propertyTypeFilters,
                metadataOptions,
                version,
                linking = true,
                detailed = true
        ) { rs ->
            getEntityPropertiesByEntitySetIdOriginIdAndPropertyTypeId(
                    rs, authorizedPropertyTypes, metadataOptions, byteBlobDataManager
            )
        }
    }

    /**
     * Returns linked entity set data detailed in a Map mapped by linking id, (normal) entity set id, origin id,
     * property type full qualified name and values respectively.
     */
    @JvmOverloads
    override fun getLinkedEntitySetBreakDown(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>>,
            version: Optional<Long>
    ): Iterable<Pair<UUID, Pair<UUID, Map<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>>>>> {
        return getEntitySetIterable(
                entityKeyIds,
                authorizedPropertyTypes,
                propertyTypeFilters,
                EnumSet.noneOf(MetadataOption::class.java),
                version,
                linking = true,
                detailed = true
        ) { rs ->
            getEntityPropertiesByEntitySetIdOriginIdAndPropertyTypeFqn(
                    rs, authorizedPropertyTypes, EnumSet.noneOf(MetadataOption::class.java), byteBlobDataManager
            )
        }
    }

    @JvmOverloads
    override fun getEntitiesWithPropertyTypeFqns(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long>,
            linking: Boolean,
            filteredDataPageDefinition: FilteredDataPageDefinition?
    ): Map<UUID, MutableMap<FullQualifiedName, MutableSet<Any>>> {
        val entitiesById = getEntitySetIterable(
                entityKeyIds,
                authorizedPropertyTypes,
                propertyTypeFilters,
                metadataOptions,
                version,
                linking,
                false,
                filteredDataPageDefinition
        ) { rs ->
            getEntityPropertiesByFullQualifiedName(
                    rs,
                    authorizedPropertyTypes,
                    metadataOptions,
                    byteBlobDataManager
            )
        }

        if (!linking) {
            return entitiesById.toMap()
        }

        return entitiesById.toList().groupBy { it.first }.mapValues { (_, normalEntitiesByLinkingId) ->
            normalEntitiesByLinkingId.flatMap { it.second.entries }.groupBy { it.key }.mapValues { (_, valuesByFqn) ->
                valuesByFqn.flatMap { it.value }.toMutableSet()
            }.toMutableMap()
        }
    }

    /**
     * Note: for linking queries, linking id and entity set id will be returned, thus data won't be merged by linking id
     */
    override fun <T> getEntitySetIterable(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            propertyTypeFilters: Map<UUID, Set<Filter>>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long>,
            linking: Boolean,
            detailed: Boolean,
            filteredDataPageDefinition: FilteredDataPageDefinition?,
            adapter: (ResultSet) -> T
    ): Iterable<T> {
        val propertyTypes = authorizedPropertyTypes.values.flatMap { it.values }.associateBy { it.id }
        val entitySetIds = entityKeyIds.keys

        return entitySetIds
                .groupBy { dataSourceResolver.getDataSourceName(it) }
                .flatMap { (dataSourceName, entitySetIdsForDataSource) ->
                    val (sql, binders) = buildPreparableFiltersSql(
                            propertyTypes,
                            propertyTypeFilters,
                            metadataOptions,
                            linking,
                            entitySetIdsForDataSource.toSet(),
                            entitySetIdsForDataSource.flatMap { entityKeyIds.getValue(it).orElse(emptySet()) }.toSet(),
                            detailed,
                            filteredDataPageDefinition

                    )

                    BasePostgresIterable(
                        PreparedStatementHolderSupplier(
                                    dataSourceResolver.getDataSource(dataSourceName),
                                    sql,
                                    FETCH_SIZE
                            ) { ps -> binders.forEach { it.bind(ps) } },
                        adapter
                    ).toList()
                }
    }

    /**
     * Updates or insert entities.
     * @param entitySetId The entity set id for which to insert entities for.
     * @param entities The entites to update or insert.
     * @param authorizedPropertyTypes The authorized property types for the insertion.
     * @param awsPassthrough True if the data will be stored directly in AWS via another means and all that is being
     * provided is the s3 prefix and key.
     * @param partitions Contains the partition information for the requested entity set.
     *
     * @return A write event summarizing the results of performing this operation.
     */
    @Timed
    override fun upsertEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            awsPassthrough: Boolean,
            propertyUpdateType: PropertyUpdateType,
    ): WriteEvent {
        val version = System.currentTimeMillis()

        val tombstoneFn = { _: Long, _: Map<UUID, Map<UUID, Set<Any>>> -> }

        return upsertEntities(
                entitySetId,
                tombstoneFn,
                entities,
                authorizedPropertyTypes,
                version,
                awsPassthrough,
                propertyUpdateType
        )
    }

    /**
     * Updates or insert entities.
     * @param entitySetId The entity set id for which to insert entities.
     * @param tombstoneFn A function that may tombstone values before performing the upsert.
     * @param entities The entities to update or insert.
     * @param authorizedPropertyTypes The authorized property types for the insertion.
     * @param version The version to use for upserting.
     * @param partitions Contains the partition information for the requested entity set.
     * @param awsPassthrough True if the data will be stored directly in AWS via another means and all that is being
     * provided is the s3 prefix and key.
     *
     * @return A write event summarizing the results of performing this operation.
     */
    override fun upsertEntities(
            entitySetId: UUID,
            tombstoneFn: (version: Long, entityBatch: Map<UUID, Map<UUID, Set<Any>>>) -> Unit,
            entities: Map<UUID, Map<UUID, Set<Any>>>, // ekids ->
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            version: Long,
            awsPassthrough: Boolean,
            propertyUpdateType: PropertyUpdateType
    ): WriteEvent {
        var updatedEntityCount = 0
        var updatedPropertyCounts = 0


        val batch = entities.entries

        var entityBatch = batch.associate { it.key to it.value }

        if (!awsPassthrough) {
            entityBatch = entityBatch.mapValues {
                JsonDeserializer.validateFormatAndNormalize(
                        it.value,
                        authorizedPropertyTypes
                ) { "Entity set $entitySetId with entity key id ${it.key}" }
            }
        }

        tombstoneFn(version, entityBatch)

        val upc = upsertEntities(
                entitySetId,
                entityBatch,
                authorizedPropertyTypes,
                version + 1,
                awsPassthrough,
                propertyUpdateType
        )

        //For now we can't track how many entities were updated in a call transactionally.
        //If we want to check how many entities were written at a specific version that is possible but
        //expensive.
        updatedEntityCount += batch.size
        updatedPropertyCounts += upc


        logger.debug("Updated $updatedEntityCount entities and $updatedPropertyCounts properties")

        return WriteEvent(version, updatedEntityCount)
    }

    override fun upsertEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            version: Long,
            awsPassthrough: Boolean,
            propertyUpdateType: PropertyUpdateType
    ): Int {

        val entitiesWithHashAndInsertData = entities.mapValues { entityKeyIdToEntity ->
            entityKeyIdToEntity.value.mapValues { propertyTypeIdToPropertyValues ->
                propertyTypeIdToPropertyValues.value.map { propertyValue ->
                    getPropertyHash(
                            entitySetId,
                            entityKeyIdToEntity.key,
                            propertyTypeIdToPropertyValues.key,
                            propertyValue,
                            authorizedPropertyTypes.getValue(propertyTypeIdToPropertyValues.key).datatype,
                            awsPassthrough
                    )
                }
            }
        }
        val hds = dataSourceResolver.resolve(entitySetId)

        return hds.connection.use { connection ->
            //Update the versions of all entities.
            val versionsArrays = PostgresArrays.createLongArray(connection, version)

            /*
             * We do not need entity level locking as the version in the ids table ensures that data is consistent even
             * if the follow property upserts fails halfway through.
             *
             * Previous me said deletes had to be handled specially, but it makes sense that clear is fine.
             *
             */

            //Update property values. We use multiple prepared statements in batch while re-using ARRAY[version].
            val upsertPropertyValues = mutableMapOf<UUID, PreparedStatement>()
            val updatedPropertyCounts = entitiesWithHashAndInsertData.entries.map { (entityKeyId, entityData) ->

                entityData.map { (propertyTypeId, hashAndInsertValue) ->
                    val upsertPropertyValue = upsertPropertyValues.getOrPut(propertyTypeId) {
                        val pt = authorizedPropertyTypes[propertyTypeId] ?: abortInsert(entitySetId, entityKeyId)
                        connection.prepareStatement(upsertPropertyValueSql(pt, propertyUpdateType))
                    }

                    hashAndInsertValue.map { (propertyHash, insertValue) ->
                        upsertPropertyValue.setObject(1, entitySetId)
                        upsertPropertyValue.setObject(2, entityKeyId)
                        upsertPropertyValue.setObject(3, propertyTypeId)
                        upsertPropertyValue.setBytes(4, propertyHash)
                        upsertPropertyValue.setObject(5, version)
                        upsertPropertyValue.setArray(6, versionsArrays)
                        upsertPropertyValue.setObject(7, insertValue)
                        upsertPropertyValue.addBatch()
                    }
                }
                upsertPropertyValues.values.map { it.executeBatch().sum() }.sum()
            }.sum()

            /**
             * At this point, we either need to either commit all versions by updating the version in the ids table our
             * fail out. Dead locks should be impossible due to explicit locking within the transaction.
             *
             */

            //Make data visible by marking new version in ids table.

            val ps = connection.prepareStatement(updateEntitySql)

            entities.keys.sorted().forEach { entityKeyId ->
                ps.setArray(1, versionsArrays)
                ps.setObject(2, version)
                ps.setObject(3, version)
                ps.setObject(4, entitySetId)
                ps.setObject(5, entityKeyId)
                ps.addBatch()
            }

            val updatedEntities = ps.executeBatch().sum()

            logger.debug("Updated $updatedEntities entities as part of insert.")
            return@use updatedPropertyCounts
        }
    }

    override fun getPropertyHash(
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
        val binaryData = value as BinaryObjectWithMetadata
        val digest = PostgresDataHasher.hashObjectToHex(binaryData.data, EdmPrimitiveTypeKind.Binary)

        //store entity set id/entity key id/property type id/property hash as key in S3
        val s3Key = ByteBlobDataManager.generateS3Key(entitySetId, entityKeyId, propertyTypeId, digest)

        byteBlobDataManager.putObject(s3Key, binaryData)
        return PostgresDataHasher.hashObject(s3Key, EdmPrimitiveTypeKind.String) to s3Key
    }

    @Timed
    override fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            propertyUpdateType: PropertyUpdateType,
    ): WriteEvent {

        val propertyTypes = authorizedPropertyTypes.values

        val tombstoneFn: (Long, Map<UUID, Map<UUID, Set<Any>>>) ->
        Unit = { version: Long,
                 entityBatch: Map<UUID, Map<UUID, Set<Any>>> ->
            tombstone(
                    entitySetId,
                    entityBatch.keys,
                    propertyTypes,
                    version,
            )
        }

        return upsertEntities(
                entitySetId,
                tombstoneFn,
                entities,
                authorizedPropertyTypes,
                System.currentTimeMillis(),
                propertyUpdateType = propertyUpdateType
        )
    }

    @Timed
    override fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            propertyUpdateType: PropertyUpdateType,
    ): WriteEvent {

        // Is the overhead from including irrelevant property types in a bulk delete really worse than performing individual queries? :thinking-face:
        val tombstoneFn =
                { version: Long,
                  entityBatch: Map<UUID, Map<UUID, Set<Any>>> ->
                    entityBatch.forEach { (entityKeyId, entity) ->
                        //Implied access enforcement as it will raise exception if lacking permission
                        tombstone(
                                entitySetId,
                                setOf(entityKeyId),
                                entity.keys.map { authorizedPropertyTypes.getValue(it) }.toSet(),
                                version
                        )
                    }
                }

        return upsertEntities(
                entitySetId,
                tombstoneFn,
                entities,
                authorizedPropertyTypes,
                System.currentTimeMillis(),
                propertyUpdateType = propertyUpdateType
        )
    }

    @Timed
    override fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties: Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>, // ekid -> ptid -> hashes -> shit
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            propertyUpdateType: PropertyUpdateType
    ): WriteEvent {
        //We expect controller to have performed access control checks upstream.

        val tombstoneFn: (Long, Map<UUID, Map<UUID, Set<Any>>>) -> Unit =
                { version: Long, entityBatch: Map<UUID, Map<UUID, Set<Any>>> ->
                    val ids = entityBatch.keys
                    tombstoneEntityPropertyHashes(
                            entitySetId,
                            replacementProperties.filter { ids.contains(it.key) },
                            version
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
                propertyUpdateType = propertyUpdateType
        )

    }

    override fun extractValues(propertyValues: Map<UUID, Set<Map<ByteBuffer, Any>>>): Map<UUID, Set<Any>> {
        return propertyValues.mapValues { (_, replacements) -> replacements.flatMap { it.values }.toSet() }
    }

    /**
     * Tombstones (writes a negative version) for the provided entity properties.
     * @param entitySetId The entity set to operate on.
     * @param entityKeyIds The entity key ids to tombstone.
     * @param authorizedPropertyTypes The property types the user is requested and is allowed to tombstone. We assume
     * that authorization checks are enforced at a higher level and that this just streamlines issuing the necessary
     * queries.
     */
    override fun clearEntityData(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
    ): WriteEvent {
        val version = System.currentTimeMillis()

        return tombstone(
                entitySetId,
                entityKeyIds,
                authorizedPropertyTypes.values,
                version
        )
    }

    /**
     * Deletes properties of entities in entity set from [DATA] table.
     */
    override fun deleteEntityData(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
    ): WriteEvent {
        // TODO same as deleteEntityDataAndEntities?
        // Delete properties from S3
        authorizedPropertyTypes.map { property ->
            if (property.value.datatype == EdmPrimitiveTypeKind.Binary) {
                deletePropertyOfEntityFromS3(entitySetId, entityKeyIds, property.key)
            }
        }
        val numUpdates = deletePropertiesFromEntities(
                entitySetId,
                entityKeyIds,
                authorizedPropertyTypes,
        )

        return WriteEvent(System.currentTimeMillis(), numUpdates)
    }

    /**
     * Deletes properties of entities in entity set from [DATA] table.
     */
    override fun deletePropertiesFromEntities(
            entitySetId: UUID,
            entities: Collection<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
    ): Int {
        val hds = dataSourceResolver.resolve(entitySetId)
        return hds.connection.use { connection ->

            val propertyTypesArr = PostgresArrays.createUuidArray(connection, authorizedPropertyTypes.keys)
            val idsArr = PostgresArrays.createUuidArray(connection, entities)

            // Delete entity and linked entity properties from data table
            connection
                    .prepareStatement(deletePropertiesOfEntitiesInEntitySet)
                    .use { ps ->
                        ps.setObject(1, entitySetId)
                        ps.setArray(2, idsArr)
                        ps.setArray(3, idsArr)
                        ps.setArray(4, propertyTypesArr)

                        ps.executeUpdate()
                    }
        }
    }

    override fun deletePropertyOfEntityFromS3(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            propertyTypeId: UUID
    ) {
        val count = AtomicLong()
        val hds = dataSourceResolver.resolve(entitySetId)
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

    /**
     * Deletes entities from [IDS] table.
     */
    override fun deleteEntities(entitySetId: UUID, entityKeyIds: Set<UUID>): WriteEvent {
        val hds = dataSourceResolver.resolve(entitySetId)

        val numUpdates = hds.connection.use { connection ->
            try {
                val ps = connection.prepareStatement(deleteEntityKeys)
                val entityArr = PostgresArrays.createUuidArray(ps.connection, entityKeyIds)
                ps.setObject(1, entitySetId)
                ps.setArray(2, entityArr)
                ps.executeUpdate()
            } catch (ex: Exception) {
                logger.error("Unable to delete entities ($entityKeyIds) in $entitySetId.")
                throw ex
            }
        }

        return WriteEvent(System.currentTimeMillis(), numUpdates)
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
     * @param partitions Contains the partition info for the requested entity set.
     *
     * @return A write event object containing a summary of the operation useful for auditing purposes.
     */
    override fun tombstone(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            propertyTypesToTombstone: Collection<PropertyType>,
            version: Long,
    ): WriteEvent {
        val hds = dataSourceResolver.resolve(entitySetId)

        return hds.connection.use { conn ->
            val propertyTypeIdsArr = PostgresArrays.createUuidArray(conn, propertyTypesToTombstone.map { it.id })
            val entityKeyIdsArr = PostgresArrays.createUuidArray(conn, entityKeyIds)

            val numUpdated = conn.prepareStatement(
                    updateVersionsForPropertyTypesInEntitiesInEntitySet()
            ).use { ps ->
                ps.setLong(1, -version)
                ps.setLong(2, -version)
                ps.setLong(3, -version)
                ps.setObject(4, entitySetId)
                ps.setArray(5, propertyTypeIdsArr)
                ps.setArray(6, entityKeyIdsArr)
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
     * @param version The version to use to tombstone.
     *
     * @return A write event object containing a summary of the operation useful for auditing purposes.
     *
     */
    override fun tombstoneEntityPropertyHashes(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>,
            version: Long,
    ): WriteEvent {
        val hds = dataSourceResolver.resolve(entitySetId)

        return hds.connection.use { conn ->
            val entityKeyIds = entities.keys
            val entityKeyIdsArr = PostgresArrays.createUuidArray(conn, entityKeyIds)

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
                                updatePropertyValueVersion.setBytes(7, update.array())
                                updatePropertyValueVersion.addBatch()

                                tombstoneLinks.setLong(1, -version)
                                tombstoneLinks.setLong(2, -version)
                                tombstoneLinks.setLong(3, -version)
                                tombstoneLinks.setObject(4, entitySetId)
                                tombstoneLinks.setArray(5, propertyTypeIdsArr)
                                tombstoneLinks.setArray(6, entityKeyIdsArr)
                                tombstoneLinks.setBytes(7, update.array())
                                tombstoneLinks.addBatch()
                            }
                }
            }
            val numUpdated = updatePropertyValueVersion.executeUpdate()
            val linksUpdated = tombstoneLinks.executeUpdate()

            WriteEvent(version, numUpdated + linksUpdated)
        }

    }

    override fun getExpiringEntitiesFromEntitySetUsingIds(
            entitySetId: UUID,
            expirationPolicy: DataExpiration,
            currentDateTime: OffsetDateTime
    ): BasePostgresIterable<UUID> {
        val hds =
                dataSourceResolver.resolve(entitySetId)

        return BasePostgresIterable(
                PreparedStatementHolderSupplier(hds, getExpiringEntitiesUsingIdsQuery(expirationPolicy)) { ps ->
                    ps.setObject(1, entitySetId)
                    bindExpirationDate(ps, 2, expirationPolicy, currentDateTime)
                }
        ) { rs -> ResultSetAdapters.id(rs) }
    }

    override fun getExpiringEntitiesFromEntitySetUsingData(
            entitySetId: UUID,
            expirationPolicy: DataExpiration,
            expirationPropertyType: PropertyType,
            currentDateTime: OffsetDateTime
    ): BasePostgresIterable<UUID> {
        val hds = dataSourceResolver.resolve(entitySetId)
        return BasePostgresIterable(
                PreparedStatementHolderSupplier(
                        hds, getExpiringEntitiesUsingDataQuery(
                        expirationPropertyType, expirationPolicy.deleteType
                )
                ) { ps ->
                    ps.setObject(1, entitySetId)
                    bindExpirationDate(ps, 2, expirationPolicy, currentDateTime, expirationPropertyType)
                    ps.setObject(3, expirationPropertyType.id)
                }
        ) { rs -> ResultSetAdapters.id(rs) }
    }

    override fun bindExpirationDate(
            ps: PreparedStatement,
            index: Int,
            expirationPolicy: DataExpiration,
            currentDateTime: OffsetDateTime,
            propertyType: PropertyType?
    ) {
        val expirationDateTime = currentDateTime.toInstant().minusMillis(expirationPolicy.timeToExpiration)

        when (expirationPolicy.expirationBase) {
            ExpirationBase.FIRST_WRITE -> {
                ps.setLong(index, expirationDateTime.toEpochMilli())
            }
            ExpirationBase.LAST_WRITE -> {
                ps.setObject(index, OffsetDateTime.ofInstant(expirationDateTime, ZoneId.systemDefault()))
            }
            else -> {
                val sqlFormat =
                        if (propertyType!!.datatype == EdmPrimitiveTypeKind.Date) Types.DATE else Types.TIMESTAMP_WITH_TIMEZONE
                ps.setObject(index, OffsetDateTime.ofInstant(expirationDateTime, ZoneId.systemDefault()), sqlFormat)
            }
        }
    }

    /**
     * PreparedStatement bind order:
     *
     * 1) entitySetId
     * 2) partitions
     * 3) expiration date(time)
     * 4) propertyTypeId
     */
    override fun getExpiringEntitiesUsingDataQuery(
            expirationPropertyType: PropertyType, deleteType: DeleteType
    ): String {
        val clearedEntitiesClause = if (deleteType == DeleteType.Soft) "AND ${VERSION.name} >= 0 " else ""

        val expirationColumnName = PostgresDataTables.getColumnDefinition(
                expirationPropertyType.postgresIndexType,
                expirationPropertyType.datatype
        ).name

        return """
            SELECT DISTINCT ${ID.name} FROM ${DATA.name}
            WHERE ${ENTITY_SET_ID.name} = ?
            AND $expirationColumnName <= ?
            AND ${PROPERTY_TYPE_ID.name} = ?
            $clearedEntitiesClause
            LIMIT $EXPIRED_DATA_BATCH_SIZE
        """.trimIndent()
    }

    /**
     * PreparedStatement bind order:
     *
     * 1) entitySetId
     * 2) partitions
     * 3) expiration datetime
     */
    override fun getExpiringEntitiesUsingIdsQuery(expirationPolicy: DataExpiration): String {
        val clearedEntitiesClause =
                if (expirationPolicy.deleteType == DeleteType.Soft) "AND ${VERSION.name} >= 0 " else ""

        val expirationField = when (expirationPolicy.expirationBase) {
            ExpirationBase.FIRST_WRITE -> "(SELECT MIN(v) FROM UNNEST(${VERSIONS.name}) AS v WHERE v > 0)" //gets the first  version from the versions column
            ExpirationBase.LAST_WRITE -> DataTables.LAST_WRITE.name
            else -> throw IllegalArgumentException(
                    "Loading expired entities using ids is not supported for expiration base ${expirationPolicy.expirationBase}"
            )
        }

        return """
            SELECT ${ID.name} FROM ${IDS.name}
            WHERE ${ENTITY_SET_ID.name} = ?
            AND $expirationField <= ?
            $clearedEntitiesClause
            LIMIT $EXPIRED_DATA_BATCH_SIZE
        """.trimIndent()
    }
}

private fun abortInsert(entitySetId: UUID, entityKeyId: UUID): Nothing {
    throw InvalidParameterException(
            "Cannot insert property type not in authorized property types for entity $entityKeyId from entity set $entitySetId."
    )
}
