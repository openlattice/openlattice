/*
 * Copyright (C) 2018. OpenLattice, Inc.
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

package com.openlattice.data.storage

import com.google.common.base.Preconditions.checkState
import com.google.common.collect.Multimaps.asMap
import com.google.common.collect.SetMultimap
import com.openlattice.data.util.PostgresDataHasher
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*
import java.util.function.Function
import java.util.function.Supplier

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
const val MAX_PREV_VERSION = "max_prev_version"
const val EXPANDED_VERSIONS = "expanded_versions"
const val FETCH_SIZE = 100000
private val logger = LoggerFactory.getLogger(PostgresEntityDataQueryService::class.java)

class PostgresEntityDataQueryService(
        private val hds: HikariDataSource,
        private val byteBlobDataManager: ByteBlobDataManager
) {
    fun getEntitiesById(
            entitySetId: UUID,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            entityKeyIds: Set<UUID>
    ): Map<UUID, Map<UUID, Set<Any>>> {
        val adapter = Function<ResultSet, Pair<UUID, Map<UUID, Set<Any>>>> {
            ResultSetAdapters.id(it) to
                    ResultSetAdapters.implicitEntityValuesById(
                            it, mapOf(entitySetId to authorizedPropertyTypes), byteBlobDataManager
                    )
        }
        return streamableEntitySet(
                mapOf(entitySetId to Optional.of(entityKeyIds)), mapOf(entitySetId to authorizedPropertyTypes),
                EnumSet.noneOf(MetadataOption::class.java), Optional.empty(), adapter
        ).toMap()
    }

    /**
     * Returns linked entity data for (entity_set_id, linking_id) pairs
     */
    fun getLinkedEntityData(
            linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>
    ): PostgresIterable<Pair<Pair<UUID, UUID>, Map<UUID, Set<Any>>>> {
        val adapter = Function<ResultSet, Pair<Pair<UUID, UUID>, Map<UUID, Set<Any>>>> {
            Pair(ResultSetAdapters.linkingId(it), ResultSetAdapters.entitySetId(it)) to
                    ResultSetAdapters.implicitEntityValuesById(
                            it, authorizedPropertyTypesByEntitySetId, byteBlobDataManager
                    )
        }
        return streamableEntitySet(
                linkingIdsByEntitySetId, authorizedPropertyTypesByEntitySetId,
                EnumSet.noneOf(MetadataOption::class.java), Optional.empty(), adapter, true
        )
    }

    fun getEntitiesByIdWithLastWrite(
            entitySetId: UUID,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            entityKeyIds: Set<UUID>
    ): Map<UUID, Map<UUID, Set<Any>>> {
        val adapter = Function<ResultSet, Pair<UUID, Map<UUID, Set<Any>>>> {
            ResultSetAdapters.id(it) to ResultSetAdapters.implicitEntityValuesByIdWithLastWrite(
                    it, authorizedPropertyTypes, byteBlobDataManager
            )
        }
        return streamableEntitySet(
                mapOf(entitySetId to Optional.of(entityKeyIds)), mapOf(entitySetId to authorizedPropertyTypes),
                EnumSet.of(MetadataOption.LAST_WRITE), Optional.empty(), adapter
        ).toMap()
    }

    fun streamableEntitySet(
            entitySetIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long> = Optional.empty(),
            linking: Boolean = false
    ): PostgresIterable<SetMultimap<FullQualifiedName, Any>> {
        return if (linking) {
            streamableLinkingEntitySet(
                    entitySetIds.map { it to Optional.empty<Set<UUID>>() }.toMap(),
                    authorizedPropertyTypes,
                    metadataOptions,
                    version
            )
        } else {
            streamableEntitySet(
                    entitySetIds.map { it to Optional.empty<Set<UUID>>() }.toMap(),
                    authorizedPropertyTypes,
                    metadataOptions,
                    version
            )
        }
    }

    @JvmOverloads
    fun streamableEntitySet(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long> = Optional.empty(),
            linking: Boolean = false
    ): PostgresIterable<SetMultimap<FullQualifiedName, Any>> {
        return if (linking) {
            streamableLinkingEntitySet(
                    mapOf(entitySetId to Optional.of(entityKeyIds)),
                    authorizedPropertyTypes,
                    metadataOptions,
                    version
            )
        } else {
            streamableEntitySet(
                    mapOf(entitySetId to Optional.of(entityKeyIds)),
                    authorizedPropertyTypes,
                    metadataOptions,
                    version
            )
        }
    }

    fun entitySetDataWithEntityKeyIdsAndPropertyTypeIds(
            entitySetId: UUID,
            entityKeyIds: Optional<Set<UUID>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long> = Optional.empty()
    ): Map<UUID, Map<UUID, Set<Any>>> {
        return streamableEntitySetWithEntityKeyIdsAndPropertyTypeIds(
                entitySetId,
                entityKeyIds,
                authorizedPropertyTypes,
                metadataOptions,
                version
        ).toMap()
    }

    fun streamableEntitySetWithEntityKeyIdsAndPropertyTypeIds(
            entitySetId: UUID,
            entityKeyIds: Optional<Set<UUID>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long> = Optional.empty()
    ): PostgresIterable<Pair<UUID, Map<UUID, Set<Any>>>> {
        val adapter = Function<ResultSet, Pair<UUID, Map<UUID, Set<Any>>>> {
            ResultSetAdapters.id(it) to
                    ResultSetAdapters.implicitEntityValuesById(
                            it, mapOf(entitySetId to authorizedPropertyTypes), byteBlobDataManager
                    )
        }
        return streamableEntitySet(
                mapOf(entitySetId to entityKeyIds), mapOf(entitySetId to authorizedPropertyTypes), metadataOptions,
                version, adapter
        )
    }

    fun streamableEntitySetWithEntityKeyIdsAndPropertyTypeIds(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): PostgresIterable<org.apache.commons.lang3.tuple.Pair<UUID, Map<FullQualifiedName, Set<Any>>>> {
        val adapter = Function<ResultSet, org.apache.commons.lang3.tuple.Pair<UUID, Map<FullQualifiedName, Set<Any>>>> {
            org.apache.commons.lang3.tuple.Pair.of(
                    ResultSetAdapters.id(it),
                    ResultSetAdapters.implicitEntityValuesByFqn(it, authorizedPropertyTypes, byteBlobDataManager)
            )
        }
        return streamableEntitySet(
                mapOf(entitySetId to Optional.of(entityKeyIds)), mapOf(entitySetId to authorizedPropertyTypes),
                EnumSet.noneOf(MetadataOption::class.java), Optional.empty<Long>(), adapter
        )
    }

    fun streamableEntitySet(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long> = Optional.empty()
    ): PostgresIterable<SetMultimap<FullQualifiedName, Any>> {
        val adapter = Function<ResultSet, SetMultimap<FullQualifiedName, Any>> {
            ResultSetAdapters.implicitNormalEntity(it, authorizedPropertyTypes, metadataOptions, byteBlobDataManager)
        }
        return streamableEntitySet(entityKeyIds, authorizedPropertyTypes, metadataOptions, version, adapter)
    }

    /**
     * Returns linked entity data for each linking id, omitting entity set id from selected columns
     */
    fun streamableLinkingEntitySet(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long> = Optional.empty()
    ): PostgresIterable<SetMultimap<FullQualifiedName, Any>> {
        val adapter = Function<ResultSet, SetMultimap<FullQualifiedName, Any>> {
            ResultSetAdapters.implicitLinkedEntity(it, authorizedPropertyTypes, metadataOptions, byteBlobDataManager)
        }
        return streamableEntitySet(entityKeyIds, authorizedPropertyTypes, metadataOptions, version, adapter, true, true)
    }

    /*
    Note: for linking queries, linking id and entity set id will be returned, thus data won't be merged by linking id
     */
    private fun <T> streamableEntitySet(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long>,
            adapter: Function<ResultSet, T>,
            linking: Boolean = false,
            omitEntitySetId: Boolean = false
    ): PostgresIterable<T> {
        return PostgresIterable(
                Supplier<StatementHolder> {
                    val connection = hds.connection
                    val statement = connection.createStatement()
                    statement.fetchSize = FETCH_SIZE

                    val allPropertyTypes = authorizedPropertyTypes.values.flatMap { it.values }.toSet()
                    val binaryPropertyTypes = allPropertyTypes
                            .associate { it.id to (it.datatype == EdmPrimitiveTypeKind.Binary) }
                    val propertyFqns = allPropertyTypes.map {
                        it.id to quote(
                                it.type.fullQualifiedNameAsString
                        )
                    }.toMap()

                    val rs = statement.executeQuery(
                            if (version.isPresent) {

                                selectEntitySetWithPropertyTypesAndVersionSql(
                                        entityKeyIds,
                                        propertyFqns,
                                        allPropertyTypes.map { it.id },
                                        authorizedPropertyTypes.mapValues { it.value.map { it.key }.toSet() },
                                        mapOf(),
                                        metadataOptions,
                                        version.get(),
                                        binaryPropertyTypes,
                                        linking,
                                        omitEntitySetId

                                )
                            } else {
                                selectEntitySetWithCurrentVersionOfPropertyTypes(
                                        entityKeyIds,
                                        propertyFqns,
                                        allPropertyTypes.map { it.id },
                                        authorizedPropertyTypes.mapValues { it.value.map { it.key }.toSet() },
                                        mapOf(),
                                        metadataOptions,
                                        binaryPropertyTypes,
                                        linking,
                                        omitEntitySetId
                                )
                            }
                    )
                    StatementHolder(connection, statement, rs)
                },
                adapter
        )
    }

    /**
     * Selects linking ids by their entity set ids with filtering on entity key ids.
     */
    fun getLinkingIds(entityKeyIds: Map<UUID, Optional<Set<UUID>>>): Map<UUID, Set<UUID>> {
        val adapter = Function<ResultSet, Pair<UUID, Set<UUID>>> {
            Pair(ResultSetAdapters.entitySetId(it), ResultSetAdapters.linkingIds(it))
        }
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.connection
            val statement = connection.createStatement()
            statement.fetchSize = FETCH_SIZE

            val rs = statement.executeQuery(selectLinkingIdsOfEntities(entityKeyIds))
            StatementHolder(connection, statement, rs)
        }, adapter).toMap()
    }

    fun getLinkingIds(entitySetId: UUID): PostgresIterable<UUID> {
        val adapter = Function<ResultSet, UUID> { ResultSetAdapters.linkingId(it) }
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.connection
            val statement = connection.createStatement()
            statement.fetchSize = FETCH_SIZE

            val rs = statement.executeQuery(selectLinkingIdsOfEntitySet(entitySetId))
            StatementHolder(connection, statement, rs)
        }, adapter)
    }

    fun getEntityKeyIdsOfLinkingIds(
            linkingIds: Set<UUID>
    ): PostgresIterable<org.apache.commons.lang3.tuple.Pair<UUID, Set<UUID>>> {
        val adapter = Function<ResultSet, org.apache.commons.lang3.tuple.Pair<UUID, Set<UUID>>> {
            org.apache.commons.lang3.tuple.Pair.of(ResultSetAdapters.linkingId(it), ResultSetAdapters.entityKeyIds(it))
        }
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.connection
            val statement = connection.createStatement()
            statement.fetchSize = FETCH_SIZE

            val rs = statement.executeQuery(selectEntityKeyIdsByLinkingIds(linkingIds))
            StatementHolder(connection, statement, rs)
        }, adapter)
    }

    fun getLinkingEntitySetIds(linkingId: UUID): PostgresIterable<UUID> {
        val adapter = Function<ResultSet, UUID> {
            ResultSetAdapters.id(it)
        }
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.connection
            val statement = connection.createStatement()
            val rs = statement.executeQuery(getLinkingEntitySetIdsOfLinkingIdQuery(linkingId))
            StatementHolder(connection, statement, rs)
        }, adapter)
    }

    fun getLinkingEntitySetIdsOfEntitySet(entitySetId: UUID): PostgresIterable<UUID> {
        val adapter = Function<ResultSet, UUID> {
            ResultSetAdapters.id(it)
        }
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.connection
            val statement = connection.createStatement()
            val rs = statement.executeQuery(getLinkingEntitySetIdsOfEntitySetIdQuery(entitySetId))
            StatementHolder(connection, statement, rs)
        }, adapter)
    }

    fun upsertEntities(
            entitySetId: UUID, entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        hds.connection.use { connection ->
            val version = System.currentTimeMillis()
            val entitySetPreparedStatement = connection.prepareStatement(upsertEntity(entitySetId, version))
            val dataTypes = authorizedPropertyTypes.mapValues { (_, propertyType) ->
                propertyType.datatype
            }

            val preparedStatements = authorizedPropertyTypes
                    .mapValues {
                        connection.prepareStatement(
                                upsertPropertyValues(
                                        entitySetId, it.key, it.value.type.fullQualifiedNameAsString, version
                                )
                        ) to connection.prepareStatement(updatePropertyVersionSql(it.key, version))
                    }



            entities.forEach { entity ->
                entitySetPreparedStatement.setObject(1, entity.key)
                entitySetPreparedStatement.addBatch()

                val entityKeyId = entity.key
                val entityData = asMap(JsonDeserializer
                                               .validateFormatAndNormalize(entity.value, dataTypes)
                                               { "Entity set $entitySetId with entity key id $entityKeyId" })

                entityData.forEach { (propertyTypeId, values) ->
                    val upsertStatements = preparedStatements[propertyTypeId]

                    if (upsertStatements == null) {
                        logger.warn(
                                "Not inserting unauthorized property in entity {} from entity set {}",
                                entityKeyId,
                                entitySetId
                        )
                    } else {
                        values.forEach { value ->
                            val (ps, ver) = upsertStatements!!

                            //Binary data types get stored in S3 bucket
                            val (propertyHash, insertValue) =
                                    if (dataTypes[propertyTypeId] == EdmPrimitiveTypeKind.Binary) {
                                        //store data in S3 bucket
                                        val pair = value as Pair<String, ByteArray>

                                        val digest = PostgresDataHasher.hashObjectToHex(
                                                pair.second, EdmPrimitiveTypeKind.Binary
                                        )
                                        //store entity set id/entity key id/property type id/property hash as key in S3
                                        val s3Key = "$entitySetId/$entityKeyId/$propertyTypeId/$digest"
                                        byteBlobDataManager.putObject(s3Key, pair.second, pair.first)
                                        PostgresDataHasher.hashObject(s3Key, EdmPrimitiveTypeKind.String) to s3Key
                                    } else {
                                        PostgresDataHasher.hashObject(value, dataTypes[propertyTypeId]) to value
                                    }

                            ps.setObject(1, entityKeyId)
                            ps.setBytes(2, propertyHash)
                            ps.setObject(3, insertValue)

                            ver.setObject(1, entitySetId)
                            ver.setObject(2, entityKeyId)
                            ver.setObject(3, propertyHash)

                            ps.addBatch()
                            ver.addBatch()
                        }
                    }
                }
            }

            //In case we want to do validation
            val updatedPropertyCounts = preparedStatements.values.map {
                val updateCount = it.first.executeBatch()
                it.second.executeBatch()
                return@map updateCount
            }.sumBy { it.sum() }

            val updatedEntityCount = entitySetPreparedStatement.executeBatch().sum()

            preparedStatements.values.forEach {
                it.first.close()
                it.second.close()
            }

            entitySetPreparedStatement.close()
            checkState(updatedEntityCount == entities.size, "Updated entity metadata count mismatch")

            logger.debug("Updated $updatedEntityCount entities and $updatedPropertyCounts properties")

            return updatedEntityCount
        }
    }

    fun clearEntitySet(entitySetId: UUID, authorizedPropertyTypes: Map<UUID, PropertyType>): Int {
        return tombstone(entitySetId) + tombstone(entitySetId, authorizedPropertyTypes.values)
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
    ): Int {
        //TODO: Make these a single transaction.
        tombstone(entitySetId, entityKeyIds, authorizedPropertyTypes.values)
        return tombstone(entitySetId, entityKeyIds)
    }

    /**int
     * Tombstones (writes a negative version) for the provided entity properties.
     * @param entitySetId The entity set to operate on.
     * @param entityKeyIds The entity key ids to tombstone.
     * @param authorizedPropertyTypes The property types the user is requested and is allowed to tombstone. We assume
     * that authorization checks are enforced at a higher level and that this just streamlines issuing the necessary
     * queries.
     */
    fun clearEntityData(
            entitySetId: UUID, entityKeyIds: Set<UUID>, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        return tombstone(entitySetId, entityKeyIds, authorizedPropertyTypes.values)
    }

    fun deleteEntityData(
            entitySetId: UUID, entityKeyIds: Set<UUID>, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        val connection = hds.connection
        return connection.use {
            authorizedPropertyTypes
                    .map { property ->
                        if (property.value.datatype == EdmPrimitiveTypeKind.Binary) {
                            val propertyTable = quote(propertyTableName(property.key))
                            val fqn = property.value.type.toString()
                            val fqnColumn = quote(fqn)
                            deletePropertyOfEntityFromS3(propertyTable, fqn, fqnColumn, entitySetId, entityKeyIds)
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
    }

    fun deleteEntitySetData(entitySetId: UUID, propertyTypes: Map<UUID, PropertyType>): Int {
        val connection = hds.connection
        return connection.use {
            propertyTypes
                    .map {
                        val s = connection.createStatement()
                        if (it.value.datatype == EdmPrimitiveTypeKind.Binary) {
                            val propertyTable = quote(propertyTableName(it.key))
                            val fqn = it.value.type.toString()
                            deletePropertiesInEntitySetFromS3(propertyTable, fqn, entitySetId)
                        }
                        val count: Int = s.executeUpdate(deletePropertiesInEntitySet(entitySetId, it.key))
                        s.close()
                        count
                    }
                    .sum()
        }
    }

    fun deletePropertyOfEntityFromS3(
            propertyTable: String, fqn: String, fqnColumn: String, entitySetId: UUID, entityKeyIds: Set<UUID>
    ) {
        val connection = hds.connection
        val ps = connection.prepareStatement(selectPropertyOfEntityInS3(propertyTable, fqnColumn, entitySetId))
        val arr = PostgresArrays.createUuidArray(connection, entityKeyIds)
        ps.setObject(1, arr)
        val rs = ps.executeQuery()
        while (rs.next()) {
            byteBlobDataManager.deleteObject(rs.getString(fqn))
        }
        ps.close()
        connection.close()
    }


    fun deletePropertiesInEntitySetFromS3(propertyTable: String, fqn: String, entitySetId: UUID) {
        PostgresIterable(
                Supplier {
                    val connection = hds.connection
                    val ps = connection.prepareStatement(
                            selectPropertiesInEntitySetInS3(propertyTable, quote(fqn), entitySetId)
                    )
                    ps.fetchSize = FETCH_SIZE
                    val rs = ps.executeQuery()
                    StatementHolder(connection, ps, rs)
                },
                Function<ResultSet, String> {
                    it.getString(fqn)
                }).asSequence().chunked(1000).forEach { byteBlobDataManager.deleteObjects(it) }
    }

    fun deleteEntitySet(entitySetId: UUID): Int {
        return hds.connection.use {
            val s = it.prepareStatement(deleteEntitySetEntityKeys(entitySetId))
            s.executeUpdate()
        }
    }

    fun deleteEntities(entitySetId: UUID, entityKeyIds: Set<UUID>): Int {
        return hds.connection.use {
            val s = it.prepareStatement(deleteEntityKeys(entitySetId))
            val arr = PostgresArrays.createUuidArray(it, entityKeyIds)
            s.setArray(1, arr)
            s.executeUpdate()
        }
    }


    fun selectPropertyOfEntityInS3(propertyTable: String, fqnColumn: String, entitySetId: UUID): String {
        return "SELECT $fqnColumn FROM $propertyTable WHERE ${PostgresColumn.ENTITY_SET_ID.name} = '$entitySetId'::uuid AND id in (SELECT * FROM UNNEST( (?)::uuid[] )) "
    }

    fun selectPropertiesInEntitySetInS3(propertyTable: String, fqnColumn: String, entitySetId: UUID): String {
        return "SELECT $fqnColumn FROM $propertyTable WHERE ${PostgresColumn.ENTITY_SET_ID.name} = '$entitySetId'::uuid "
    }

    /**
     * Replace Replacing an entity involves setting all properties for that versions for that entity to -now()*
     */
    fun replaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        tombstone(entitySetId, entities.keys, authorizedPropertyTypes.values)
        return upsertEntities(entitySetId, entities, authorizedPropertyTypes)
    }

    fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        //Only tombstone properties being replaced.
        entities.forEach {
            val entity = it.value
            //Implied access enforcement as it will raise exception if lacking permission
            tombstone(entitySetId, setOf(it.key), entity.keys.map { authorizedPropertyTypes[it]!! }.toSet())
        }
        return upsertEntities(entitySetId, entities, authorizedPropertyTypes)
    }

    fun replacePropertiesInEntities(
            entitySetId: UUID,
            replacementProperties: Map<UUID, SetMultimap<UUID, Map<ByteBuffer, Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        //We expect controller to have performed access control checks upstream.
        tombstone(entitySetId, replacementProperties)
        //This performs unnecessary copies and we should fix at some point
        val replacementValues = replacementProperties.asSequence().map {
            it.key to extractValues(
                    it.value
            )
        }.toMap()
        return upsertEntities(entitySetId, replacementValues, authorizedPropertyTypes)
    }


    private fun extractValues(
            replacementProperties: SetMultimap<UUID, Map<ByteBuffer, Any>>
    ): Map<UUID, Set<Any>> {
        return replacementProperties.asMap().map {
            it.key to it.value.flatMap { it.values }.toSet()
        }.toMap()
    }

    /**
     * Tombstones the provided set of property types for each provided entity key.
     */
    private fun tombstone(
            entitySetId: UUID, entityKeyIds: Set<UUID>, propertyTypesToTombstone: Collection<PropertyType>
    ): Int {
        val connection = hds.connection
        connection.use {
            val tombstoneVersion = -System.currentTimeMillis()

            return propertyTypesToTombstone
                    .map {
                        val ps = connection.prepareStatement(
                                updatePropertyVersionForDataKey(entitySetId, it.id, tombstoneVersion)
                        )
                        entityKeyIds.forEach {
                            ps.setObject(1, it)
                            ps.addBatch()
                        }
                        ps.executeBatch().sum()
                    }
                    .sum()
        }
    }

    /**
     * Tombstones the provided set of property types for each provided entity key.
     */
    private fun tombstone(entitySetId: UUID, propertyTypesToTombstone: Collection<PropertyType>): Int {
        val connection = hds.connection
        connection.use {
            val tombstoneVersion = -System.currentTimeMillis()

            return propertyTypesToTombstone
                    .map {
                        val propertyTypeId = it.id
                        connection.createStatement().use {
                            it.executeUpdate(
                                    updatePropertyVersionForEntitySet(entitySetId, propertyTypeId, tombstoneVersion)
                            )
                        }
                    }
                    .sum()
        }
    }

    /**
     * Tombstones specific property values through hash id
     */
    private fun tombstone(entitySetId: UUID, entities: Map<UUID, SetMultimap<UUID, Map<ByteBuffer, Any>>>): Int {
        val connection = hds.connection
        return connection.use {
            val tombstoneVersion = -System.currentTimeMillis()
            val propertyTypePreparedStatements = entities.values
                    .flatMap { it.keySet() }
                    .toSet()
                    .map {
                        it to connection.prepareStatement(
                                updatePropertyValueVersion(entitySetId, it, tombstoneVersion)
                        )
                    }
                    .toMap()

            entities.forEach {
                val entityKeyId = it.key
                asMap(it.value)
                        .map {
                            val ps = propertyTypePreparedStatements[it.key]!!
                            //TODO: We're currently doing this one hash at a time and we should consider doing it using in query
                            it.value.forEach {
                                it.keys.forEach {
                                    ps.setObject(1, entityKeyId)
                                    ps.setBytes(2, it.array()) //hash
                                    ps.addBatch()
                                }
                            }
                        }
            }
            propertyTypePreparedStatements.values.map(PreparedStatement::executeBatch).map(IntArray::sum).sum()
        }
    }

    private fun tombstone(entitySetId: UUID): Int {
        val connection = hds.connection
        return connection.use {
            val ps = it.prepareStatement(updateAllEntityVersions(entitySetId, -System.currentTimeMillis()))
            ps.executeUpdate()
        }
    }

    private fun tombstone(entitySetId: UUID, entityKeyIds: Set<UUID>): Int {
        val connection = hds.connection
        return connection.use {
            val ps = connection.prepareStatement(updateEntityVersion(entitySetId, -System.currentTimeMillis()))
            entityKeyIds.forEach {
                ps.setObject(1, it)
                ps.addBatch()
            }
            ps.executeBatch().sum()
        }
    }
}

/**
 * This function prepares a SQL query that will update all the versions in an entity set.
 *
 * TODO: Update this to be a prepared statement where version and entity_set_id are passed in.
 * @param entitySetId Id of entity set for which to perform version updates.
 * @param version The version to set
 */
fun updateAllEntityVersions(entitySetId: UUID, version: Long): String {
    return "UPDATE ${IDS.name} SET versions = versions || $version, version = $version " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' "
}

/**
 * This function prepares a SQL query that will update all the version of a specific entity in an entity set.
 *
 * TODO: Update this to be a prepared statement where version and entity_set_id are passed in.
 * @param entitySetId Id of entity set for which to perform version updates.
 * @param version The version to set
 */
fun updateEntityVersion(entitySetId: UUID, version: Long): String {
    return updateAllEntityVersions(entitySetId, version) + " AND ${ID_VALUE.name} = ? "
}

fun updatePropertyVersionForDataKey(entitySetId: UUID, propertyTypeId: UUID, version: Long): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    return "UPDATE $propertyTable SET versions = versions || $version, version = $version " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId'::uuid AND ${ID_VALUE.name} = ? "
}

fun updatePropertyVersionForEntitySet(entitySetId: UUID, propertyTypeId: UUID, version: Long): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    return "UPDATE $propertyTable SET versions = versions || $version, version = $version " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId'::uuid "
}

fun updatePropertyValueVersion(entitySetId: UUID, propertyTypeId: UUID, version: Long): String {
    return updatePropertyVersionForDataKey(entitySetId, propertyTypeId, version) +
            "AND ${HASH.name} = ?"
}

fun deletePropertiesInEntitySet(entitySetId: UUID, propertyTypeId: UUID): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    return "DELETE FROM $propertyTable WHERE ${ENTITY_SET_ID.name} = '$entitySetId' "
}

fun deletePropertiesOfEntities(entitySetId: UUID, propertyTypeId: UUID): String {
    return deletePropertiesInEntitySet(entitySetId, propertyTypeId) +
            " AND id in (SELECT * FROM UNNEST( (?)::uuid[] )) "
}

fun deleteEntityKeys(entitySetId: UUID): String {
    return deleteEntitySetEntityKeys(entitySetId) + "AND ${ID.name} in (SELECT * FROM UNNEST( (?)::uuid[] )) "
}

fun deleteEntitySetEntityKeys(entitySetId: UUID): String {
    return "DELETE FROM ${IDS.name} WHERE ${ENTITY_SET_ID.name} = '$entitySetId' "
}

fun upsertEntity(entitySetId: UUID, version: Long): String {
    //Last writer wins for entities
    return "UPDATE ${IDS.name} SET versions = ${VERSIONS.name} || ARRAY[$version], " +
            "${VERSION.name} = GREATEST(${VERSION.name},$version), ${LAST_WRITE.name} = now() " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ${ID_VALUE.name} = ?"
}

fun upsertPropertyValues(entitySetId: UUID, propertyTypeId: UUID, propertyType: String, version: Long): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    val columns = setOf(
            ENTITY_SET_ID.name,
            ID_VALUE.name,
            HASH.name,
            quote(propertyType),
            VERSION.name,
            VERSIONS.name,
            LAST_WRITE.name,
            LAST_PROPAGATE.name
    )

    //Insert new row or update version. We only perform update if we're the winning timestamp.
    return "INSERT INTO $propertyTable (${columns.joinToString(
            ","
    )}) VALUES('$entitySetId'::uuid,?,?,?,$version,ARRAY[$version],now(), now()) " +
            "ON CONFLICT (${ENTITY_SET_ID.name},${ID_VALUE.name}, ${HASH.name}) " +
            "DO UPDATE SET versions = $propertyTable.${VERSIONS.name} || EXCLUDED.${VERSIONS.name} "

}

fun updatePropertyVersionSql(propertyTypeId: UUID, version: Long): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    return "UPDATE $propertyTable SET ${VERSION.name} = $version " +
            "WHERE ${ENTITY_SET_ID.name} = ? AND ${ID_VALUE.name} = ? AND ${HASH.name} = ? " +
            "AND abs(${VERSION.name}) < $version "
}

fun selectEntitySetWithPropertyTypes(
        entitySetId: UUID,
        entityKeyIds: Optional<Set<UUID>>,
        authorizedPropertyTypes: Map<UUID, String>,
        metadataOptions: Set<MetadataOption>,
        binaryPropertyTypes: Map<UUID, Boolean>
): String {
    val esTableName = DataTables.quote(DataTables.entityTableName(entitySetId))

    val entityKeyIdsClause = entityKeyIds.map { "AND ${entityKeyIdsClause(it)} " }.orElse(" ")
    //@formatter:off
    val columns = setOf(ID_VALUE.name) +
            metadataOptions.map { ResultSetAdapters.mapMetadataOptionToPostgresColumn(it).name } +
            authorizedPropertyTypes.values.map(::quote)

    return "SELECT ${columns.filter(String::isNotBlank).joinToString(",")} FROM (SELECT * \n" +
            "FROM $esTableName " +
            "WHERE version > 0 $entityKeyIdsClause" +
            ") as $esTableName" +
            authorizedPropertyTypes
                    .map { "LEFT JOIN ${subSelectLatestVersionOfPropertyTypeInEntitySet(entitySetId, entityKeyIdsClause, it.key, it.value, binaryPropertyTypes[it.key]!!)} USING (${ID.name} )" }
                    .joinToString("\n")
    //@formatter:on
}

fun selectEntitySetWithPropertyTypesAndVersion(
        entitySetId: UUID,
        entityKeyIds: Optional<Set<UUID>>,
        authorizedPropertyTypes: Map<UUID, String>,
        metadataOptions: Set<MetadataOption>,
        version: Long,
        binaryPropertyTypes: Map<UUID, Boolean>
): String {
    val esTableName = DataTables.quote(DataTables.entityTableName(entitySetId))
    val entityKeyIdsClause = entityKeyIds.map { "AND ${entityKeyIdsClause(it)} " }.orElse(" ")
    //@formatter:off
    val columns = setOf(ID_VALUE.name) +
            metadataOptions.map { ResultSetAdapters.mapMetadataOptionToPostgresColumn(it).name } +
            authorizedPropertyTypes.values.map(::quote)

    return "SELECT ${columns.filter(String::isNotBlank).joinToString(",")} FROM ( SELECT * " +
            "FROM $esTableName " +
            "WHERE version > 0 $entityKeyIdsClause" +
            ") as $esTableName" +
            authorizedPropertyTypes
                    .map { "LEFT JOIN ${selectVersionOfPropertyTypeInEntitySet(entitySetId, entityKeyIdsClause, it.key, it.value, version, binaryPropertyTypes[it.key]!!)} USING (${ID.name} )" }
                    .joinToString("\n")
    //@formatter:on
}

internal fun selectVersionOfPropertyTypeInEntitySet(
        entitySetId: UUID,
        entityKeyIdsClause: String,
        propertyTypeId: UUID,
        fqn: String,
        version: Long,
        binary: Boolean
): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    val arrayAgg = " array_agg(${DataTables.quote(fqn)}) as ${DataTables.quote(fqn)} "


    return "(SELECT ${ENTITY_SET_ID.name}, " +
            "   ${ID_VALUE.name}, " +
            "   $arrayAgg " +
            "FROM ${subSelectFilteredVersionOfPropertyTypeInEntitySet(
                    entitySetId, entityKeyIdsClause, propertyTypeId, fqn, version, binary
            )}" +
            "LEFT JOIN $propertyTable USING(entity_set_id, id) GROUP BY(${ENTITY_SET_ID.name},${ID_VALUE.name})" +
            ") as $propertyTable "
}


internal fun subSelectLatestVersionOfPropertyTypeInEntitySet(
        entitySetId: UUID,
        entityKeyIdsClause: String,
        propertyTypeId: UUID,
        fqn: String,
        binary: Boolean
): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    val arrayAgg = " array_agg(${DataTables.quote(fqn)}) as ${DataTables.quote(fqn)} "

    return "(SELECT ${ENTITY_SET_ID.name}," +
            " ${ID_VALUE.name}," +
            " $arrayAgg" +
            "FROM $propertyTable " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ${VERSION.name} >= 0 $entityKeyIdsClause" +
            "GROUP BY (${ENTITY_SET_ID.name}, ${ID_VALUE.name})) as $propertyTable "
}

internal fun subSelectFilteredVersionOfPropertyTypeInEntitySet(
        entitySetId: UUID,
        entityKeyIdsClause: String,
        propertyTypeId: UUID,
        fqn: String,
        version: Long,
        binary: Boolean
): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))

    return "(SELECT ${ENTITY_SET_ID.name},${ID_VALUE.name} FROM (SELECT ${ENTITY_SET_ID.name},${ID_VALUE.name}, max(versions) as abs_max, max(abs(versions)) as max_abs " +
            "FROM (SELECT entity_set_id, id, unnest(versions) as versions FROM $propertyTable " +
            "WHERE ${ENTITY_SET_ID.name}='$entitySetId' $entityKeyIdsClause) as $EXPANDED_VERSIONS " +
            "WHERE abs(versions) <= $version " +
            "GROUP BY(${ENTITY_SET_ID.name},${ID_VALUE.name})) as unfiltered_data_keys WHERE max_abs=abs_max ) as data_keys "

}

internal fun entityKeyIdsClause(entityKeyIds: Set<UUID>): String {
    return if (entityKeyIds.isEmpty()) {
        " TRUE "
    } else {
        "${ID_VALUE.name} IN ('" + entityKeyIds.joinToString("','") + "')"
    }
}

internal fun selectLinkingIdsOfEntities(entityKeyIds: Map<UUID, Optional<Set<UUID>>>): String {
    val entitiesClause = buildEntitiesClause(entityKeyIds, false)
    return "SELECT ${ENTITY_SET_ID.name}, array_agg(${LINKING_ID.name}) as ${LINKING_ID.name} " +
            "FROM ${IDS.name} " +
            "WHERE ${LINKING_ID.name} IS NOT NULL $entitiesClause " +
            "GROUP BY ${ENTITY_SET_ID.name} "
}

internal fun selectLinkingIdsOfEntitySet(entitySetId: UUID): String {
    return "SELECT DISTINCT ${LINKING_ID.name} " +
            "FROM ${IDS.name} " +
            "WHERE ${VERSION.name} > 0 AND ${LINKING_ID.name} IS NOT NULL AND ${ENTITY_SET_ID.name} = '$entitySetId'"
}

internal fun getLinkingEntitySetIdsOfLinkingIdQuery(linkingId: UUID): String {
    val selectEntitySetIdOfLinkingId =
            "SELECT DISTINCT ${ENTITY_SET_ID.name} " +
                    "FROM ${IDS.name} " +
                    "WHERE ${LINKING_ID.name} = '$linkingId'"
    return "SELECT ${ID.name} " +
            "FROM ${ENTITY_SETS.name} " +
            "INNER JOIN ( $selectEntitySetIdOfLinkingId ) as linked_es " +
            "ON ( ${ENTITY_SET_ID.name}= ANY( ${LINKED_ENTITY_SETS.name} ) )"
}

internal fun getLinkingEntitySetIdsOfEntitySetIdQuery(entitySetId: UUID): String {
    return "SELECT ${ID.name} " +
            "FROM ${ENTITY_SETS.name} " +
            "WHERE '$entitySetId' = ANY(${LINKED_ENTITY_SETS.name})"
}
