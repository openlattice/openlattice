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

import com.google.common.collect.Multimaps.asMap
import com.google.common.collect.SetMultimap
import com.openlattice.data.EntityDataKey
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresColumn.*
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
import java.time.OffsetDateTime
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

class PostgresEntityDataQueryService(private val hds: HikariDataSource) {

    fun getEntitiesById(
            entitySetId: UUID,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            entityKeyIds: Set<UUID>
    ): Map<UUID, Map<UUID, Set<Any>>> {
        val adapter = Function<ResultSet, Pair<UUID, Map<UUID, Set<Any>>>> {
            ResultSetAdapters.id(it) to ResultSetAdapters.implicitEntityValuesById(it, authorizedPropertyTypes)
        }
        return streamableEntitySet(
                mapOf(entitySetId to Optional.of(entityKeyIds)), mapOf(entitySetId to authorizedPropertyTypes),
                EnumSet.noneOf(MetadataOption::class.java), Optional.empty(), adapter
        ).toMap()
    }

    fun streamableEntitySet(
            entitySetIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long> = Optional.empty(),
            linking: Boolean = false
    ): PostgresIterable<SetMultimap<FullQualifiedName, Any>> {
        return if(linking) {
            streamableLinkingEntitySet(
                    entitySetIds.map { it to Optional.empty<Set<UUID>>() }.toMap(),
                    authorizedPropertyTypes,
                    metadataOptions,
                    version)
        } else {
            streamableEntitySet(
                    entitySetIds.map { it to Optional.empty<Set<UUID>>() }.toMap(),
                    authorizedPropertyTypes,
                    metadataOptions,
                    version)
        }
    }

    fun streamableEntitySet(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long> = Optional.empty(),
            linking: Boolean = false
    ): PostgresIterable<SetMultimap<FullQualifiedName, Any>> {
        return if(linking) {
            streamableLinkingEntitySet(
                    mapOf( entitySetId to Optional.of(entityKeyIds) ),
                    authorizedPropertyTypes,
                    metadataOptions,
                    version)
        } else {
            streamableEntitySet(
                    mapOf( entitySetId to Optional.of(entityKeyIds) ),
                    authorizedPropertyTypes,
                    metadataOptions,
                    version)
        }
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
                    ResultSetAdapters.implicitEntityValuesById(it, authorizedPropertyTypes)
        }
        return streamableEntitySet(
                mapOf(entitySetId to entityKeyIds), mapOf(entitySetId to authorizedPropertyTypes), metadataOptions,
                version, adapter
        )
    }

    fun streamableEntitySet(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long> = Optional.empty()
    ): PostgresIterable<SetMultimap<FullQualifiedName, Any>> {
        val adapter = Function<ResultSet, SetMultimap<FullQualifiedName, Any>> {
            ResultSetAdapters.implicitNormalEntity(it, authorizedPropertyTypes, metadataOptions)
        }
        return streamableEntitySet(entityKeyIds, authorizedPropertyTypes, metadataOptions, version, adapter, false)
    }

    fun streamableLinkingEntitySet(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long> = Optional.empty()
    ): PostgresIterable<SetMultimap<FullQualifiedName, Any>> {
        val adapter = Function<ResultSet, SetMultimap<FullQualifiedName, Any>> {
            ResultSetAdapters.implicitLinkedEntity(it, authorizedPropertyTypes, metadataOptions)
        }
        return streamableEntitySet(entityKeyIds, authorizedPropertyTypes, metadataOptions, version, adapter, true)
    }

    private fun <T> streamableEntitySet(
            entityKeyIds: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long>,
            adapter: Function<ResultSet, T>,
            linking: Boolean = false
    ): PostgresIterable<T> {
        return PostgresIterable(
                Supplier<StatementHolder> {
                    val connection = hds.connection
                    val statement = connection.createStatement()
                    statement.fetchSize = FETCH_SIZE

                    val allPropertyTypes = authorizedPropertyTypes.values.flatMap { it.values }.toSet()
                    val binaryPropertyTypes = allPropertyTypes
                            .associate { it.id to (it.datatype == EdmPrimitiveTypeKind.Binary) }
                    val propertyFqns = allPropertyTypes.map { it.id to quote(it.type.fullQualifiedNameAsString) }.toMap()

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
                                        linking,
                                        binaryPropertyTypes
                                )
                            } else {
                                selectEntitySetWithCurrentVersionOfPropertyTypes(
                                        entityKeyIds,
                                        propertyFqns,
                                        allPropertyTypes.map { it.id },
                                        authorizedPropertyTypes.mapValues { it.value.map { it.key }.toSet() },
                                        mapOf(),
                                        metadataOptions,
                                        linking,
                                        binaryPropertyTypes
                                )
                            }
                    )
                    StatementHolder(connection, statement, rs)
                },
                adapter
        )
    }

    fun getEntityDataKeysOfLinkingIds(linkingIds:Set<UUID> ) : PostgresIterable<EntityDataKey> {
        val adapter = Function<ResultSet, EntityDataKey> {
            ResultSetAdapters.entityDataKey(it)
        }
        return PostgresIterable( Supplier<StatementHolder> {
            val connection = hds.connection
            val statement = connection.createStatement()
            statement.fetchSize = FETCH_SIZE

            val rs = statement.executeQuery(selectEntityKeysOfLinkingIds(linkingIds))
            StatementHolder(connection, statement, rs)
        }, adapter)
    }

    fun upsertEntities(
            entitySetId: UUID, entities: Map<UUID, Map<UUID, Set<Any>>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        val connection = hds.connection
        connection.use {
            val version = System.currentTimeMillis()
            val entitySetPreparedStatement = connection.prepareStatement(upsertEntity(entitySetId, version))
            val datatypes = authorizedPropertyTypes.map { it.key to it.value.datatype }.toMap()
            val preparedStatements = authorizedPropertyTypes
                    .map {
                        it.key to connection.prepareStatement(
                                upsertPropertyValues(
                                        entitySetId, it.key, it.value.type.fullQualifiedNameAsString, version
                                )
                        )
                    }
                    .toMap()

            entities.forEach {
                entitySetPreparedStatement.setObject(1, it.key)
                entitySetPreparedStatement.addBatch()

                val entityKeyId = it.key
                val entityData = JsonDeserializer.validateFormatAndNormalize(it.value, datatypes)

                asMap(entityData)
                        .forEach {
                            val propertyTypeId = it.key
                            val properties = it.value
                            properties.forEach {
                                if (it == null) {
                                    logger.error(
                                            "Encountered null property value of type {} for entity set {} with entity key id {}",
                                            propertyTypeId, entitySetId, entityKeyId
                                    )
                                } else {
                                    val ps = preparedStatements[propertyTypeId]
                                    ps?.setObject(1, entityKeyId)
                                    ps?.setBytes(2, PostgresDataHasher.hashObject(it, datatypes[propertyTypeId]))
                                    ps?.setObject(3, it)
                                    ps?.addBatch()
                                    if (ps == null) {
                                        logger.warn(
                                                "Skipping unauthorized property in entity $entityKeyId from entity set $entitySetId"
                                        )
                                    }
                                }
                            }
                        }
            }

            //In case we want to do validation
            val updatedPropertyCounts = preparedStatements.values.map { it.executeBatch() }.sumBy { it.sum() }
            val updatedEntityCount = entitySetPreparedStatement.executeBatch().sum()
            preparedStatements.values.forEach(PreparedStatement::close)
            entitySetPreparedStatement.close()

            logger.debug("Updated $updatedEntityCount entities and $updatedPropertyCounts properties")

            return updatedEntityCount
        }
    }

    fun clearEntitySet(entitySetId: UUID, authorizedPropertyTypes: Map<UUID, PropertyType>): Int {
        return tombstone(entitySetId) + tombstone(entitySetId, authorizedPropertyTypes.values)
    }

    /**int
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

    fun deleteEntities(
            entitySetId: UUID, entityKeyIds: Set<UUID>, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        val connection = hds.connection
        return connection.use {
            return authorizedPropertyTypes
                    .map {
                        val s = connection.createStatement()
                        val count: Int = s.executeUpdate(deletePropertiesOfEntities(entitySetId, it.key, entityKeyIds))
                        s.close()
                        count
                    }
                    .sum()
        }
    }

    fun deleteEntitySet(entitySetId: UUID, authorizedPropertyTypes: Map<UUID, PropertyType>): Int {
        val connection = hds.connection
        return connection.use {
            authorizedPropertyTypes
                    .map {
                        val s = connection.createStatement()
                        val count: Int = s.executeUpdate(deletePropertiesInEntitySet(entitySetId, it.key))
                        s.close()
                        count
                    }
                    .sum()
        }
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
            return propertyTypePreparedStatements.values.map(PreparedStatement::executeBatch).map(IntArray::sum).sum()
        }
    }

    private fun tombstone(entitySetId: UUID): Int {
        val connection = hds.connection
        return connection.use {
            val ps = it.prepareStatement(updateAllEntityVersions(entitySetId, -System.currentTimeMillis()))
            return ps.executeUpdate()
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
            return ps.executeBatch().sum()
        }
    }

    fun markAsIndexed(entitySetId: UUID, batchToIndex: Set<UUID>): Int {
        hds.connection.use {
            it.prepareStatement(updateLastIndexSql(entitySetId)).use {
                val arr = PostgresArrays.createUuidArray(it.connection, batchToIndex)
                it.setObject(1, OffsetDateTime.now())
                it.setArray(2, arr)
                return it.executeUpdate()
            }

        }
    }

    fun markAsLinked(entitySetId: UUID, processedEntities: Set<UUID>): Int {
        hds.connection.use {
            it.prepareStatement(updateLastLinkSql(entitySetId)).use {
                val arr = PostgresArrays.createUuidArray(it.connection, processedEntities)
                it.setObject(1, OffsetDateTime.now())
                it.setArray(2, arr)
                return it.executeUpdate()
            }

        }
    }

    fun markAsProcessed(entitySetId: UUID, processedEntities: Set<UUID>, processedTime: OffsetDateTime): Int {
        hds.connection.use {
            it.prepareStatement(updateLastPropagateSql(entitySetId)).use {
                val arr = PostgresArrays.createUuidArray(it.connection, processedEntities)
                it.setObject(1, processedTime)
                it.setArray(2, arr)
                return it.executeUpdate()
            }

        }
    }
}

fun updateLastIndexSql(entitySetId: UUID): String {
    return "UPDATE ${IDS.name} SET ${LAST_INDEX.name} = ? " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ${ID.name} IN (SELECT UNNEST( (?)::uuid[] ))"
}

fun updateLastLinkSql(entitySetId: UUID): String {
    return "UPDATE ${IDS.name} SET ${LAST_LINK.name} = ? " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ${ID.name} IN (SELECT UNNEST( (?)::uuid[] ))"
}

fun updateLastPropagateSql(entitySetId: UUID): String {
    return "UPDATE ${IDS.name} SET ${LAST_PROPAGATE.name} = ? " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ${ID.name} IN (SELECT UNNEST( (?)::uuid[] ))"
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
    return "DELETE FROM $propertyTable WHERE ${ENTITY_SET_ID.name} = '$entitySetId'::uuid "
}

fun deletePropertiesOfEntities(entitySetId: UUID, propertyTypeId: UUID, entityKeyIds: Set<UUID>): String {
    return deletePropertiesInEntitySet(
            entitySetId, propertyTypeId
    ) + " WHERE id in (SELECT * FROM UNNEST( (?)::uuid[] )) "
}

fun deleteEntities(entitySetId: UUID, entityKeyIds: Set<UUID>): String {
    return deleteEntitySet(entitySetId) + "AND ${ID.name} in (SELECT * FROM UNNEST( (?)::uuid[] )) "
}

fun deleteEntitySet(entitySetId: UUID): String {
    return "DELETE FROM ${IDS.name} WHERE ${ENTITY_SET_ID.name} = '$entitySetId' "
}

fun upsertEntity(entitySetId: UUID, version: Long): String {
    val columns = setOf(
            ENTITY_SET_ID.name,
            ID_VALUE.name,
            VERSION.name,
            VERSIONS.name,
            LAST_WRITE.name,
            LAST_INDEX.name,
            LAST_LINK.name,
            LAST_PROPAGATE.name
    )
    //Last writer wins for entities
    return "INSERT INTO ${IDS.name} (${columns.joinToString(",")}) " +
            "VALUES( '$entitySetId', ?,$version,ARRAY[$version],now(),'-infinity','-infinity','-infinity') " +
            "ON CONFLICT (${ID_VALUE.name}) " +
            "DO UPDATE SET versions = ${IDS.name}.${VERSIONS.name} || EXCLUDED.${VERSIONS.name}, " +
            "${VERSION.name} = EXCLUDED.${VERSION.name}, " +
            "${LAST_WRITE.name} = EXCLUDED.${LAST_WRITE.name} " +
            "WHERE EXCLUDED.${VERSION.name} > abs(${IDS.name}.version) "
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
            "DO UPDATE SET versions = $propertyTable.${VERSIONS.name} || EXCLUDED.${VERSIONS.name}, " +
            "${VERSION.name} = EXCLUDED.${VERSION.name} " +
            "WHERE EXCLUDED.${VERSION.name} > abs($propertyTable.version) "
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
            metadataOptions.map{ ResultSetAdapters.mapMetadataOptionToPostgresColumn(it).name } +
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
            metadataOptions.map{ ResultSetAdapters.mapMetadataOptionToPostgresColumn(it).name } +
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
    val arrayAgg = if (binary) {
        " array_agg(encode(${DataTables.quote(fqn)}, 'base64')) as ${DataTables.quote(fqn)} "
    } else {
        " array_agg(${DataTables.quote(fqn)}) as ${DataTables.quote(fqn)} "
    }

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
    val arrayAgg = if (binary) {
        " array_agg(encode(${DataTables.quote(fqn)}, 'base64')) as ${DataTables.quote(fqn)} "
    } else {
        " array_agg(${DataTables.quote(fqn)}) as ${DataTables.quote(fqn)} "
    }
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
    //TODO: Potential Latent bug-- shouldn't break anything but when empty set is passed it will do IN('') which may
    //cause a type exception
    return "${ID_VALUE.name} IN ('" + entityKeyIds.joinToString("','") + "')"
}



