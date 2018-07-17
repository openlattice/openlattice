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

import com.google.common.collect.HashMultimap
import com.google.common.collect.Multimaps
import com.google.common.collect.SetMultimap
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.JsonDeserializer
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
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
const val BATCH_SIZE = 10000
private val logger = LoggerFactory.getLogger(PostgresEntityDataQueryService::class.java)

class PostgresEntityDataQueryService(private val hds: HikariDataSource) {
    fun streamableEntitySet(
            entitySetId: UUID,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long> = Optional.empty()
    ): PostgresIterable<SetMultimap<FullQualifiedName, Any>> {
        return streamableEntitySet(entitySetId, setOf(), authorizedPropertyTypes, metadataOptions, version)
    }

    fun streamableEntitySet(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, PropertyType>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long> = Optional.empty()
    ): PostgresIterable<SetMultimap<FullQualifiedName, Any>> {
        return PostgresIterable(
                Supplier<StatementHolder> {
                    val connection = hds.getConnection()
                    val statement = connection.createStatement()
                    val rs = statement.executeQuery(
                            if (version.isPresent) {
                                selectEntitySetWithPropertyTypesAndVersion(
                                        entitySetId,
                                        entityKeyIds,
                                        authorizedPropertyTypes.map { it.key to it.value.type.fullQualifiedNameAsString }.toMap(),
                                        metadataOptions,
                                        version.get()
                                )
                            } else {
                                selectEntitySetWithPropertyTypes(
                                        entitySetId,
                                        entityKeyIds,
                                        authorizedPropertyTypes.map { it.key to it.value.type.fullQualifiedNameAsString }.toMap(),
                                        metadataOptions
                                )
                            }
                    )
                    StatementHolder(connection, statement, rs)
                },
                Function<ResultSet, SetMultimap<FullQualifiedName, Any>> {
                    ResultSetAdapters.implicitEntity(it, authorizedPropertyTypes, metadataOptions)
                }
        )
    }


    fun upsertEntities(
            entitySetId: UUID, entities: Map<UUID, SetMultimap<UUID, Any>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        val connection = hds.getConnection()
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

                Multimaps
                        .asMap(entityData)
                        .forEach {
                            val propertyTypeId = it.key
                            val properties = it.value
                            properties.forEach {
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

            //In case we want to do validation
            val updatedPropertyCounts = preparedStatements.values.map { it.executeBatch() }.sumBy { it.sum() }
            val updatedEntityCount = entitySetPreparedStatement.executeBatch().sum()
            preparedStatements.values.forEach(PreparedStatement::close)
            entitySetPreparedStatement.close()

            logger.debug("Updated $updatedEntityCount entities and $updatedPropertyCounts properties")

            return updatedEntityCount
        }
    }

    fun clearEntitySet(entitySetId: UUID): Int {
        return tombstone(entitySetId)
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

    fun deleteEntities(
            entitySetId: UUID, entityKeyIds: Set<UUID>, authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        val connection = hds.getConnection()
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
        val connection = hds.getConnection()
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
            entities: Map<UUID, SetMultimap<UUID, Any>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        tombstone(entitySetId, entities.keys, authorizedPropertyTypes.values)
        return upsertEntities(entitySetId, entities, authorizedPropertyTypes)
    }

    fun partialReplaceEntities(
            entitySetId: UUID,
            entities: Map<UUID, SetMultimap<UUID, Any>>,
            authorizedPropertyTypes: Map<UUID, PropertyType>
    ): Int {
        //Only tombstone properties being replaced.
        entities.forEach {
            val entity = it.value
            //Implied access enforcement as it will raise exception if lacking permission
            tombstone(entitySetId, setOf(it.key), entity.keySet().map { authorizedPropertyTypes[it]!! }.toSet())
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
    ): SetMultimap<UUID, Any> {
        val values: SetMultimap<UUID, Any> = HashMultimap.create()
        replacementProperties.asMap().forEach {
            values.putAll(
                    it.key,
                    it.value.asSequence().flatMap { it.values.asSequence() }.asIterable()
            )
        }
        return values
    }

    /**
     * Tombstones the provided set of property types for each provided entity key.
     */
    private fun tombstone(
            entitySetId: UUID, entityKeyIds: Set<UUID>, propertyTypesToTombstone: Collection<PropertyType>
    ): Int {
        val connection = hds.getConnection()
        connection.use {
            val tombstoneVersion = -System.currentTimeMillis()

            return propertyTypesToTombstone
                    .map {
                        val ps = connection.prepareStatement(
                                updatePropertyVersion(entitySetId, it.id, tombstoneVersion)
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
     * Tombstones specific property values through hash id
     */
    private fun tombstone(entitySetId: UUID, entities: Map<UUID, SetMultimap<UUID, Map<ByteBuffer, Any>>>): Int {
        val connection = hds.getConnection()
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
                Multimaps
                        .asMap(it.value)
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
        val connection = hds.getConnection()
        return connection.use {
            val ps = it.prepareStatement(updateAllEntityVersions(entitySetId, System.currentTimeMillis()))
            return ps.executeUpdate()
        }
    }

    private fun tombstone(entitySetId: UUID, entityKeyIds: Set<UUID>): Int {
        val connection = hds.getConnection()
        return connection.use {
            val ps = connection.prepareStatement(updateEntityVersion(entitySetId, System.currentTimeMillis()))
            entityKeyIds.forEach {
                ps.setObject(1, it)
                ps.addBatch()
            }
            return ps.executeBatch().sum()
        }
    }
}

fun updateAllEntityVersions(entitySetId: UUID, version: Long): String {
    val entitiesTable = quote(entityTableName(entitySetId))
    return "UPDATE $entitiesTable SET versions = versions || $version, version = $version "
}

fun updateEntityVersion(entitySetId: UUID, version: Long): String {
    return updateAllEntityVersions(entitySetId, version) + " WHERE ${ID_VALUE.name} = ? "
}

fun updatePropertyVersion(entitySetId: UUID, propertyTypeId: UUID, version: Long): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    return "UPDATE $propertyTable SET versions = versions || $version, version = $version " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId'::uuid AND ${ID_VALUE.name} = ? "
}

fun updatePropertyValueVersion(entitySetId: UUID, propertyTypeId: UUID, version: Long): String {
    return updatePropertyVersion(entitySetId, propertyTypeId, version) +
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
    val esTableName = DataTables.quote(DataTables.entityTableName(entitySetId))
    return "DELETE FROM $esTableName WHERE id in (SELECT * FROM UNNEST( (?)::uuid[] )) "
}

fun deleteEntitySet(entitySetId: UUID): String {
    val esTableName = DataTables.quote(DataTables.entityTableName(entitySetId))
    return "DROP TABLE $esTableName"
}

fun upsertEntity(entitySetId: UUID, version: Long): String {
    val esTableName = DataTables.quote(DataTables.entityTableName(entitySetId))
    val columns = setOf(
            ID_VALUE.name,
            VERSION.name,
            VERSIONS.name,
            LAST_WRITE.name,
            LAST_INDEX.name
    )
    return "INSERT INTO $esTableName (${columns.joinToString(",")}) " +
            "VALUES( ?,$version,ARRAY[$version],now(),'-infinity') " +
            "ON CONFLICT (${ID_VALUE.name}) " +
            "DO UPDATE SET versions = $esTableName.${VERSIONS.name} || EXCLUDED.${VERSIONS.name}, " +
            "${VERSION.name} = EXCLUDED.${VERSION.name}, " +
            "${LAST_WRITE.name} = now() " +
            "WHERE EXCLUDED.${VERSION.name} > abs($esTableName.version) "
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
            LAST_WRITE.name
    )

    //Insert new row or update version. We only perform update if we're the winning timestamp.
    return "INSERT INTO $propertyTable (${columns.joinToString(
            ","
    )}) VALUES('$entitySetId'::uuid,?,?,?,$version,ARRAY[$version],now()) " +
            "ON CONFLICT (${ENTITY_SET_ID.name},${ID_VALUE.name}, ${HASH.name}) " +
            "DO UPDATE SET versions = $propertyTable.${VERSIONS.name} || EXCLUDED.${VERSIONS.name}, " +
            "${VERSION.name} = EXCLUDED.${VERSION.name} " +
            "WHERE EXCLUDED.${VERSION.name} > abs($propertyTable.version) "
}

 fun selectEntitySetWithPropertyTypes(
        entitySetId: UUID,
        entityKeyIds: Set<UUID>,
        authorizedPropertyTypes: Map<UUID, String>,
        metadataOptions: Set<MetadataOption>
): String {
    val esTableName = DataTables.quote(DataTables.entityTableName(entitySetId))
    val entityKeyIdsClause: Optional<String> =
            if (entityKeyIds.isEmpty()) {
                Optional.empty()
            } else {
                Optional.of(entityKeyIdsClause(entityKeyIds))
            }
    //@formatter:off
    val columns = setOf(
            "${ID_VALUE.name}",
            if(metadataOptions.contains(MetadataOption.LAST_WRITE) ) {"${LAST_WRITE.name}" } else { "" },
            if(metadataOptions.contains(MetadataOption.LAST_INDEX) ) {"${LAST_INDEX.name}" } else { "" })
            .union( authorizedPropertyTypes.values.map(::quote ) )

    return "SELECT ${columns.filter(String::isNotBlank).joinToString (",")} \n" +
            "FROM $esTableName \n" +
            authorizedPropertyTypes
                    .map { "LEFT JOIN ${subSelectLatestVersionOfPropertyTypeInEntitySet(entitySetId, entityKeyIdsClause, it.key, it.value )} USING (${ID.name} )" }
                    .joinToString("\n" ) +
            if( entityKeyIdsClause.isPresent ) { " WHERE ${entityKeyIdsClause.get()} " } else  { " " }
    //@formatter:on
}

fun selectEntitySetWithPropertyTypesAndVersion(
        entitySetId: UUID,
        entityKeyIds: Set<UUID>,
        authorizedPropertyTypes: Map<UUID, String>,
        metadataOptions: Set<MetadataOption>,
        version: Long
): String {
    val esTableName = DataTables.quote(DataTables.entityTableName(entitySetId))
    val entityKeyIdsClause: Optional<String> =
            if (entityKeyIds.isEmpty()) {
                Optional.empty()
            } else {
                Optional.of(entityKeyIdsClause(entityKeyIds))
            }
    //@formatter:off
    val columns = setOf(
            "${ID_VALUE.name}",
            if(metadataOptions.contains(MetadataOption.LAST_WRITE) ) {"${LAST_WRITE.name}" } else { "" },
            if(metadataOptions.contains(MetadataOption.LAST_INDEX) ) {"${LAST_INDEX.name}" } else { "" })
            .union( authorizedPropertyTypes.values.map(::quote ) )

    return "SELECT ${columns.filter(String::isNotBlank).joinToString (",")} \n" +
            "FROM $esTableName \n" +
            authorizedPropertyTypes
                    .map { "LEFT JOIN ${selectVersionOfPropertyTypeInEntitySet(entitySetId, entityKeyIdsClause, it.key, it.value, version )} USING (${ID.name} )" }
                    .joinToString("\n" ) +
            if( entityKeyIdsClause.isPresent ) { " WHERE ${entityKeyIdsClause.get()} " } else  { " " }
    //@formatter:on
}

internal fun selectVersionOfPropertyTypeInEntitySet(
        entitySetId: UUID,
        entityKeyIdsClause: Optional<String>,
        propertyTypeId: UUID,
        fqn: String,
        version: Long
): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    return "(SELECT ${ENTITY_SET_ID.name}, " +
            "   ${ID_VALUE.name}, " +
            "   ${DataTables.quote(fqn)}, " +
            "   $MAX_PREV_VERSION " +
            "FROM ${subSelectFilteredVersionOfPropertyTypeInEntitySet(
                    entitySetId, entityKeyIdsClause, propertyTypeId, fqn, version
            )}" +
            "WHERE ARRAY[$MAX_PREV_VERSION] <@ versions) as $propertyTable AND ${entityKeyIdsClause.get()}"
}

// We could combine latest and versioned reads, but it's easier to understand if they are separate.
internal fun subSelectLatestVersionOfPropertyTypeInEntitySet(
        entitySetId: UUID,
        entityKeyIdsClause: Optional<String>,
        propertyTypeId: UUID,
        fqn: String
): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    return "(SELECT ${ENTITY_SET_ID.name}," +
            " ${ID_VALUE.name}," +
            " array_agg(${DataTables.quote(fqn)}) as ${DataTables.quote(fqn)}," +
            " ${VERSION.name} " +
            "FROM $propertyTable " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ${VERSION.name} >= 0 " +
            //@formatter:off
            if( entityKeyIdsClause.isPresent ) { " AND ${entityKeyIdsClause.get()} " } else { " " } +
            //@formatter:on
            "GROUP BY (${ENTITY_SET_ID.name}, ${ID_VALUE.name}, ${HASH.name})) as $propertyTable "
}

fun subSelectFilteredVersionOfPropertyTypeInEntitySet(
        entitySetId: UUID,
        entityKeyIdsClause: Optional<String>,
        propertyTypeId: UUID,
        fqn: String,
        version: Long
): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    return "(SELECT ${ENTITY_SET_ID.name}," +
            " ${ID_VALUE.name}, " +
            " ${HASH.name}, " +
            " array_agg(${DataTables.quote(fqn)}) as ${DataTables.quote(fqn)}," +
            " array_agg($EXPANDED_VERSIONS) as versions," +
            " max(abs($EXPANDED_VERSIONS)) as $MAX_PREV_VERSION " +
            "FROM $propertyTable, unnest(versions) as $EXPANDED_VERSIONS " +
            "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND abs($EXPANDED_VERSIONS) <= $version " +
            //@formatter:off
            if( entityKeyIdsClause.isPresent ) { " AND ${entityKeyIdsClause.get()} " } else { " " } +
            //@formatter:on
            "GROUP BY (${ENTITY_SET_ID.name}," +
            "   ${ID_VALUE.name}," +
            "   ${HASH.name})" +
            ") as $propertyTable "
}

internal fun entityKeyIdsClause(entityKeyIds: Set<UUID>): String {
    return "${ID_VALUE.name} IN ('" + entityKeyIds.joinToString("','") + "')"
}