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

import com.google.common.collect.SetMultimap
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.*
import com.openlattice.postgres.DataTables.*
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.*
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
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


class PostgresEntityDataQueryServiceOld(
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
        return getLinkedEntityDataWithMetadata(
                linkingIdsByEntitySetId,
                authorizedPropertyTypesByEntitySetId,
                EnumSet.noneOf(MetadataOption::class.java)
        )
    }

    fun getLinkedEntityDataWithMetadata(
            linkingIdsByEntitySetId: Map<UUID, Optional<Set<UUID>>>,
            authorizedPropertyTypesByEntitySetId: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: EnumSet<MetadataOption>
    ): PostgresIterable<Pair<Pair<UUID, UUID>, Map<UUID, Set<Any>>>> {

        val adapter = Function<ResultSet, Pair<Pair<UUID, UUID>, Map<UUID, Set<Any>>>> {
            Pair(ResultSetAdapters.linkingId(it), ResultSetAdapters.entitySetId(it)) to
                    if (metadataOptions.isEmpty()) {
                        ResultSetAdapters.implicitEntityValuesById(
                                it, authorizedPropertyTypesByEntitySetId, byteBlobDataManager
                        )
                    } else {
                        ResultSetAdapters.implicitEntityValuesByIdWithLastWrite(
                                it, authorizedPropertyTypesByEntitySetId.values.firstOrNull() ?: mapOf(),
                                byteBlobDataManager
                        )
                    }
        }
        return streamableEntitySet(
                linkingIdsByEntitySetId, authorizedPropertyTypesByEntitySetId,
                metadataOptions, Optional.empty(), adapter, true
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

    @JvmOverloads
    fun streamableEntitySet(
            entitySetId: UUID,
            entityKeyIds: Set<UUID>,
            authorizedPropertyTypes: Map<UUID, Map<UUID, PropertyType>>,
            metadataOptions: Set<MetadataOption>,
            version: Optional<Long> = Optional.empty()
    ): PostgresIterable<SetMultimap<FullQualifiedName, Any>> {
        return streamableEntitySet(
                mapOf(entitySetId to Optional.of(entityKeyIds)),
                authorizedPropertyTypes,
                metadataOptions,
                version
        )
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
                    ResultSetAdapters.implicitEntityValuesByIdWithLastWrite(
                            it, authorizedPropertyTypes, byteBlobDataManager
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

                    connection.autoCommit = false
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
                                        authorizedPropertyTypes.mapValues { it.value.keys },
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
                                        authorizedPropertyTypes.mapValues { it.value.keys },
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

    fun getEntityKeyIdsInEntitySet(entitySetId: UUID): PostgresIterable<UUID> {
        val adapter = Function<ResultSet, UUID> {
            ResultSetAdapters.id(it)
        }
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.connection
            val statement = connection.prepareStatement(getEntityKeyIdsOfEntitySetQuery())
            statement.setObject(1, entitySetId)
            val rs = statement.executeQuery()
            StatementHolder(connection, statement, rs)
        }, adapter)
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
            connection.autoCommit = false
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
            connection.autoCommit = false
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
            connection.autoCommit = false
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
}




fun selectEntitySetWithPropertyTypes(
        entitySetId: UUID,
        entityKeyIds: Optional<Set<UUID>>,
        authorizedPropertyTypes: Map<UUID, String>,
        metadataOptions: Set<MetadataOption>,
        binaryPropertyTypes: Map<UUID, Boolean>
): String {
    val esTableName = quote(entityTableName(entitySetId))

    val entityKeyIdsClause = entityKeyIds.map { "AND ${entityKeyIdsClause(it)} " }.orElse(" ")
    //@formatter:off
    val columns = setOf(ID_VALUE.name) +
            metadataOptions.map { ResultSetAdapters.mapMetadataOptionToPostgresColumn(it) } +
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

internal fun selectVersionOfPropertyTypeInEntitySet(
        entitySetId: UUID,
        entityKeyIdsClause: String,
        propertyTypeId: UUID,
        fqn: String,
        version: Long,
        binary: Boolean
): String {
    val propertyTable = quote(propertyTableName(propertyTypeId))
    val arrayAgg = " array_agg(${quote(fqn)}) as ${quote(fqn)} "


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
    val arrayAgg = " array_agg(${quote(fqn)}) as ${quote(fqn)} "

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
    val filterLinkingIds = " AND ${LINKING_ID.name} IS NOT NULL "
    val withClause = buildWithClause(true, entitiesClause + filterLinkingIds)
    val joinColumnsSql = (entityKeyIdColumnsList + LINKING_ID.name).joinToString(",")

    return "$withClause SELECT ${ENTITY_SET_ID.name}, array_agg(${LINKING_ID.name}) as ${LINKING_ID.name} " +
            "FROM $FILTERED_ENTITY_KEY_IDS " +
            "INNER JOIN ${ENTITY_KEY_IDS.name} USING($joinColumnsSql) " +
            "GROUP BY ${ENTITY_SET_ID.name} "
}

internal fun selectLinkingIdsOfEntitySet(entitySetId: UUID): String {
    return "SELECT DISTINCT ${LINKING_ID.name} " +
            "FROM ${ENTITY_KEY_IDS.name} " +
            "WHERE ${VERSION.name} > 0 AND ${LINKING_ID.name} IS NOT NULL AND ${ENTITY_SET_ID.name} = '$entitySetId'"
}

internal fun getLinkingEntitySetIdsOfLinkingIdQuery(linkingId: UUID): String {
    val selectEntitySetIdOfLinkingId =
            "SELECT DISTINCT ${ENTITY_SET_ID.name} " +
                    "FROM ${ENTITY_KEY_IDS.name} " +
                    "WHERE ${LINKING_ID.name} = '$linkingId'"
    val wrapLocalTable = "SELECT ${ID.name}, ${LINKED_ENTITY_SETS.name} from ${ENTITY_SETS.name}"
    return "SELECT ${ID.name} " +
            "FROM ( $wrapLocalTable ) as entity_set_ids " +
            "INNER JOIN ( $selectEntitySetIdOfLinkingId ) as linked_es " +
            "ON ( ${ENTITY_SET_ID.name}= ANY( ${LINKED_ENTITY_SETS.name} ) )"
}

internal fun getLinkingEntitySetIdsOfEntitySetIdQuery(entitySetId: UUID): String {
    return "SELECT ${ID.name} " +
            "FROM ${ENTITY_SETS.name} " +
            "WHERE '$entitySetId' = ANY(${LINKED_ENTITY_SETS.name})"
}

internal fun getEntityKeyIdsOfEntitySetQuery(): String {
    return "SELECT ${ID.name} FROM ${ENTITY_KEY_IDS.name} WHERE ${ENTITY_SET_ID.name} = ? "
}

