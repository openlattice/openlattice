package com.openlattice.linking

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresTable.LINKING_LOG
import com.zaxxer.hikari.HikariDataSource
import java.util.*

class PostgresLinkingLogService(
        val hds: HikariDataSource,
        val mapper: ObjectMapper
) : LinkingLogService {

    override fun createOrUpdateCluster(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>, newCluster: Boolean) {
        if ( newCluster ) {
            createCluster( linkingId, linkedEntities )
        } else {
            updateCluster( linkingId, linkedEntities )
        }
    }

    private fun createCluster(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>) {
        hds.connection.use { conn ->
            conn.prepareStatement(INSERT_LOG_SQL).use { ps ->
                ps.setObject(1, linkingId)
                ps.setString(2, mapper.writeValueAsString(linkedEntities))
                ps.setLong(3, System.currentTimeMillis())
                ps.executeUpdate()
            }
        }
    }

    private fun updateCluster(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>) {
        hds.connection.use { conn ->
            conn.prepareStatement(ADD_LINK_SQL).use { ps ->
                linkedEntities.forEach { (esid, ekids) ->
                    val asRay = PostgresArrays.createTextArray(conn, listOf(esid.toString()))
                    ps.setArray(1, asRay)
                    ps.setString(2, esid.toString())
                    ps.setString(3, mapper.writeValueAsString(ekids))
                    ps.setLong(4, System.currentTimeMillis())
                    ps.setObject(5, linkingId)
                    ps.addBatch()
                }
                ps.executeUpdate()
            }
        }
    }

    override fun clearEntitiesFromCluster(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>) {
        hds.connection.use { conn ->
            conn.prepareStatement(REMOVE_ENTITY_SQL).use { ps ->
                linkedEntities.forEach { (esid, ekids) ->
                    val ekidsArray = PostgresArrays.createTextArray(conn, ekids.stream().map { it.toString() } )
                    ps.setString(1, esid.toString())
                    ps.setArray(2, ekidsArray )
                    ps.setArray(3, PostgresArrays.createTextArray( conn, listOf( esid.toString() )))
                    ps.setString(4, esid.toString())
                    ps.setArray(5, ekidsArray )
                    ps.setString(6, esid.toString())
                    ps.setLong(7, System.currentTimeMillis())
                    ps.setObject(8, linkingId)
                    ps.addBatch()
                }
                ps.executeUpdate()
            }
        }
    }

    override fun readVersion(linkingId: UUID, version: Long): Map<UUID, Set<UUID>> {
        hds.connection.use { conn ->
            conn.prepareStatement(READ_VERSION_LINKED_SQL).use { ps ->
                ps.setObject(1, linkingId)
                ps.setLong(2, version)
                ps.executeQuery().use { rs->
                    rs.next()
                    val searchConstraintsJson = rs.getString( ID_MAP_FIELD )
                    return mapper.readValue( searchConstraintsJson )
                }
            }
        }
    }

    override fun readLatestLinkLog(linkingId: UUID): Map<UUID, Set<UUID>> {
        hds.connection.use { conn ->
            conn.prepareStatement(READ_LATEST_LINKED_SQL).use { ps ->
                ps.setObject(1, linkingId)
                ps.executeQuery().use { rs->
                    rs.next()
                    return mapper.readValue( rs.getString( ID_MAP_FIELD ))
                }
            }
        }
    }
}

private val READ_VERSION_LINKED_SQL = "SELECT ${ID_MAP.name} " +
        "FROM ${LINKING_LOG.name} " +
        "WHERE ${LINKING_ID.name} = ? AND ${VERSION.name} <= ? " +
        "ORDER BY ${VERSION.name} DESC " +
        "LIMIT 1"

private val LOG_COLUMNS = LINKING_LOG.columns.joinToString(",", transform = PostgresColumnDefinition::getName)

private val APPEND_PREFIX = "INSERT INTO ${LINKING_LOG.name} ($LOG_COLUMNS) "

private val INSERT_LOG_SQL = "$APPEND_PREFIX VALUES (?,?::jsonb,?)"

private val APPEND_SUFFIX = "FROM ${LINKING_LOG.name} " +
        "WHERE ${LINKING_ID.name}=? " +
        "ORDER BY ${VERSION.name} DESC " +
        "LIMIT 1 "

private val ADD_LINK_SQL = APPEND_PREFIX +
        "( SELECT ${LINKING_ID.name}, " +
            "jsonb_set( ${ID_MAP.name}, ?, COALESCE(${ID_MAP.name}->?, '[]'::jsonb) || ?::jsonb ), " +
            "? " +
        "$APPEND_SUFFIX ) "

// Clear Entity
private val REMOVE_ENTITY_SQL= APPEND_PREFIX +
        "( SELECT ${LINKING_ID.name}, " +
            "CASE WHEN jsonb_array_length( (${ID_MAP.name}->?)-? ) > 0 " +
                "THEN jsonb_set( ${ID_MAP.name}, ?, (${ID_MAP.name}->?)-? ) " + // resulting array is *not* empty
                "ELSE ${ID_MAP.name}-? " +                                      // resulting array is empty
            "END" +
            ", ? " +
        "$APPEND_SUFFIX ) "

private val READ_LATEST_LINKED_SQL = "SELECT ${ID_MAP.name} $APPEND_SUFFIX"

