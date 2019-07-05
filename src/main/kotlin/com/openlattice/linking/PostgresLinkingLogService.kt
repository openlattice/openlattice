package com.openlattice.linking

import com.fasterxml.jackson.databind.ObjectMapper
import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresTable.LINKING_LOG
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.sql.PreparedStatement
import java.util.*

class PostgresLinkingLogService(
        val hds: HikariDataSource,
        val mapper: ObjectMapper
) : LinkingLogService {

    override fun logLinkCreated(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>) {
        safePrepStatementExec(INSERT_LOG_SQL) { ps, conn  ->
            ps.setObject(1, linkingId)
            ps.setString(2, mapper.writeValueAsString(linkedEntities) )
            ps.setLong(3, System.currentTimeMillis())
        }
    }

    override fun logEntitiesAddedToLink(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>) {
        linkedEntities.forEach { esid, ekids ->
            safePrepStatementExec(ADD_LINK_SQL) { ps, conn  ->
                ps.setObject(1, linkingId)
                ps.setObject(2, linkingId)
                val asRay = PostgresArrays.createTextArray(conn, listOf(esid.toString()))
                ps.setArray(3, asRay)
                ps.setString(4, esid.toString())
                ps.setString(5, mapper.writeValueAsString(ekids))
                ps.setLong(6, System.currentTimeMillis())
                ps.setObject(7, linkingId)
                ps.addBatch()
            }
        }
    }

    override fun logEntitiesRemovedFromLink(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>) {
        linkedEntities.forEach { esid, ekids ->
            ekids.forEach { ekid ->
                safePrepStatementExec(REMOVE_ENTITY_SQL) { ps, conn ->
                    ps.setObject(1, linkingId)
//                    val asRay = PostgresArrays.createTextArray( conn, listOf(esid.toString(), ekid.toString()) )
//                    ps.setArray(2, asRay)
                    ps.setString(2, esid.toString())
                    ps.setString(3, ekid.toString())
                    ps.setObject(4, linkingId)
                    ps.addBatch()
                }
            }
        }
    }

    override fun readLatestLinkLog(linkingId: UUID) {
        safePrepStatementExec(READ_LATEST_LINKED_SQL) { ps, conn ->
            ps.setObject(1, linkingId)
        }
    }


    private fun safePrepStatementExec(prepStatementSql: String, bindFunc: (PreparedStatement, Connection) -> Unit ) {
        hds.connection.use { conn ->
            conn.prepareStatement(prepStatementSql).use { ps ->
                bindFunc(ps, conn)
                ps.execute()
            }
        }
    }
}

private val LOG_COLUMNS = LINKING_LOG.columns.joinToString(",", transform = PostgresColumnDefinition::getName)

private val INSERT_LOG_SQL = "INSERT INTO ${LINKING_LOG.name} ($LOG_COLUMNS) VALUES (?,?::jsonb,?)"

// Read latest version of a link
private val READ_LATEST_LINKED_SQL = "SELECT ${ID_MAP.name} FROM ${LINKING_LOG.name} WHERE ${LINKING_ID.name} = ? ORDER BY ${VERSION.name} LIMIT 1"

// Add a link. Grab previous value from a temp table
private val ADD_LINK_SQL = "WITH old_json as " +
        "( SELECT ${ID_MAP.name} as value FROM ${LINKING_LOG.name} WHERE ${LINKING_ID.name} = ? ORDER BY ${VERSION.name} LIMIT 1 ) " +
        "UPDATE ${LINKING_LOG.name} " +
        "SET ${LINKING_ID.name} = ?, " +
        "${ID_MAP.name} = jsonb_set( old_json.value, ?, COALESCE(${ID_MAP.name}->?, '[]'::jsonb) || ?::jsonb ), " +
        "${VERSION.name} = ? " +
        "FROM old_json " +
        "WHERE ${LINKING_ID.name} = ?"



// Delete Entity
private val REMOVE_ENTITY_SQL= "UPDATE ${LINKING_LOG.name} " +
        "SET ${ID_MAP.name} = " +
        "((SELECT ${ID_MAP.name} as value FROM ${LINKING_LOG.name} WHERE ${LINKING_ID.name} = ? ORDER BY ${VERSION.name} LIMIT 1 )->?) " +
        "- ? " +
        "WHERE ${LINKING_ID.name} = ?"
