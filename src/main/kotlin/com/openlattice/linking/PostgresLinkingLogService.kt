package com.openlattice.linking

import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresTable.LINKING_LOG
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.sql.PreparedStatement
import java.util.*

class PostgresLinkingLogService( val hds: HikariDataSource ) : LinkingLogService {

    override fun createOrUpdateForLinks(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>) {

        safePrepStatementExec(INSERT_LOG_SQL) { ps, conn  ->
            // edk -> esid -> List<ekid> -> PGArray
            linkedEntities.forEach {( esid, ekids ) ->
                ekids.forEach {ekid ->
                    ps.setObject(1, linkingId)
                    ps.setObject(2, esid)
                    ps.setObject(3, ekid)
                    ps.setLong(4, System.currentTimeMillis())
                    ps.addBatch()
                }
            }
        }
    }

    override fun clearLink(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>) {
            safePrepStatementExec(INSERT_LOG_SQL) { ps, conn ->
            // edk -> esid -> List<ekid> -> PGArray
            linkedEntities.forEach { (esid, ekids) ->
                ekids.forEach { ekid ->
                    ps.setObject(1, linkingId)
                    ps.setObject(2, esid)
                    ps.setObject(3, ekid)
                    ps.setLong(4, System.currentTimeMillis() * -1)
                    ps.addBatch()
                }
            }
        }
    }

    override fun deleteLink(linkingId: UUID, entitySetId: UUID ) {
        safePrepStatementExec(DELETE_LOG_SQL) { ps, conn ->
            ps.setObject(1, linkingId)
            ps.setObject(2, entitySetId)
        }
    }

    override fun deleteEntitiesFromLink(linkingId: UUID, entitySetIdToEntityKeyIds: Map<UUID, Set<UUID>>) {
        safePrepStatementExec(DELETE_LOG_SQL) { ps, conn ->
            entitySetIdToEntityKeyIds.forEach { ( esid, ekids ) ->
                ekids.forEach { ekid ->
                    ps.setObject(1, linkingId)
                    ps.setObject(2, esid)
                    ps.setObject(3, ekid)
                    ps.addBatch()
                }
            }
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

private val INSERT_LOG_SQL = "INSERT INTO ${LINKING_LOG.name} ($LOG_COLUMNS) VALUES (?,?,?,?)"

private val DELETE_LOG_SQL = "DELETE FROM ${LINKING_LOG.name} WHERE ${LINKING_ID.name} = ? AND ${ENTITY_SET_ID.name} = ?"

private val DELETE_ENTITIES_SQL = "DELETE FROM ${LINKING_LOG.name} " +
        "WHERE ${LINKING_ID.name} = ? " +
        "AND ${ENTITY_SET_ID.name} = ? " +
        "AND ${ENTITY_ID.name} = ?"
