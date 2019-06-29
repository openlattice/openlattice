package com.openlattice.linking

import com.openlattice.postgres.PostgresArrays
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresTable.LINKING_LOG
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.sql.PreparedStatement
import java.util.*

class PostgresLinkingLogService( val hds: HikariDataSource ) : LinkingLogService {

    override fun createOrUpdateForLinks(linkingId: UUID, linkedEntities: Map<UUID, Set<UUID>>) {

        safePrepStatementExec(UPSERT_LINKING_LOG_SQL) { ps, conn  ->
            // edk -> esid -> List<ekid> -> PGArray
            linkedEntities.mapValues {
                PostgresArrays.createUuidArray(conn, it.value.stream())
            }.forEach { (esid, pgArray) ->
                ps.setObject(1, linkingId)
                ps.setObject(2, esid)
                ps.setArray(2, pgArray)
                ps.addBatch()
            }
        }
    }

    fun safePrepStatementExec( prepStatementSql: String, bindFunc: (PreparedStatement, Connection) -> Unit ) {
        hds.connection.use { conn ->
            conn.prepareStatement(prepStatementSql).use { ps ->
                bindFunc(ps, conn)
                ps.execute()
            }
        }
    }
}

private val LOG_COLUMNS = LINKING_LOG.columns.joinToString(",", transform = PostgresColumnDefinition::getName)

private val UPSERT_LINKING_LOG_SQL = "INSERT INTO ${LINKING_LOG.name} ($LOG_COLUMNS) VALUES (?,?,?,now())"

