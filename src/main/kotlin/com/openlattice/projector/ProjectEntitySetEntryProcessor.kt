package com.openlattice.projector

import com.hazelcast.core.Offloadable
import com.openlattice.ApiUtil
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.postgres.PostgresColumn
import com.openlattice.rhizome.hazelcast.entryprocessors.AbstractReadOnlyRhizomeEntryProcessor
import com.openlattice.transporter.tableName
import com.openlattice.transporter.types.TransporterColumnSet
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import java.util.*

/**
 *
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
class ProjectEntitySetEntryProcessor(
        val columns: TransporterColumnSet,
        val organizationId: UUID,
        val usersToColumnPermissions: Map<String, List<String>>
): AbstractReadOnlyRhizomeEntryProcessor<UUID, EntitySet, Unit>(),
        Offloadable,
        TransporterDependent
{
    private lateinit var data: TransporterDatastore

    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>) {
        val es = entry.value
        es.flags.add(EntitySetFlag.MATERIALIZED)

        data.createOrgDataSource( organizationId ).connection.use { conn ->
            conn.createStatement().executeUpdate(
                    createEntityTypeView(
                            es.name,
                            entry.key,
                            tableName(es.entityTypeId),
                            columns
                    )
            )
            conn.createStatement().use { statement ->
                usersToColumnPermissions.forEach { ( username, allowedCols ) ->
                    statement.addBatch(
                            AssemblerConnectionManager.grantSelectSql( es.name, username, allowedCols )
                    )
                }
                statement.executeBatch()
            }
        }

        entry.setValue(es)
    }

    fun createEntityTypeView(
            entitySetName: String,
            entitySetId: UUID,
            etTableName: String,
            cols: TransporterColumnSet
    ): String {
        val colsSql = cols.map {( id, col ) ->
            val column = ApiUtil.dbQuote(id.toString())
            "$column as ${col.dataTableColumnName}"
        }.joinToString()

        return """
            CREATE VIEW $entitySetName AS 
                SELECT $colsSql FROM $etTableName
                WHERE ${PostgresColumn.ENTITY_SET_ID.name} = $entitySetId
        """.trimIndent()
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }

    override fun init(data: TransporterDatastore) {
        this.data = data
    }
}