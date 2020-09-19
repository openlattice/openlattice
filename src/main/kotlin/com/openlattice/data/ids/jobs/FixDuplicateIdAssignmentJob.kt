package com.openlattice.data.ids.jobs

import com.geekbeast.rhizome.jobs.AbstractDistributedJob
import com.openlattice.data.EntityKey
import com.openlattice.hazelcast.serializers.decorators.IdGenerationAware
import com.openlattice.hazelcast.serializers.decorators.MetastoreAware
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.postgres.PostgresColumn.ID
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.PostgresTable.SYNC_IDS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.PreparedStatementHolderSupplier
import com.zaxxer.hikari.HikariDataSource
import java.lang.IllegalStateException
import java.util.*

const val BATCH_SIZE = 128000

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class FixDuplicateIdAssignmentJob(
        state: FixDuplicateIdAssignmentJobState
) : AbstractDistributedJob<Long, FixDuplicateIdAssignmentJobState>(state), MetastoreAware,IdGenerationAware {
    @Transient
    private lateinit var hds: HikariDataSource

    @Transient
    private lateinit var idService: HazelcastIdGenerationService

    override fun processNextBatch() {
        /**
         * Process ids in batches.
         */
        val entityKeysAndIds = BasePostgresIterable(PreparedStatementHolderSupplier(hds, SELECT_NEXT_IDS) { ps ->
            ps.setObject(1, state.id)
        }) { rs ->
            ResultSetAdapters.id(rs) to ResultSetAdapters.entityKey(rs)
        }.groupBy ({ it.first } , {it.second})

        entityKeysAndIds
                .asSequence()
                .filter { it.value.size > 1 } //Only process entries with duplicates.
                .forEach { (id, entityKeys) ->
                    val entitySetAppearances = entityKeys.groupingBy{ it.entitySetId } .eachCount()

                    if( entitySetAppearances.values.all{ it == 1 } ) {
                        /**
                         * Each entity set appears only once. We will assign new ids to the entity keys and
                         */
                        assignNewIdsAndMoveData( id, entityKeys.subList(1, entityKeys.size) )

                    } else {
                        /**
                         * We have two entity keys in the same entity set assigned to the same id. This is
                         * extra special bad and we do not know how to handle without manual intervention
                         */
                    }
                }

        state.id = entityKeysAndIds.keys.max() ?: throw IllegalStateException("Entity key ids cannot be empty.")
    }

    private fun assignNewIdsAndMoveData(originalId: UUID, entityKeys: List<EntityKey>) {
        val newIds = idService.getNextIds(entityKeys.size)
        clearIdAssignment( originalId, entityKeys )
        entityKeys.forEach { entityKey ->


        }
    }

    private fun clearIdAssignment(originalId: UUID, entityKeys: List<EntityKey>) {
        TODO("Not yet implemented")
    }

    override fun initialize() {
        state.idCount = getIdCount()
    }

    override fun setHikariDataSource(hds: HikariDataSource) {
        this.hds = hds
    }


    private fun getIdCount(): Long {
        return hds.connection.use { connection ->
            connection.createStatement().use { statement ->
                val rs = statement.executeQuery(COUNT_IDS)
                if (rs.next()) {
                    ResultSetAdapters.count(rs)
                } else {
                    throw IllegalStateException("Result set for counting ids cannot empty.")
                }
            }

        }
    }

    override fun setIdGenerationService(idService: HazelcastIdGenerationService) {
        this.idService = idService
    }
}

private val COUNT_IDS = """
SELECT count(*) FROM ${IDS.name}
""".trimIndent()
private val SELECT_NEXT_IDS = """
SELECT * FROM ${SYNC_IDS.name} WHERE ${ID.name} IN (SELECT ${ID.name} FROM ${IDS.name} WHERE ${ID.name} > ? ORDER BY ${ID.name} LIMIT $BATCH_SIZE)
""".trimIndent()