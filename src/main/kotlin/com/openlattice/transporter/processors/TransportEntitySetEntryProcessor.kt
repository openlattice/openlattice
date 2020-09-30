package com.openlattice.transporter.processors

import com.hazelcast.core.Offloadable
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.transporter.createEntitySetView
import com.openlattice.transporter.tableName
import com.openlattice.transporter.types.TransporterDatastore
import com.openlattice.transporter.types.TransporterDependent
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*

/**
 *
 * @author Drew Bailey &lt;drew@openlattice.com&gt;
 */
data class TransportEntitySetEntryProcessor(
        val columns: Map<UUID, FullQualifiedName>,
        val organizationId: UUID,
        val usersToColumnPermissions: Map<String, List<String>>
): AbstractRhizomeEntryProcessor<UUID, EntitySet, Void?>(),
        Offloadable,
        TransporterDependent<TransportEntitySetEntryProcessor>
{
    @Transient
    private lateinit var data: TransporterDatastore

    companion object {
        private val logger = LoggerFactory.getLogger(TransportEntitySetEntryProcessor::class.java)
    }

    override fun process(entry: MutableMap.MutableEntry<UUID, EntitySet>): Void? {
        check(::data.isInitialized) { TransporterDependent.NOT_INITIALIZED }
        val es = entry.value
        if ( es.flags.contains(EntitySetFlag.TRANSPORTED)){
            return null
        }
        es.flags.add(EntitySetFlag.TRANSPORTED)
        entry.setValue(es)

        try {
            data.linkOrgDbToTransporterDb( organizationId )

            data.datastore().connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeUpdate(
                            createEntitySetView(
                                    es.name,
                                    entry.key,
                                    tableName(es.entityTypeId),
                                    columns
                            )
                    )
                }
            }

            data.createOrgDataSource(organizationId).connection.use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.executeUpdate(
                            data.importTablesFromForeignSchema(
                                    TransporterDatastore.ENTERPRISE_FDW_SCHEMA,
                                    setOf(es.name),
                                    TransporterDatastore.ORG_VIEWS_SCHEMA,
                                    data.getOrgFdw( organizationId )
                            )
                    )
                    // TODO - need to apply these as roles due to the maximum row width thing
                    usersToColumnPermissions.forEach { ( username, allowedCols ) ->
                        stmt.addBatch(
                                AssemblerConnectionManager.grantSelectSql(es.name, username, allowedCols)
                        )
                    }
//                stmt.executeBatch()
                }
            }
        } catch ( ex: Exception ) {
            logger.error("Marking entity set id as not materialized {}", entry.key, ex)
            es.flags.remove(EntitySetFlag.TRANSPORTED)
            entry.setValue( es )
        }
        return null
    }

    override fun getExecutorName(): String {
        return Offloadable.OFFLOADABLE_EXECUTOR
    }

    override fun init(data: TransporterDatastore): TransportEntitySetEntryProcessor {
        this.data = data
        return this
    }
}