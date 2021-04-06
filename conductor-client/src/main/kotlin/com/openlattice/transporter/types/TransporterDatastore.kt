package com.openlattice.transporter.types

import com.geekbeast.configuration.postgres.PostgresConfiguration
import com.kryptnostic.rhizome.configuration.RhizomeConfiguration
import com.openlattice.ApiHelpers
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.authorization.Acl
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Action
import com.openlattice.edm.EntitySet
import com.openlattice.edm.PropertyTypeIdFqn
import com.openlattice.postgres.PostgresProjectionService
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.TableColumn
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager
import com.openlattice.postgres.external.ExternalDatabasePermissioningService
import com.openlattice.postgres.external.Schemas
import com.openlattice.transporter.*
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.*

/**
 * TransporterDatastore configures entries in [rhizome] postgres configuration
 */
@Component
class TransporterDatastore(
        private val assemblerConfiguration: AssemblerConfiguration,
        rhizome: RhizomeConfiguration,
        private val exConnMan: ExternalDatabaseConnectionManager,
        private val exDbPermMan: ExternalDatabasePermissioningService
) {
    companion object {
        private val logger = LoggerFactory.getLogger(TransporterDatastore::class.java)

        // database in atlas where the data is transported
        const val TRANSPORTER_DB_NAME = "transporter"

        // fdw name for atlas <-> production fdw
        const val ENTERPRISE_FDW_NAME = "enterprise"
    }

    private var transporterHds: HikariDataSource = exConnMan.createDataSource(
            TRANSPORTER_DB_NAME,
            assemblerConfiguration.server.clone() as Properties,
            assemblerConfiguration.ssl
    )

    init {
        logger.info("Initializing TransporterDatastore")
        if (rhizome.postgresConfiguration.isPresent) {
            initializeFDW(rhizome.postgresConfiguration.get())
        }
        val sp = PostgresProjectionService.loadSearchPathForCurrentUser(transporterHds)
        if (!sp.contains(Schemas.ENTERPRISE_FDW_SCHEMA.label)) {
            logger.error("bad search path: {}", sp)
        }
    }

    fun transportEntitySet(
            organizationId: UUID,
            es: EntitySet,
            ptIdToFqnColumns: Set<PropertyTypeIdFqn>,
            columnAcls: List<Acl>,
            columnsById: Map<AclKey, TableColumn>
    ) {
        val esName = es.name
        val orgHds = exConnMan.connectToOrg(organizationId)

        linkOrgDbToTransporterDb(orgHds, organizationId, es.entityTypeId)

        destroyEdgeViewInOrgDb(orgHds, esName)

        // import edges table from foreign server
        importEdgesTableToOrg(orgHds, organizationId)

        // import et table from foreign server
        PostgresProjectionService.importTablesFromFdw(
                orgHds,
                constructFdwName(organizationId),
                Schemas.PUBLIC_SCHEMA.label,
                setOf(entityTypeTableName(es.entityTypeId)),
                Schemas.TRANSPORTER_SCHEMA.label
        )

        // create edge view in org db
        createEdgeViewInOrgDb(
                orgHds,
                esName,
                es.id
        )

        // create view in org db
        createEntitySetViewInOrgDb(
                orgHds,
                es.id,
                esName,
                es.entityTypeId,
                ptIdToFqnColumns
        )

        // create roles, apply permissions
        applyViewAndEdgePermissions(
                orgHds,
                es.id,
                esName,
                ptIdToFqnColumns,
                columnAcls,
                columnsById
        )
    }

    fun destroyTransportedEntitySet(
            organizationId: UUID,
            entitySetId: UUID,
            entityTypeId: UUID,
            name: String,
            orgHds: HikariDataSource = exConnMan.connectToOrg(organizationId)
    ) {
        destroyEdgeViewInOrgDb(orgHds, name)

        destroyEntitySetViewInOrgDb(orgHds, name)

        removePreviouslyTransported(orgHds, entitySetId, entityTypeId)
    }

    private fun linkOrgDbToTransporterDb(
            orgDatasource: HikariDataSource,
            organizationId: UUID,
            entityTypeId: UUID
    ) {
        val fdwName = constructFdwName(organizationId)
        PostgresProjectionService.createFdwBetweenDatabases(
                orgDatasource,
                assemblerConfiguration.server.getProperty("username"),
                assemblerConfiguration.server.getProperty("password"),
                exConnMan.appendDatabaseToJdbcPartial(
                        assemblerConfiguration.server.getProperty("jdbcUrl"),
                        TRANSPORTER_DB_NAME
                ),
                assemblerConfiguration.server.getProperty("username"),
                Schemas.TRANSPORTER_SCHEMA,
                fdwName
        )

        PostgresProjectionService.importTablesFromFdw(
                orgDatasource,
                fdwName,
                Schemas.PUBLIC_SCHEMA.label,
                setOf(quotedEtTableName(entityTypeId)),
                Schemas.TRANSPORTER_SCHEMA.label
        )
    }

    private fun initializeFDW(rhizomeConfig: PostgresConfiguration) {
        PostgresProjectionService.createFdwBetweenDatabases(
                transporterHds,
                rhizomeConfig.hikariConfiguration.getProperty("username"),
                rhizomeConfig.hikariConfiguration.getProperty("password"),
                rhizomeConfig.hikariConfiguration.getProperty("jdbcUrl").removeSuffix("?sslmode=require"),
                assemblerConfiguration.server.getProperty("username"),
                Schemas.ENTERPRISE_FDW_SCHEMA,
                ENTERPRISE_FDW_NAME
        )

        PostgresProjectionService.importTablesFromFdw(
                transporterHds,
                ENTERPRISE_FDW_NAME,
                Schemas.PUBLIC_SCHEMA.label,
                setOf(
                        PostgresTable.IDS.name,
                        PostgresTable.DATA.name,
                        PostgresTable.E.name
                ),
                Schemas.ENTERPRISE_FDW_SCHEMA.label
        )

        transporterHds.close()
        transporterHds = exConnMan.connectToTransporter()
    }

    fun datastore(): HikariDataSource {
        return transporterHds
    }

    private fun constructFdwName(organizationId: UUID): String {
        return ApiHelpers.dbQuote("fdw_$organizationId")
    }

    fun removePreviouslyTransported(
            orgDatasource: HikariDataSource,
            entitySetId: UUID,
            entityTypeId: UUID
    ) {
        // update me to remove edges as well
        orgDatasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(
                        removePreviouslyTransportedEntities(Schemas.TRANSPORTER_SCHEMA, entitySetId, entityTypeId)
                )
            }
        }
    }

    fun destroyTransportedEntityTypeTableInOrg(
            orgDatasource: HikariDataSource,
            entityTypeId: UUID
    ) {
        orgDatasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(
                        dropForeignTypeTable(Schemas.TRANSPORTER_SCHEMA, entityTypeId)
                )
            }
        }
    }

    fun destroyEntitySetViewInOrgDb(
            orgDatasource: HikariDataSource,
            entitySetName: String
    ) {
        orgDatasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(destroyView(Schemas.ASSEMBLED_ENTITY_SETS, entitySetName))
            }
        }
    }

    fun destroyEdgeViewInOrgDb(
            orgDatasource: HikariDataSource,
            entitySetName: String) {
        orgDatasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(destroyEdgeView(Schemas.ASSEMBLED_ENTITY_SETS, entitySetName))
            }
        }
    }

    private fun createEdgeViewInOrgDb(
            orgDatasource: HikariDataSource,
            entitySetName: String,
            entitySetId: UUID
    ) {
        orgDatasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(
                        createEdgeSetViewInSchema(
                                entitySetName,
                                entitySetId,
                                Schemas.ASSEMBLED_ENTITY_SETS,
                                Schemas.TRANSPORTER_SCHEMA
                        )
                )
            }
        }
    }

    private fun createEntitySetViewInOrgDb(
            orgDatasource: HikariDataSource,
            entitySetId: UUID,
            entitySetName: String,
            entityTypeId: UUID,
            ptIdToFqnColumns: Set<PropertyTypeIdFqn>
    ) {
        orgDatasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeUpdate(
                        createEntitySetViewInSchemaFromSchema(
                                entitySetName,
                                entitySetId,
                                Schemas.ASSEMBLED_ENTITY_SETS,
                                entityTypeId,
                                ptIdToFqnColumns,
                                Schemas.TRANSPORTER_SCHEMA
                        )
                )
            }
        }
    }

    private fun applyViewAndEdgePermissions(
            orgDatasource: HikariDataSource,
            entitySetId: UUID,
            entitySetName: String,
            ptIdToFqnColumns: Set<PropertyTypeIdFqn>,
            columnAcls: List<Acl>,
            columnsById: Map<AclKey, TableColumn>
    ) {
        exDbPermMan.initializeAssemblyPermissions(
                orgDatasource,
                entitySetId,
                entitySetName,
                ptIdToFqnColumns.toSet()
        )

        exDbPermMan.updateAssemblyPermissions(
                Action.SET,
                columnAcls,
                columnsById
        )
    }

    private fun checkIfTableExists(
            orgDatasource: HikariDataSource,
            schema: Schemas,
            tableName: String
    ): Boolean {
        orgDatasource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery(checkIfTableExistsQuery(schema, tableName)).use { rs ->
                    return rs.next() && rs.getBoolean(1)
                }
            }
        }
    }

    private fun importEdgesTableToOrg(
            orgDatasource: HikariDataSource,
            organizationId: UUID
    ) {
        val edgesTableExists = checkIfTableExists(
                orgDatasource,
                Schemas.TRANSPORTER_SCHEMA,
                MAT_EDGES_TABLE_NAME
        )
        if (edgesTableExists) {
            return
        }

        PostgresProjectionService.importTableFromFdw(
                orgDatasource,
                constructFdwName(organizationId),
                Schemas.PUBLIC_SCHEMA.label,
                MAT_EDGES_TABLE_NAME,
                Schemas.TRANSPORTER_SCHEMA.label
        )
    }
}