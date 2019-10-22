package com.openlattice

import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.assembler.AssemblerConnectionManager
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.indexing.MAX_DURATION_MILLIS
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organizations.ExternalDatabaseManagementService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.util.*

/**
 *
 */
class BackgroundExternalDatabaseUpdatingService(
        private val hazelcastInstance: HazelcastInstance,
        private val edms: ExternalDatabaseManagementService
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundExternalDatabaseUpdatingService::class.java)
    }

    private val organizationExternalDatabaseColumns: IMap<UUID, OrganizationExternalDatabaseColumn> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COlUMN.name)
    private val organizationExternalDatabaseTables: IMap<UUID, OrganizationExternalDatabaseTable> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.name)
    private val securableObjectTypes: IMap<AclKey, SecurableObjectType> = hazelcastInstance.getMap(HazelcastMap.SECURABLE_OBJECT_TYPES.name)
    private val aclKeys: IMap<String, UUID> = hazelcastInstance.getMap(HazelcastMap.ACL_KEYS.name)

    @Scheduled(fixedRate = MAX_DURATION_MILLIS)
    fun scanOrganizationDatabases() {
        //get organizationIds
        val orgIds = edms.getOrganizationDBNames()
        orgIds.forEach {orgId ->
            val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
            val currentTableIds = mutableSetOf<UUID>()
            val currentColumnsByTableId = edms.getColumnIdsByTable(orgId, dbName, currentTableIds)
            currentColumnsByTableId.forEach { (tableName, columnNames) ->
                //check if table existed previously
                val fqn = FullQualifiedName(orgId.toString(), tableName)
                val id = aclKeys[fqn.fullQualifiedNameAsString]
                if (id == null) {
                    //create new securable object for this table
                    //TODO handling of permissions
                    val newTable = OrganizationExternalDatabaseTable(Optional.empty(), tableName, tableName, Optional.empty(), orgId)
                    val newTableId = edms.createOrganizationExternalDatabaseTable(orgId, newTable)
                    currentTableIds.add(newTableId)
                    val newColumns = edms.createNewColumnObjects(dbName, tableName, newTableId, orgId)
                    newColumns.forEach {
                        edms.createOrganizationExternalDatabaseColumn(orgId, it)
                        //TODO handling of permissions
                    }
                } else {
                    currentTableIds.add(id)
                }

            }
            //check if tables have been deleted (i.e. if our map is out of date)
            val missingIds = organizationExternalDatabaseColumns.keys - currentTableIds
            if (missingIds.isNotEmpty()) {
                edms.deleteOrganizationExternalDatabaseTables(orgId, missingIds)
            }
        }


    }
    private fun getExternalDatabaseObjectId(containingObjectId: UUID, name: String): UUID {
        val fqn = FullQualifiedName(containingObjectId.toString(), name)
        val id = aclKeys.get(fqn.fullQualifiedNameAsString)
        checkState(id != null, "External database object with name $name does not exist")
        return id!!
    }

}