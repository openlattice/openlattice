package com.openlattice.organizations

import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.PostgresRoles.Companion.buildAtlasPostgresUsername
import com.openlattice.assembler.PostgresRoles.Companion.buildPostgresUsername
import com.openlattice.authorization.*
import com.openlattice.authorization.securable.SecurableObjectType
import com.openlattice.controllers.exceptions.BadRequestException
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.OrganizationAtlasColumn
import com.openlattice.organization.OrganizationAtlasTable
import com.openlattice.organizations.roles.SecurePrincipalsManager

import com.openlattice.postgres.DataTables.quote
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*

class AtlasDataService(
        private val hazelcastInstance: HazelcastInstance,
        private val hds: HikariDataSource,
        private val assemblerConfiguration: AssemblerConfiguration, //for now using this, may need to make a separate one
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val aclKeyReservations: HazelcastAclKeyReservationService,
        private val authorizationManager: AuthorizationManager
) {

    private val organizationAtlasColumns: IMap<UUID, OrganizationAtlasColumn> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_ATLAS_COlUMN.name)
    private val organizationAtlasTables: IMap<UUID, OrganizationAtlasTable> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_ATLAS_TABLE.name)
    private val securableObjectTypes: IMap<AclKey, SecurableObjectType> = hazelcastInstance.getMap(HazelcastMap.SECURABLE_OBJECT_TYPES.name)
    private val logger = LoggerFactory.getLogger(AtlasDataService::class.java)

    //lifted from assembly connection manager, likely will need to be customized
    companion object {
        @JvmStatic
        fun connect(dbName: String, config: Properties, useSsl: Boolean): HikariDataSource {
            config.computeIfPresent("jdbcUrl") { _, jdbcUrl ->
                "${(jdbcUrl as String).removeSuffix(
                        "/"
                )}/$dbName" + if (useSsl) {
                    "?sslmode=require"
                } else {
                    ""
                }
            }
            return HikariDataSource(HikariConfig(config))
        }
    }

    fun connect(dbName: String): HikariDataSource {
        return connect(dbName, assemblerConfiguration.server.clone() as Properties, assemblerConfiguration.ssl)
    }

    fun updatePermissionsOnAtlas(dbName: String, ipAddress: String, req: List<AclData>) {
        val sqlStatements = mutableSetOf<String>()
        val permissions = req.groupBy { it.action }
        permissions.entries.forEach {
            when (it.key) {
                Action.ADD -> {
                    val command = "GRANT"
                    it.value.forEach {
                        val tableAndColumnNames = getTableAndColumnNames(it)
                        it.acl.aces.forEach {
                            //THIS IS SUPER DUPER WRONG
                            val privileges = if (it.permissions.contains(Permission.WRITE)) {
                                "ALL"
                            } else {
                                "SELECT"
                            }
                            val sql = updatePrivilegesSql(it.principal, command, privileges, tableAndColumnNames.first, tableAndColumnNames.second)
                        }
                    }
                }
                Action.REMOVE -> {
                    val command = "REVOKE"
                    it.value.forEach {
                        val tableAndColumnNames = getTableAndColumnNames(it)
                        it.acl.aces.forEach {
                            //THIS IS SUPER DUPER WRONG
                            val privileges = if (it.permissions.contains(Permission.WRITE)) {
                                "ALL"
                            } else {
                                "SELECT"
                            }
                            val sql = updatePrivilegesSql(it.principal, command, privileges, tableAndColumnNames.first, tableAndColumnNames.second)
                        }
                    }

                }
                Action.SET -> {
                    val revokeCommand = "REVOKE"
                    val grantCommand = "GRANT"
                    it.value.forEach {
                        val tableAndColumnNames = getTableAndColumnNames(it)
                        it.acl.aces.forEach {
                            //THIS IS SUPER DUPER WRONG
                            val privileges = if (it.permissions.contains(Permission.WRITE)) {
                                "ALL"
                            } else {
                                "SELECT"
                            }

                            //first revoke existing permissions then grant specified permissions
                            val revokeSql = updatePrivilegesSql(it.principal, revokeCommand, "ALL", tableAndColumnNames.first, tableAndColumnNames.second)
                            val grantSql = updatePrivilegesSql(it.principal, grantCommand, privileges, tableAndColumnNames.first, tableAndColumnNames.second)
                        }

                    }
                }
                else -> {
                    logger.error("Invalid action ${it.key} specified for request")
                    throw BadRequestException("Invalid ")
                }
            }


        }

        //add/set? what's difference, does set remove permissions?
        //remove
        //request?? IGNORE THIS FOR NOW

        connect(dbName).use {
            getGrantSql(principal, tableName, columnNames)
            //do batches!! look at assemblerconnectionmanager
        }
    }

    private fun getTableAndColumnNames(aclData: AclData): Pair<String, String> {
        val aclKey = aclData.acl.aclKey
        val securableObjectType = securableObjectTypes[aclKey]!!
        val tableName: String
        val columnName: String
        val securableObjectId = aclKey.last()
        if (securableObjectType == SecurableObjectType.OrganizationAtlasColumn) {
            val organizationAtlasColumn = organizationAtlasColumns[securableObjectId]!!
            tableName = organizationAtlasTables[organizationAtlasColumn.tableId]!!.name
            columnName = organizationAtlasColumn.name
        } else {
            val organizationAtlasTable = organizationAtlasTables[securableObjectId]!!
            tableName = organizationAtlasTable.name
            columnName = ""
        }
        //add checks in here for map indexing
        return Pair(tableName, columnName)
    }

    fun createOrganizationAtlasTable(orgId: UUID, table: OrganizationAtlasTable): UUID {
        val principal = Principals.getCurrentUser()
        Principals.ensureUser(principal)

        val tableFQN = FullQualifiedName(orgId.toString(), table.name)
        aclKeyReservations.reserveIdAndValidateType(table, tableFQN::getFullQualifiedNameAsString)
        checkState(organizationAtlasTables.putIfAbsent(table.id, table) == null,
                "OrganizationAtlasColumn ${tableFQN.fullQualifiedNameAsString} already exists")

        val tableAclKey = AclKey(orgId, table.id)
        authorizationManager.setSecurableObjectType(tableAclKey, SecurableObjectType.OrganizationAtlasTable)
        authorizationManager.addPermission(tableAclKey, principal, EnumSet.allOf(Permission::class.java))
        //eventBus?

        return table.id
    }

    fun createOrganizationAtlasColumn(orgId: UUID, column: OrganizationAtlasColumn): UUID {
        val principal = Principals.getCurrentUser()
        Principals.ensureUser(principal)

        checkState(organizationAtlasTables[column.tableId] == null,
                "OrganizationAtlasColumn ${column.name} belongs to a table that does not exist")
        val columnFQN = FullQualifiedName(column.tableId.toString(), column.name)
        aclKeyReservations.reserveIdAndValidateType(column, columnFQN::getFullQualifiedNameAsString)
        checkState(organizationAtlasColumns.putIfAbsent(column.id, column) == null,
                "OrganizationAtlasColumn ${columnFQN.fullQualifiedNameAsString} already exists")

        val columnAclKey = AclKey(orgId, column.tableId, column.id)
        authorizationManager.setSecurableObjectType(columnAclKey, SecurableObjectType.OrganizationAtlasColumn)
        authorizationManager.addPermission(columnAclKey, principal, EnumSet.allOf(Permission::class.java))

        return column.id
    }

    private fun updatePrivilegesSql(principal: Principal, command: String, privileges: String, tableName: String, columnName: String): String {
        val securePrincipal = securePrincipalsManager.getPrincipal(principal.id)
        val dbUser = quote(buildPostgresUsername(securePrincipal))
        return "$command $privileges $columnName ON $tableName TO $dbUser"
    }

}