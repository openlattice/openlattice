package com.openlattice.organizations

import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.PostgresRoles.Companion.buildAtlasPostgresUsername
import com.openlattice.authorization.*
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.organization.OrganizationAtlasColumn
import com.openlattice.organization.OrganizationAtlasTable
import com.openlattice.organizations.roles.SecurePrincipalsManager

import com.openlattice.postgres.DataTables.quote
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.util.*

class AtlasDataService(
        private val hazelcastInstance: HazelcastInstance,
        private val hds: HikariDataSource,
        private val assemblerConfiguration: AssemblerConfiguration, //for now using this, may need to make a separate one
        private val securePrincipalsManager: SecurePrincipalsManager,
        private val aclKeyReservations: HazelcastAclKeyReservationService
        ) {

    private val organizationAtlasColumns: IMap<UUID, OrganizationAtlasColumn> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_ATLAS_COlUMN.name)
    private val organizationAtlasTables: IMap<UUID, OrganizationAtlasTable> = hazelcastInstance.getMap(HazelcastMap.ORGANIZATION_ATLAS_TABLE.name)
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

//    fun updatePermissionsOnAtlas(dbName: String, ipAddress: String, req: List<AclData>) {
//        val permissions = req.map{it.action to mapOf(it.acl.aclKey to it.acl.aces)}.toMap()
//        permissions.entries.forEach {
//            when ( it.key ) {
//                Action.ADD -> {
//                    it.value.forEach {
//                        //get securable object from AclKey, know type from length of aclkey? ahhhhhhhhh not gooooood
//                        it.value.forEach{
//                            val sql = getGrantSql(it.principal,
//                        }
//                    }
//                }
//        }
//
//
//        }
//
//        //add/set? what's difference, does set remove permissions?
//        //remove
//        //request??
//
//        connect(dbName).use {
//            getGrantSql(principal, tableName, columnNames)
//        }
//    }

    fun createOrganizationAtlasTable( table: OrganizationAtlasTable ) : UUID {
        aclKeyReservations.reserveIdAndValidateType(table, table::name)
        checkState(organizationAtlasTables.putIfAbsent(table.id, table) == null,
                "OrganizationAtlasColumn ${table.name} already exists")
    }

    fun createOrganizationAtlasColumn(column: OrganizationAtlasColumn) : UUID {
        var table = organizationAtlasTables[column.tableId]
        checkState(table == null,
                "OrganizationAtlasColumn ${column.name} belongs to a table that does not exist")
        val uniqueColumnName = "${table!!.name}_${column.name}"
        aclKeyReservations.reserveIdAndValidateType(column, {uniqueColumnName}) //TODO ask if this is dumb
        checkState(organizationAtlasColumns.putIfAbsent(column.id, column) == null,
                "OrganizationAtlasColumn $uniqueColumnName already exists")

    }

    private fun getGrantSql(principal: Principal, tableName: String, columnNames: Optional<Set<String>>): String {
        val securePrincipal = securePrincipalsManager.getPrincipal(principal.id)
        val dbUser = quote(buildAtlasPostgresUsername(securePrincipal))

        val columnsList = if (columnNames.isPresent) {
            "( ${columnNames.get().joinToString(",") { it }} )"
        } else { "" }

        return "GRANT SELECT $columnsList ON $tableName TO $dbUser"
    }

}