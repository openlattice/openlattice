package com.openlattice.external

import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.JdbcConnection
import com.openlattice.organizations.external.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresExternalSqlDatabaseManager(jdbcConnection: JdbcConnection): ExternalSqlDatabaseManager {
    override fun getDriverName(): String {
        TODO("Not yet implemented")
    }

    override fun getTables(): Map<TableKey, TableMetadata> {
        TODO("Not yet implemented")
    }

    override fun getSchemas(): Map<String, SchemaMetadata> {
        TODO("Not yet implemented")
    }

    override fun getViews(): Map<String, ViewMetadata> {
        TODO("Not yet implemented")
    }

    override fun grantPrivilegeOnSchemaToRole(privilege: SchemaPrivilege, role: Role) {
        TODO("Not yet implemented")
    }

    override fun grantPrivilegeOnTableToRole(privilege: TablePrivilege, role: Role) {
        TODO("Not yet implemented")
    }

    override fun grantPrivilegeOnViewToRole(privilege: TablePrivilege, role: Role) {
        TODO("Not yet implemented")
    }

    override fun grantPrivilegeOnDatabaseToRole(privilege: DatabasePrivilege, role: Role) {
        TODO("Not yet implemented")
    }

    override fun grantRoleToRole(roleToGrant: Role, target: Role) {
        TODO("Not yet implemented")
    }

    override fun createSchema(schema: SchemaMetadata, owner: Role) {
        TODO("Not yet implemented")
    }

    override fun createTable(table: TableMetadata, owner: Role) {
        TODO("Not yet implemented")
    }

    override fun createView(view: ViewMetadata, owner: Role) {
        TODO("Not yet implemented")
    }

    override fun isDataMaskingNativelySupported(): Boolean {
        TODO("Not yet implemented")
    }

    override fun createDataMaskingPolicy(maskingPolicy: String, schema: String) {
        TODO("Not yet implemented")
    }

    override fun applyDataMaskingPolicy(table: TableMetadata) {
        TODO("Not yet implemented")
    }

    override fun isRoleManagementEnabled(): Boolean {
        TODO("Not yet implemented")
    }

    override fun createUser(user: SecurablePrincipal) {
        TODO("Not yet implemented")
    }

    override fun createRole(role: SecurablePrincipal) {
        TODO("Not yet implemented")
    }

    override fun deleteUser(user: SecurablePrincipal) {
        TODO("Not yet implemented")
    }

    override fun deleteRole(role: SecurablePrincipal) {
        TODO("Not yet implemented")
    }
}