package com.openlattice.external

import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.external.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface ExternalSqlDatabaseManager {
    fun getDriverName() : String
    //These aren't paged for now, since even if warehouse has a 1M tables with each entry taking 1kb to describe it would
    //only be 1 GB of metadata from API call.
    fun getTables(): Map<String, TableMetadata>
    fun getSchemas(): Map<String, SchemaMetadata>
    fun getViews(): Map<String, ViewMetadata>

    fun grantPrivilegeOnSchemaToRole(privilege: SchemaPrivilege, role: Role)
    fun grantPrivilegeOnTableToRole(privilege: TablePrivilege, role: Role)
    fun grantPrivilegeOnViewToRole(privilege: TablePrivilege, role: Role)
    fun grantPrivilegeOnDatabaseToRole(privilege: DatabasePrivilege, role: Role)
    fun grantRoleToRole(roleToGrant: Role, target: Role)

    fun createSchema(schema: SchemaMetadata, owner: Role)
    fun createTable(table: TableMetadata, owner: Role)
    fun createView(view: ViewMetadata, owner: Role)

    fun isDataMaskingNativelySupported(): Boolean
    /**
     * These polices are applied by calling alter table in SQL server and snowflake
     * Mariadb requires configuring max scale, we will need to refine how to express these
     * more flexibly... i.e a standard set of templates and then allow for custom entry across
     * registered database types.
     */
    //These function only work for native policies.
    fun createDataMaskingPolicy(maskingPolicy: String, schema: String)
    fun applyDataMaskingPolicy(table: TableMetadata)

    //Only usable if JdbcConnection.ROLE_MANAGER = true, i.e isRoleManagementEnabled = true
    fun isRoleManagementEnabled(): Boolean
    fun createUser(user: SecurablePrincipal)
    fun createRole(role: SecurablePrincipal)

    //TODO: We should probably use aclKey or principal
    fun deleteUser(user: SecurablePrincipal)
    fun deleteRole(role: SecurablePrincipal)
}
