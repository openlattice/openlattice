package com.openlattice.external

import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.external.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
interface ExternalSqlDatabaseManager {
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

    fun createSchema( schema: SchemaMetadata, owner:Role )
    fun createTable( table:TableMetadata, owner:Role)
    fun createView( view: ViewMetadata, owner: Role)

    //Only usable if JdbcConnection.ROLE_MANAGER = true
    fun createUser(user:SecurablePrincipal)
    fun createRole(role:SecurablePrincipal)

    //TODO: We should probably use aclKey or principal
    fun deleteUser(user:SecurablePrincipal)
    fun deleteRole(role:SecurablePrincipal)
}
