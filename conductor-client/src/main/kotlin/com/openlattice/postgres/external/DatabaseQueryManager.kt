package com.openlattice.postgres.external

import com.openlattice.authorization.Principal
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.PropertyType
import com.openlattice.organization.roles.Role
import com.openlattice.organizations.OrganizationDatabase
import com.zaxxer.hikari.HikariDataSource
import java.util.*

interface DatabaseQueryManager {

    /**
     * Creates a private organization database that can be used for uploading data using launchpad.
     * Also sets up foreign data wrapper using assembler in assembler so that materialized views of data can be
     * provided.
     */
    fun createAndInitializeOrganizationDatabase(organizationId: UUID): OrganizationDatabase

    fun createAndInitializeCollaborationDatabase(collaborationId: UUID): OrganizationDatabase

    fun addMembersToCollaboration(collaborationId: UUID, memberRoles: Collection<String>)

    fun collaborationMemberGrantSql(dbName: String, memberRoles: Iterable<String>): String

    fun removeMembersFromCollaboration(collaborationId: UUID, memberRoles: Collection<String>)

    fun collaborationMemberRevokeSql(dbName: String, memberRoles: Iterable<String>): String

    fun createAndInitializeSchemas(collaborationId: UUID, schemaNameToAuthorizedPgRoles: Map<String, Collection<String>>)

    fun addMembersToCollabInSchema(collaborationId: UUID, schemaName: String, members: Collection<String>)

    fun removeMembersFromSchemaInCollab(collaborationId: UUID, schemaName: String, members: Collection<String>)

    fun removeMembersFromDatabaseInCollab(collaborationId: UUID, members: Collection<String>)

    fun renameSchema(collaborationId: UUID, oldName: String, newName: String)

    fun dropSchemas(collaborationId: UUID, schemasToDrop: Iterable<String>)

    fun addMembersToOrganization(organizationId: UUID, dataSource: HikariDataSource, members: Set<Principal>)

    fun addMembersToOrganization(
            organizationId: UUID,
            authorizedPropertyTypesOfEntitySetsByPrincipal: Map<SecurablePrincipal, Map<EntitySet, Collection<PropertyType>>>
    )

    fun addMembersToOrganization(
            organizationId: UUID,
            dataSource: HikariDataSource,
            authorizedPropertyTypesOfEntitySetsByPrincipal: Map<SecurablePrincipal, Map<EntitySet, Collection<PropertyType>>>
    )

    fun removeMembersFromOrganization(
            organizationId: UUID,
            principals: Collection<SecurablePrincipal>
    )

    fun updateCredentialInDatabase(unquotedUserId: String, credential: String)

    fun createDatabase(dbName: String)

    fun dropOrganizationDatabase(organizationId: UUID)

    fun dropDatabase(dbName: String)

    fun getAllRoles(): Set<Role>

    fun getAllUsers(): Set<SecurablePrincipal>

    fun dropUserIfExists(user: SecurablePrincipal)

    fun renameDatabase(currentDatabaseName: String, newDatabaseName: String)

    fun getDatabaseOid(dbName: String): Int

    fun createRenameDatabaseFunctionIfNotExists()

}