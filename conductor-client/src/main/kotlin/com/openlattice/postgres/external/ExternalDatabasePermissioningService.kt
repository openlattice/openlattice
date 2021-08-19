package com.openlattice.postgres.external

import com.openlattice.authorization.Acl
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.Action
import com.openlattice.authorization.SecurablePrincipal
import com.openlattice.edm.PropertyTypeIdFqn
import com.openlattice.organization.ExternalColumn
import com.openlattice.organization.ExternalTable
import com.openlattice.organization.roles.Role
import com.openlattice.postgres.TableColumn
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 * @author Drew Bailey (drew@openlattice.com)
 */
interface ExternalDatabasePermissioningService {

    /**
     * Creates [role] in the configured external database
     */
    fun createRole(role: Role)

    /**
     * Creates user [principal] in the configured external database
     */
    fun createUnprivilegedUser(principal: SecurablePrincipal)

    /**
     * Sets privileges a user on an organization's columns
     */
    fun executePrivilegesUpdate(action: Action, acls: List<Acl>)

    /**
     * Adds [sourcePrincipalAclKey] to [targetPrincipalAclKeys]
     */
    fun addPrincipalToPrincipals(sourcePrincipalAclKey: AclKey, targetPrincipalAclKeys: Set<AclKey>)

    /**
     * Removes [principalToRemove] from [fromPrincipals]
     */
    fun removePrincipalsFromPrincipals(principalsToRemove: Set<AclKey>, fromPrincipals: Set<AclKey>)

    /**
     * Initializes permissions on [propertyTypes] for [entitySet] in org database for [organizationId]
     */
    fun initializeAssemblyPermissions(
            entitySetId: UUID,
            entitySetName: String,
            propertyTypes: Set<PropertyTypeIdFqn>
    )

    /**
     * Updates permissions on [propertyTypes] for [entitySet] in org database for [organizationId]
     */
    fun updateAssemblyPermissions(
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<AclKey, TableColumn>
    )

    /**
     * Initializes permissions on [columns] for [table] in org database for [organizationId]
     */
    fun initializeExternalTablePermissions(
            table: ExternalTable,
            columns: Set<ExternalColumn>
    )

    /**
     * Initializes permissions on [columns] for [table] view in collaboration database for [collaborationId]
     */
    fun initializeProjectedTableViewPermissions(
            schema: String,
            table: ExternalTable,
            columns: Set<ExternalColumn>
    )

    /**
     * Updates permissions on [columns] for [table] in org database for [organizationId]
     */
    fun updateExternalTablePermissions(
            action: Action,
            columnAcls: List<Acl>,
            columnsById: Map<AclKey, TableColumn>
    )

    /**
     * Drops permission roles on tables and columns [tablesToColumnIds] in org database for [organizationId]
     */
    fun destroyExternalTablePermissions(
            tablesToColumnIds: Map<UUID, Set<UUID>>
    )
}