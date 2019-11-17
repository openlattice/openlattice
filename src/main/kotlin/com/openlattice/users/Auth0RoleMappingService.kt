package com.openlattice.users

import com.auth0.json.mgmt.users.User
import com.openlattice.authorization.AclKey
import com.openlattice.authorization.SystemRole
import com.openlattice.authorization.initializers.AuthorizationInitializationTask
import com.openlattice.organizations.roles.SecurePrincipalsManager

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class Auth0RoleMappingService ( val spm: SecurePrincipalsManager) {

    private val userRoleAclKey: AclKey = spm.lookup(AuthorizationInitializationTask.GLOBAL_USER_ROLE.principal)
    private val adminRoleAclKey: AclKey = spm.lookup(AuthorizationInitializationTask.GLOBAL_ADMIN_ROLE.principal)

    private fun mapRoles(user: User, spm: SecurePrincipalsManager, roles: Set<String> ) {
        roles.map {role ->
            when( role ) {
                SystemRole.ADMIN.name -> adminRoleAclKey
                SystemRole.USER.name  -> userRoleAclKey
                SystemRole.AUTHENTICATED_USER.name -> userRoleAclKey
                else -> userRoleAclKey //spm.getAllRolesInOrganization()
            }
        }
    }

}