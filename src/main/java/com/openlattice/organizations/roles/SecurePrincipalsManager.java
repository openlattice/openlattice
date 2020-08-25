/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.organizations.roles;

import com.auth0.json.mgmt.users.User;
import com.hazelcast.query.Predicate;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.organization.roles.Role;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface SecurePrincipalsManager {

    /**
     * @param owner     The owner of a role. Usually the organization.
     * @param principal The principal which to create.
     * @return True if the securable principal was created false otherwise.
     */
    boolean createSecurablePrincipalIfNotExists( Principal owner, SecurablePrincipal principal );

    /**
     * Retrieves a securable principal by acl key lookup.
     *
     * @param aclKey The acl key for the securable principal.
     * @return The securable principal identified by acl key.
     */
    SecurablePrincipal getSecurablePrincipal( AclKey aclKey );

    @Nonnull SecurablePrincipal getPrincipal( String principalId );

    Collection<SecurablePrincipal> getAllRolesInOrganization( UUID organizationId );

    Collection<SecurablePrincipal> getSecurablePrincipals( Predicate<AclKey, SecurablePrincipal> p );

    void createSecurablePrincipal(
            Principal owner, SecurablePrincipal principal );

    void updateTitle( AclKey aclKey, String title );

    void updateDescription( AclKey aclKey, String description );

    void deletePrincipal( AclKey aclKey );

    void deleteAllRolesInOrganization( UUID organizationId );

    void addPrincipalToPrincipal( AclKey source, AclKey target );

    void removePrincipalFromPrincipal( AclKey source, AclKey target );

    void removePrincipalsFromPrincipals( Set<AclKey> sources, Set<AclKey> target );

    Collection<SecurablePrincipal> getAllPrincipalsWithPrincipal( AclKey aclKey );

    Collection<SecurablePrincipal> getParentPrincipalsOfPrincipal( AclKey aclKey );

    boolean principalHasChildPrincipal( AclKey parent, AclKey child );

    // Methods about users
    Collection<Principal> getAllUsersWithPrincipal( AclKey principal );

    Collection<User> getAllUserProfilesWithPrincipal( AclKey principal );

    boolean principalExists( Principal p );

    User getUser( String userId );

    Role getRole( UUID organizationId, UUID roleId );

    AclKey lookup( Principal p );

    Map<Principal, AclKey> lookup( Set<Principal> principals );

    Role lookupRole( Principal principal );

    Collection<SecurablePrincipal> getSecurablePrincipals( Collection<Principal> members );

    Collection<SecurablePrincipal> getAllPrincipals( SecurablePrincipal sp );

    /**
     * Returns all Principals, which have all the specified permissions on the securable object
     * @param key The securable object
     * @param permissions Set of permission to check for
     */
    Set<Principal> getAuthorizedPrincipalsOnSecurableObject( AclKey key, EnumSet<Permission> permissions );

    SecurablePrincipal getSecurablePrincipalById( UUID id );

    UUID getCurrentUserId();
}
