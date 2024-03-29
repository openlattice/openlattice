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
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.organization.roles.Role;

import javax.annotation.Nonnull;
import java.util.Collection;
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

    Collection<SecurablePrincipal> getAllRolesInOrganization( UUID organizationId );

    Map<UUID, Collection<SecurablePrincipal>> getAllRolesInOrganizations( Collection<UUID> organizationIds );

    Collection<SecurablePrincipal> getSecurablePrincipals( Predicate<AclKey, SecurablePrincipal> p );

    void updateTitle( AclKey aclKey, String title );

    void updateDescription( AclKey aclKey, String description );

    void deletePrincipal( AclKey aclKey );

    void deleteAllRolesInOrganization( UUID organizationId );

    void addPrincipalToPrincipal( AclKey source, AclKey target );

    /**
     * Grants an AclKey to a set of AclKeys, and returns any that were updated.
     *
     * @param source  The child AclKey to grant
     * @param targets The parent AclKeys that will be granted [source]
     * @return all AclKeys that were updated. Any target AclKey that already had [source] as a child will not be included.
     */
    Set<AclKey> addPrincipalToPrincipals( AclKey source, Set<AclKey> targets );

    void removePrincipalFromPrincipal( AclKey source, AclKey target );

    void removePrincipalsFromPrincipals( Set<AclKey> sources, Set<AclKey> target );

    /**
     * Reads
     */

    @Nonnull SecurablePrincipal getSecurablePrincipal( String principalId );

    Map<AclKey, SecurablePrincipal> getSecurablePrincipals( Set<AclKey> aclKeys );

    Collection<SecurablePrincipal> getParentPrincipalsOfPrincipal( AclKey aclKey );

    Map<UUID, Set<SecurablePrincipal>> getOrganizationMembers( Set<UUID> organizationIds );

    Set<Principal> getOrganizationMemberPrincipals( UUID organizationId );

    boolean principalHasChildPrincipal( AclKey parent, AclKey child );

    Collection<SecurablePrincipal> getAllUsersWithPrincipal( AclKey aclKey );

    // Methods about users
    Collection<User> getAllUserProfilesWithPrincipal( AclKey principal );

    boolean principalExists( Principal p );

    User getUser( String userId );

    Role getRole( UUID organizationId, UUID roleId );

    AclKey lookup( Principal p );

    Map<Principal, AclKey> lookup( Set<Principal> principals );

    Role lookupRole( Principal principal );

    Collection<SecurablePrincipal> getSecurablePrincipals( Collection<Principal> members );

    Collection<SecurablePrincipal> getAllPrincipals( SecurablePrincipal sp );

    Map<SecurablePrincipal, Set<Principal>> bulkGetUnderlyingPrincipals( Set<SecurablePrincipal> sps );

    UUID getCurrentUserId();

    void ensurePrincipalsExist( Set<AclKey> aclKeys );

    Set<Role> getAllRoles();

    Set<SecurablePrincipal> getAllUsers();

}
