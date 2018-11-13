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

import com.openlattice.authorization.*;
import com.openlattice.directory.pojo.Auth0UserBasic;
import com.openlattice.organization.roles.Role;
import com.google.common.collect.SetMultimap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.query.Predicate;

import java.util.*;

public interface SecurePrincipalsManager {

    /**
     * @param owner     The owner of a role. Usually the organization.
     * @param principal The principal which to create.
     * @return True if the securable principal was created false otherwise.
     */
    boolean createSecurablePrincipalIfNotExists( Principal owner, SecurablePrincipal principal );

    //    SecurablePrincipal getSecurablePrincipal( Principal principal );

    /**
     * Retrieves a securable principal by acl key lookup.
     *
     * @param aclKey The acl key for the securable principal.
     * @return The securable principal identified by acl key.
     */
    SecurablePrincipal getSecurablePrincipal( AclKey aclKey );

    SecurablePrincipal getPrincipal( String principalId );

    Optional<SecurablePrincipal> maybeGetSecurablePrincipal( Principal p );

    Collection<SecurablePrincipal> getSecurablePrincipals( PrincipalType principalType );

    Collection<SecurablePrincipal> getAllRolesInOrganization( UUID organizationId );

    SetMultimap<SecurablePrincipal, SecurablePrincipal> getRolesForUsersInOrganization( UUID organizationId );

    Collection<SecurablePrincipal> getSecurablePrincipals( Predicate p );

    void createSecurablePrincipal(
            Principal owner, SecurablePrincipal principal );

    void updateTitle( AclKey aclKey, String title );

    void updateDescription( AclKey aclKey, String description );

    void deletePrincipal( AclKey aclKey );

    void deleteAllRolesInOrganization( UUID organizationId );

    void addPrincipalToPrincipal( AclKey source, AclKey target );

    void removePrincipalFromPrincipal( AclKey source, AclKey target );

    Map<AclKey, Object> executeOnPrincipal( EntryProcessor<AclKey, SecurablePrincipal> ep, Predicate p );

    //More logical to use Principal

    void removePrincipalFromPrincipals( AclKey source, Predicate targetFilter );

    Collection<SecurablePrincipal> getAllPrincipalsWithPrincipal( AclKey aclKey );

    Collection<SecurablePrincipal> getParentPrincipalsOfPrincipal( AclKey aclKey );

    boolean principalHasChildPrincipal( AclKey parent, AclKey child );

    // Methods about users
    Collection<Principal> getAllUsersWithPrincipal( AclKey principal );

    Collection<Auth0UserBasic> getAllUserProfilesWithPrincipal( AclKey principal );

    boolean principalExists( Principal p );

    Auth0UserBasic getUser( String userId );

    Role getRole( UUID organizationId, UUID roleId );

    AclKey lookup( Principal p );

    Collection<Principal> getPrincipals( Predicate<AclKey, SecurablePrincipal> p );

    Collection<SecurablePrincipal> getSecurablePrincipals( Set<Principal> members );

    Collection<SecurablePrincipal> getAllPrincipals( SecurablePrincipal sp );
}
