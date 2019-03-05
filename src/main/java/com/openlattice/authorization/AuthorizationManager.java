

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

package com.openlattice.authorization;

import com.codahale.metrics.annotation.Timed;
import com.openlattice.authorization.paging.AuthorizedObjectsSearchResult;
import com.openlattice.authorization.securable.SecurableObjectType;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Stream;

/**
 * The authorization manager manages permissions for all securable objects in the system.
 * <p>
 * Authorization behavior is summarized below:
 * <ul>
 * <li>No inheritance and that all permissions are explicitly set.</li>
 * <li>For permissions that are present we follow a least restrictive model for determining access</li>
 * <li>If no relevant permissions are present for Principal set, access is denied.</li>
 * </ul>
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public interface AuthorizationManager {

    /**
     * Creates an empty acl.
     *
     * @param aclKey     The key for the object whose acl is being created.
     * @param objectType The type of the object for lookup purposes.
     */
    void setSecurableObjectType( AclKey aclKey, SecurableObjectType objectType );

    @Timed
    void addPermission(
            AclKey aclKeys,
            Principal principal,
            EnumSet<Permission> permissions );

    @Timed
    void addPermission(
            AclKey aclKeys,
            Principal principal,
            EnumSet<Permission> permissions,
            OffsetDateTime expirationDate );

    @Timed
    void removePermission(
            AclKey aclKeys,
            Principal principal,
            EnumSet<Permission> permissions );

    @Timed
    void setPermission(
            AclKey aclKeys,
            Principal principal,
            EnumSet<Permission> permissions );

    @Timed
    void setPermission(
            AclKey aclKeys,
            Principal principal,
            EnumSet<Permission> permissions,
            OffsetDateTime expirationDate );

    @Timed
    void setPermission( Set<AclKey> aclKeys, Set<Principal> principals, EnumSet<Permission> permissions );

    @Timed
    void deletePermissions( AclKey aclKey );

    @Timed
    void deletePrincipalPermissions( Principal principal );

    @Timed Map<AclKey, EnumMap<Permission, Boolean>> maybeFastAccessChecksForPrincipals(
            Set<AccessCheck> accessChecks,
            Set<Principal> principals );

    @Timed Map<AclKey, EnumMap<Permission, Boolean>> authorize(
            Map<AclKey, EnumSet<Permission>> requests,
            Set<Principal> principals );

    @Timed
    Stream<Authorization> accessChecksForPrincipals(
            Set<AccessCheck> accessChecks,
            Set<Principal> principals );

    @Timed
    boolean checkIfHasPermissions(
            AclKey aclKeys,
            Set<Principal> principals,
            EnumSet<Permission> requiredPermissions );

    boolean checkIfUserIsOwner( AclKey aclkeys, Principal principal );
    // Utility functions for retrieving permissions

    Set<Permission> getSecurableObjectPermissions(
            AclKey aclKeys,
            Set<Principal> principals );

    Acl getAllSecurableObjectPermissions( AclKey key );

    /**
     * Returns all Principals, which have all the specified permissions on the securable object
     * @param key The securable object
     * @param permissions Set of permission to check for
     */
    Set<Principal> getAuthorizedPrincipalsOnSecurableObject( AclKey key, EnumSet<Permission> permissions );

    Stream<AclKey> getAuthorizedObjectsOfType(
            Principal principal,
            SecurableObjectType objectType,
            EnumSet<Permission> permissions );

    Stream<AclKey> getAuthorizedObjectsOfType(
            Set<Principal> principal,
            SecurableObjectType objectType,
            EnumSet<Permission> permissions );

    AuthorizedObjectsSearchResult getAuthorizedObjectsOfType(
            NavigableSet<Principal> principals,
            SecurableObjectType objectType,
            Permission permission,
            String offset,
            int pageSize );

    Stream<AclKey> getAuthorizedObjects( Principal principal, EnumSet<Permission> permissions );

    Stream<AclKey> getAuthorizedObjects( Set<Principal> principal, EnumSet<Permission> permissions );

    Iterable<Principal> getSecurableObjectOwners( AclKey key );

    Map<AceKey, AceValue> getPermissionMap( Set<AclKey> aclKeys, Set<Principal> principals );
}
