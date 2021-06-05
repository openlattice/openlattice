

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
import com.google.common.collect.SetMultimap;
import com.hazelcast.query.Predicate;
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
     * Bulk function for setting or initializing securable object types.
     *
     * @param aclKeys    The acl keys to set to a specific object type.
     * @param objectType The securable object type to be set for the aclKeys
     */
    @Timed
    void setSecurableObjectTypes( Set<AclKey> aclKeys, SecurableObjectType objectType );

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
            Set<Permission> permissions,
            OffsetDateTime expirationDate );

    /**
     * Method for bulk adding permissions to a single principal across multiple acl keys of the same type.
     *
     * @param keys                The acl keys to which permissions will be added.
     * @param principal           The principal who will be receiving permissions.
     * @param permissions         The permissions that will be added.
     * @param securableObjectType The securable object type for which the permissions are being added. This will
     *                            override the existing object type, so care must be taken to call this for keys of the right type.
     */
    @Timed
    void addPermissions(
            Set<AclKey> keys,
            Principal principal,
            EnumSet<Permission> permissions,
            SecurableObjectType securableObjectType );

    /**
     * Method for bulk adding permissions to a single principal across multiple acl keys of the same type.
     *
     * @param keys                The acl keys to which permissions will be added.
     * @param principal           The principal who will be receiving permissions.
     * @param permissions         The permissions that will be added.
     * @param securableObjectType The securable object type for which the permissions are being added. This will
     *                            override the existing object type, so care must be taken to call this for keys of the right type.
     * @param expirationDate      The expiration data for the permission changes.
     */
    @Timed
    void addPermissions(
            Set<AclKey> keys,
            Principal principal,
            EnumSet<Permission> permissions,
            SecurableObjectType securableObjectType,
            OffsetDateTime expirationDate );

    @Timed
    void addPermissions( List<Acl> acls );

    @Timed
    void removePermissions( List<Acl> acls );

    @Timed
    void setPermissions( List<Acl> acls );

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
    void setPermissions( Map<AceKey, EnumSet<Permission>> permissions );

    @Timed
    void deletePermissions( AclKey aclKey );

    @Timed
    void deletePrincipalPermissions( Principal principal );

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

    // Utility functions for retrieving permissions

    /**
     * @param aclKeySets the list of groups of AclKeys for wich to get the most restricted set of permissions
     * @param principals the pricipals to check against
     * @return the intersection of permission for each set of aclKeys
     */
    Map<Set<AclKey>, EnumSet<Permission>> getSecurableObjectSetsPermissions(
            Collection<Set<AclKey>> aclKeySets,
            Set<Principal> principals );

    Set<Permission> getSecurableObjectPermissions(
            AclKey aclKey,
            Set<Principal> principals );

    Acl getAllSecurableObjectPermissions( AclKey key );

    Set<Acl> getAllSecurableObjectPermissions( Set<AclKey> keys );

    /**
     * Returns all Principals, which have all the specified permissions on the securable object
     *
     * @param key         The securable object
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

    @Timed Stream<AclKey> getAuthorizedObjectsOfType(
            Set<Principal> principals,
            SecurableObjectType objectType,
            EnumSet<Permission> permissions,
            Predicate additionalFilter );

    Set<Principal> getSecurableObjectOwners( AclKey key );

    @Timed
    SetMultimap<AclKey, Principal> getOwnersForSecurableObjects( Collection<AclKey> aclKeys );
}
