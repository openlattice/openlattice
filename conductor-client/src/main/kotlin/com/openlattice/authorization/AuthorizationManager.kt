package com.openlattice.authorization

import com.codahale.metrics.annotation.Timed
import com.google.common.collect.SetMultimap
import com.openlattice.authorization.securable.SecurableObjectType
import java.time.OffsetDateTime
import java.util.*
import java.util.stream.Stream

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
interface AuthorizationManager {

    /**
     * Bulk function for setting or initializing securable object types.
     *
     * @param aclKeys    The acl keys to set to a specific object type.
     * @param objectType The securable object type to be set for the aclKeys
     */
    @Timed
    fun setSecurableObjectTypes(aclKeys: Set<AclKey>, objectType: SecurableObjectType)

    /**
     * Creates an empty acl.
     *
     * @param aclKey     The key for the object whose acl is being created.
     * @param objectType The type of the object for lookup purposes.
     */
    fun setSecurableObjectType(aclKey: AclKey, objectType: SecurableObjectType)

    @Timed
    fun addPermission(
            aclKey: AclKey,
            principal: Principal,
            permissions: EnumSet<Permission>,
            expirationDate: OffsetDateTime = OffsetDateTime.MAX
    )

    @Timed
    fun addPermissions(acls: List<Acl>)

    @Timed
    fun removePermissions(acls: List<Acl>)

    @Timed
    fun setPermissions(acls: List<Acl>)

    // Permissions cleanup on object deletion

    // Permissions cleanup on object deletion
    @Timed
    fun deletePermissions(aclKey: AclKey)

    @Timed
    fun deletePrincipalPermissions(principal: Principal)

    // Auth checks

    // Auth checks
    @Timed
    fun authorize(
            requests: Map<AclKey, EnumSet<Permission>>,
            principals: Set<Principal>
    ): Map<AclKey, EnumMap<Permission, Boolean>>

    @Timed
    fun accessChecksForPrincipals(
            accessChecks: Set<AccessCheck>,
            principals: Set<Principal>
    ): Stream<Authorization>

    @Timed
    fun checkIfHasPermissions(
            aclKeys: AclKey,
            principals: Set<Principal>,
            requiredPermissions: EnumSet<Permission>
    ): Boolean

    // Utility functions for retrieving permissions

    // Utility functions for retrieving permissions
    /**
     * @param aclKeySets the list of groups of AclKeys for wich to get the most restricted set of permissions
     * @param principals the pricipals to check against
     * @return the intersection of permission for each set of aclKeys
     */
    fun getSecurableObjectSetsPermissions(
            aclKeySets: Collection<Set<AclKey>>,
            principals: Set<Principal>
    ): Map<Set<AclKey>, EnumSet<Permission>>

    fun getSecurableObjectPermissions(
            aclKey: AclKey,
            principals: Set<Principal>
    ): Set<Permission>

    fun getAllSecurableObjectPermissions(key: AclKey): Acl

    fun getAllSecurableObjectPermissions(keys: Set<AclKey>): Set<Acl>

    /**
     * Returns all Principals, which have all the specified permissions on the securable object
     *
     * @param key         The securable object
     * @param permissions Set of permission to check for
     */
    fun getAuthorizedPrincipalsOnSecurableObject(key: AclKey, permissions: EnumSet<Permission>): Set<Principal>

    fun getAuthorizedObjectsOfType(
            principal: Principal,
            objectType: SecurableObjectType,
            permissions: EnumSet<Permission>
    ): Stream<AclKey>

    fun getAuthorizedObjectsOfType(
            principal: Set<Principal>,
            objectType: SecurableObjectType,
            permissions: EnumSet<Permission>): Stream<AclKey>

    @Timed
    fun getOwnersForSecurableObjects(aclKeys: Collection<AclKey>): SetMultimap<AclKey, Principal>
}