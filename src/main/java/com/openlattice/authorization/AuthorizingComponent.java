

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

import static com.openlattice.authorization.EdmAuthorizationHelper.READ_PERMISSION;
import static com.openlattice.authorization.EdmAuthorizationHelper.WRITE_PERMISSION;

import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.controllers.exceptions.ForbiddenException;
import com.openlattice.edm.type.PropertyType;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface AuthorizingComponent {
    Logger logger = LoggerFactory.getLogger( AuthorizingComponent.class );

    AuthorizationManager getAuthorizationManager();

    default <T extends AbstractSecurableObject> Predicate<T> isAuthorizedObject(
            Permission requiredPermission,
            Permission... requiredPermissions ) {
        return abs -> isAuthorized( requiredPermission, requiredPermissions )
                .test( new AclKey( abs.getId() ) );
    }

    default Predicate<AclKey> isAuthorized(
            Permission requiredPermission,
            Permission... requiredPermissions ) {
        return isAuthorized( EnumSet.of( requiredPermission, requiredPermissions ) );
    }

    default Predicate<AclKey> isAuthorized( EnumSet<Permission> requiredPermissions ) {
        return aclKey -> getAuthorizationManager().checkIfHasPermissions( aclKey,
                Principals.getCurrentPrincipals(),
                requiredPermissions );
    }

    default boolean owns( AclKey aclKey ) {
        return isAuthorized( Permission.OWNER ).test( new AclKey( aclKey ) );
    }

    default void ensureReadAccess( AclKey aclKey ) {
        accessCheck( aclKey, READ_PERMISSION );
    }

    default void ensureWriteAccess( AclKey aclKey ) {
        accessCheck( aclKey, WRITE_PERMISSION );
    }

    default void ensureOwnerAccess( AclKey aclKey ) {
        accessCheck( aclKey, EnumSet.of( Permission.OWNER ) );
    }

    default void ensureLinkAccess( AclKey aclKey ) {
        accessCheck( aclKey, EnumSet.of( Permission.LINK ) );
    }

    default void ensureAdminAccess() {
        if ( !Principals.getCurrentPrincipals().contains( Principals.getAdminRole() ) ) {
            throw new ForbiddenException( "Only admins are allowed to perform this action." );
        }
    }

    default Map<AclKey, EnumMap<Permission, Boolean>> authorize(
            Map<AclKey, EnumSet<Permission>> requiredPermissionsByAclKey ) {
        return authorize( requiredPermissionsByAclKey, Principals.getCurrentPrincipals() );
    }

    default Map<AclKey, EnumMap<Permission, Boolean>> authorize(
            Map<AclKey, EnumSet<Permission>> requiredPermissionsByAclKey,
            Set<Principal> principals ) {
        return getAuthorizationManager().authorize( requiredPermissionsByAclKey, principals );
    }

    default void accessCheck( Map<UUID, PropertyType> authorizedPropertyTypes, Set<UUID> requiredPropertyTypes ) {
        final boolean authorized = authorizedPropertyTypes.keySet().containsAll( requiredPropertyTypes );
        if ( !authorized ) {
            logger.warn( "Authorization failed. Required {} but only found {}.",
                    requiredPropertyTypes,
                    authorizedPropertyTypes.keySet() );
            throw new ForbiddenException( "Insufficient permissions to perform operation." );
        }

    }

    default void accessCheck( Map<AclKey, EnumSet<Permission>> requiredPermissionsByAclKey ) {
        final boolean authorized = getAuthorizationManager()
                .authorize( requiredPermissionsByAclKey, Principals.getCurrentPrincipals() )
                .values().stream().allMatch( m -> m.values().stream().allMatch( v -> v ) );

        if ( !authorized ) {
            logger.warn( "Authorization failed for required permissions: {}", requiredPermissionsByAclKey );
            throw new ForbiddenException( "Insufficient permissions to perform operation." );
        }
    }

    default void accessCheck( AclKey aclKey, EnumSet<Permission> requiredPermissions ) {
        if ( !getAuthorizationManager().checkIfHasPermissions(
                aclKey,
                Principals.getCurrentPrincipals(),
                requiredPermissions ) ) {
            throw new ForbiddenException( "Object " + aclKey.toString() + " is not accessible." );
        }
    }

    default Stream<AclKey> getAccessibleObjects(
            SecurableObjectType securableObjectType,
            EnumSet<Permission> requiredPermissions ) {
        return getAuthorizationManager().getAuthorizedObjectsOfType(
                Principals.getCurrentPrincipals(),
                securableObjectType,
                requiredPermissions );
    }
}
