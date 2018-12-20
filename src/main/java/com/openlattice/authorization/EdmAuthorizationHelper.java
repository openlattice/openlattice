

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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.hazelcast.util.Preconditions;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public class EdmAuthorizationHelper implements AuthorizingComponent {
    public static final EnumSet<Permission> WRITE_PERMISSION = EnumSet.of( Permission.WRITE );
    public static final EnumSet<Permission> READ_PERMISSION = EnumSet.of( Permission.READ );

    private final EdmManager           edm;
    private final AuthorizationManager authz;

    public EdmAuthorizationHelper( EdmManager edm, AuthorizationManager authz ) {
        this.edm = Preconditions.checkNotNull( edm );
        this.authz = Preconditions.checkNotNull( authz );
    }

    public Map<UUID, PropertyType> getAuthorizedPropertyTypes(
            UUID entitySetId,
            EnumSet<Permission> requiredPermissions ) {
        return getAuthorizedPropertyTypes( entitySetId,
                requiredPermissions,
                edm.getPropertyTypesForEntitySet( entitySetId ) );
    }

    public Map<UUID, PropertyType> getAuthorizedPropertyTypes(
            UUID entitySetId,
            EnumSet<Permission> requiredPermissions,
            Map<UUID, PropertyType> propertyTypes ) {

        Map<AclKey, EnumSet<Permission>> accessRequest = propertyTypes.keySet().stream()
                .map( ptId -> new AclKey( entitySetId, ptId ) )
                .collect( Collectors.toMap( Function.identity(), aclKey -> requiredPermissions ) );

        Map<AclKey, EnumMap<Permission, Boolean>> authorizations = authorize( accessRequest );
        authorizations.entrySet().stream()
                .filter( authz -> authz.getValue().values().stream().anyMatch( v -> !v ) )
                .forEach( authz -> propertyTypes.remove( authz.getKey().get( 1 ) ) );

        return propertyTypes;
    }

    public Map<UUID, Map<UUID, PropertyType>> getAuthorizedPropertyTypes(Set<UUID> entitySetIds,
                                                                          Set<UUID> selectedProperties,
                                                                         EnumSet<Permission> requiredPermissions) {
        return entitySetIds.stream()
                .collect( Collectors.toMap(
                        it -> it,
                        it -> getAuthorizedPropertyTypes(
                                it,
                                requiredPermissions,
                                edm.getPropertyTypesAsMap( selectedProperties ) ) ) );
    }

    public Set<UUID> getAuthorizedPropertiesOnEntitySet(
            UUID entitySetId,
            EnumSet<Permission> requiredPermissions ) {
        return getAuthorizedPropertiesOnEntitySet(
                entitySetId,
                getAllPropertiesOnEntitySet( entitySetId ),
                requiredPermissions );
    }

    public Map<UUID, Map<UUID, PropertyType>> getAuthorizedPropertiesOnEntitySets(
            Set<UUID> entitySetIds,
            EnumSet<Permission> requiredPermissions ) {
        return getAuthorizedPropertyTypes(
                entitySetIds,
                getAllPropertiesOnEntitySet( entitySetIds.iterator().next() ),
                requiredPermissions );
    }

    public Set<UUID> getAuthorizedPropertiesOnEntitySet(
            UUID entitySetId,
            Set<UUID> selectedProperties,
            EnumSet<Permission> requiredPermissions ) {
        return authz.accessChecksForPrincipals( selectedProperties.stream()
                .map( ptId -> new AccessCheck( new AclKey( entitySetId, ptId ), requiredPermissions ) ).collect(
                        Collectors.toSet() ), Principals.getCurrentPrincipals() )
                .filter( authorization -> authorization.getPermissions().values().stream().allMatch( val -> val ) )
                .map( authorization -> authorization.getAclKey().get( 1 ) ).collect( Collectors.toSet() );
    }

    public Set<PropertyType> getAuthorizedPropertyTypesOnEntitySet(
            UUID entitySetId,
            EnumSet<Permission> requiredPermissions ) {
        return ImmutableSet.copyOf( edm.getPropertyTypes(
                authz.accessChecksForPrincipals( getAllPropertiesOnEntitySet( entitySetId ).stream()
                        .map( ptId -> new AccessCheck( new AclKey( entitySetId, ptId ), requiredPermissions ) )
                        .collect( Collectors.toSet() ), Principals.getCurrentPrincipals() )
                        .filter( authorization ->
                                authorization.getPermissions().values().stream().allMatch( val -> val ) )
                        .map( authorization -> authorization.getAclKey().get( 1 ) )
                        .collect( Collectors.toSet() ) ) );

    }

    /**
     * Get all property types of an entity set
     * @param entitySetId
     * @return all the property type ids on the entity type of the entity set
     */
    public Set<UUID> getAllPropertiesOnEntitySet( UUID entitySetId ) {
        EntitySet es = edm.getEntitySet( entitySetId );
        EntityType et = edm.getEntityType( es.getEntityTypeId() );
        return et.getProperties();
    }

    @Override public AuthorizationManager getAuthorizationManager() {
        return authz;
    }

    public static Map<AclKey, EnumSet<Permission>> aclKeysForAccessCheck(
            UUID entitySetId,
            Set<UUID> propertyTypeIds,
            EnumSet<Permission> requiredPermission ) {
        return propertyTypeIds.stream().map( ptId -> new AclKey( entitySetId, ptId ) )
                .collect( Collectors.toMap( Function.identity(), e -> requiredPermission ) );
    }

    public static Map<AclKey, EnumSet<Permission>> aclKeysForAccessCheck(
            SetMultimap<UUID, UUID> rawAclKeys,
            EnumSet<Permission> requiredPermission ) {
        return rawAclKeys.entries().stream()
                .collect( Collectors.toMap( e -> new AclKey( e.getKey(), e.getValue() ), e -> requiredPermission ) );
    }
}
