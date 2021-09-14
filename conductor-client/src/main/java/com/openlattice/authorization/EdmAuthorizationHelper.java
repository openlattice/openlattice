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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.EntitySetManager;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class EdmAuthorizationHelper implements AuthorizingComponent {
    public static final EnumSet<Permission>  TRANSPORT_PERMISSION = EnumSet.of( Permission.MATERIALIZE );
    public static final EnumSet<Permission>  READ_PERMISSION      = EnumSet.of( Permission.READ );
    public static final EnumSet<Permission>  WRITE_PERMISSION = EnumSet.of( Permission.WRITE );
    public static final EnumSet<Permission>  OWNER_PERMISSION = EnumSet.of( Permission.OWNER );
    private final       EdmManager           edm;
    private final       AuthorizationManager authz;
    private final       EntitySetManager     entitySetManager;

    public EdmAuthorizationHelper( EdmManager edm, AuthorizationManager authz, EntitySetManager entitySetManager ) {
        this.edm = edm;
        this.authz = authz;
        this.entitySetManager = entitySetManager;
    }

    @Timed
    public Map<UUID, PropertyType> getAuthorizedPropertyTypes(
            UUID entitySetId,
            EnumSet<Permission> requiredPermissions ) {
        return getAuthorizedPropertyTypes( entitySetId, requiredPermissions, Principals.getCurrentPrincipals() );
    }

    @Timed
    public Map<UUID, PropertyType> getAuthorizedPropertyTypes(
            UUID entitySetId,
            EnumSet<Permission> requiredPermissions,
            Set<Principal> principals ) {
        final var propertyTypes = entitySetManager.getPropertyTypesForEntitySet( entitySetId );

        return getAuthorizedPropertyTypes( entitySetId, requiredPermissions, propertyTypes, principals );
    }

    @Timed
    public Map<UUID, PropertyType> getAuthorizedPropertyTypes(
            UUID entitySetId,
            EnumSet<Permission> requiredPermissions,
            Map<UUID, PropertyType> propertyTypes,
            Set<Principal> principals ) {

        final var entitySet = entitySetManager.getEntitySet( entitySetId );

        return ( entitySet.isLinking() )
                ? getAuthorizedPropertyTypesOfLinkingEntitySet(
                entitySet, propertyTypes.keySet(), requiredPermissions, principals )
                : getAuthorizedPropertyTypesOfNormalEntitySet(
                entitySetId, propertyTypes, requiredPermissions, principals );
    }

    /**
     * @return Authorized property types for the requested permissions where at least 1 requested principal has been
     * authorization for.
     */
    @VisibleForTesting
    public Map<UUID, PropertyType> getAuthorizedPropertyTypesOfNormalEntitySet(
            UUID entitySetId,
            Map<UUID, PropertyType> propertyTypes,
            EnumSet<Permission> requiredPermissions,
            Set<Principal> principals ) {

        Map<AclKey, EnumSet<Permission>> accessRequest = propertyTypes.keySet().stream()
                .map( ptId -> new AclKey( entitySetId, ptId ) )
                .collect( Collectors.toMap( Function.identity(), aclKey -> requiredPermissions ) );

        Map<AclKey, EnumMap<Permission, Boolean>> authorizations = authorize( accessRequest, principals );
        return authorizations.entrySet().stream()
                .filter( authz -> authz.getValue().values().stream().allMatch( v -> v ) )
                .map( authz -> authz.getKey().get( 1 ) )
                .collect( Collectors.toMap( Function.identity(), propertyTypes::get ) );
    }

    private Map<UUID, PropertyType> getAuthorizedPropertyTypesOfLinkingEntitySet(
            EntitySet linkingEntitySet,
            Set<UUID> propertyTypeIds,
            EnumSet<Permission> requiredPermissions,
            Set<Principal> principals ) {
        if ( linkingEntitySet.getLinkedEntitySets().isEmpty() ) {
            return Maps.newHashMap();
        }

        final var propertyPermissions = getPermissionsOnLinkingEntitySetProperties(
                linkingEntitySet.getLinkedEntitySets(), propertyTypeIds, principals );

        return propertyPermissions.entrySet().stream()
                .filter( entry -> entry.getValue().containsAll( requiredPermissions ) )
                .collect( Collectors.toMap(
                        entry -> entry.getKey().getId(),
                        Map.Entry::getKey
                ) );
    }

    /**
     * Note: entitysets are assumed to have same entity type
     */
    @Timed
    public @NotNull Map<UUID, Map<UUID, PropertyType>> getAuthorizedPropertyTypes(
            Set<UUID> entitySetIds,
            Set<UUID> selectedProperties,
            EnumSet<Permission> requiredPermissions ) {
        return getAuthorizedPropertyTypes(
                entitySetIds, selectedProperties, requiredPermissions, Principals.getCurrentPrincipals() );
    }

    /**
     * Note: entitysets are assumed to have same entity type
     */
    public @NotNull Map<UUID, Map<UUID, PropertyType>> getAuthorizedPropertyTypes(
            Set<UUID> entitySetIds,
            Set<UUID> selectedProperties,
            EnumSet<Permission> requiredPermissions,
            Set<Principal> principals ) {
        return entitySetIds.stream()
                .collect( Collectors.toMap(
                        it -> it,
                        it -> getAuthorizedPropertyTypes(
                                it,
                                requiredPermissions,
                                edm.getPropertyTypesAsMap( selectedProperties ),
                                principals ) ) );
    }

    /**
     * @see EdmAuthorizationHelper#getAuthorizedPropertiesByNormalEntitySets(EntitySet, EnumSet, Set)
     */
    @Timed
    public Map<UUID, Map<UUID, PropertyType>> getAuthorizedPropertiesByNormalEntitySets(
            EntitySet linkingEntitySet,
            EnumSet<Permission> requiredPermissions ) {
        return getAuthorizedPropertiesByNormalEntitySets(
                linkingEntitySet, requiredPermissions, Principals.getCurrentPrincipals() );
    }

    /**
     * Returns authorized property types for a linking entity set, which is the intersection of authorized properties on
     * each its normal entity sets.
     *
     * @param linkingEntitySet    the linking entity set to check for
     * @param requiredPermissions the permissions to check for
     * @param principals          the principals to check against
     * @return Map of authorized property types by normal entity sets
     */
    @Timed
    public Map<UUID, Map<UUID, PropertyType>> getAuthorizedPropertiesByNormalEntitySets(
            EntitySet linkingEntitySet,
            EnumSet<Permission> requiredPermissions,
            Set<Principal> principals ) {
        if ( linkingEntitySet.getLinkedEntitySets().isEmpty() ) {
            return Maps.newHashMap();
        }

        final var propertyTypeIds = getAllPropertiesOnEntitySet(
                linkingEntitySet.getLinkedEntitySets().iterator().next() );
        final var propertyPermissions = getPermissionsOnLinkingEntitySetProperties(
                linkingEntitySet.getLinkedEntitySets(), propertyTypeIds, principals );

        final var authorizedProperties = propertyPermissions.entrySet().stream()
                .filter( entry -> entry.getValue().containsAll( requiredPermissions ) )
                .collect( Collectors.toMap(
                        entry -> entry.getKey().getId(),
                        Map.Entry::getKey
                ) );

        return linkingEntitySet.getLinkedEntitySets().stream().collect( Collectors.toMap(
                Function.identity(),
                esId -> authorizedProperties
        ) );

    }

    /**
     * Note: entitysets are assumed to have same entity type
     */
    @Timed
    public Map<UUID, Map<UUID, PropertyType>> getAuthorizedPropertyTypesByNormalEntitySet(
            EntitySet linkingEntitySet,
            Set<UUID> selectedProperties,
            EnumSet<Permission> requiredPermissions ) {
        if ( linkingEntitySet.getLinkedEntitySets().isEmpty() ) {
            return Maps.newHashMap();
        }

        final var propertyPermissions = getPermissionsOnLinkingEntitySetProperties(
                linkingEntitySet.getLinkedEntitySets(), selectedProperties, Principals.getCurrentPrincipals() );

        final var authorizedProperties = propertyPermissions.entrySet().stream()
                .filter( entry -> entry.getValue().containsAll( requiredPermissions ) )
                .collect( Collectors.toMap(
                        entry -> entry.getKey().getId(),
                        Map.Entry::getKey
                ) );

        return linkingEntitySet.getLinkedEntitySets().stream().collect( Collectors.toMap(
                Function.identity(),
                esId -> authorizedProperties
        ) );
    }

    /**
     * @return the intersection of permissions for each provided property type id of the normal entity sets
     */
    private Map<PropertyType, EnumSet<Permission>> getPermissionsOnLinkingEntitySetProperties(
            Set<UUID> entitySetIds, Set<UUID> selectedProperties, Set<Principal> principals ) {
        final var propertyTypes = edm.getPropertyTypesAsMap( selectedProperties );

        final var aclKeySets = propertyTypes.keySet().stream().collect( Collectors.toMap(
                Function.identity(),
                ptId -> entitySetIds.stream().map( esId -> new AclKey( esId, ptId ) ).collect( Collectors.toSet() ) ) );

        final var permissionsMap = authz.getSecurableObjectSetsPermissions( aclKeySets.values(), principals );

        return propertyTypes.values().stream().collect( Collectors.toMap( Function.identity(),
                pt -> permissionsMap.get( aclKeySets.get( pt.getId() ) ) ) );
    }

    /**
     * Returns authorized property types for entity sets.
     * Note: entity sets are assumed to have same entity type
     *
     * @param entitySetIds        the ids of entity sets to check for
     * @param requiredPermissions the permissions to check for
     * @param principals          the principals to check against
     * @return Map of authorized property types by entity set ids
     */
    @Timed
    public Map<UUID, Map<UUID, PropertyType>> getAuthorizedPropertiesOnNormalEntitySets(
            Set<UUID> entitySetIds,
            EnumSet<Permission> requiredPermissions,
            Set<Principal> principals ) {
        return ( entitySetIds.isEmpty() )
                ? Maps.newHashMap()
                : getAuthorizedPropertyTypes(
                entitySetIds,
                getAllPropertiesOnEntitySet( entitySetIds.iterator().next() ),
                requiredPermissions,
                principals );
    }

    @Timed
    public Set<UUID> getAuthorizedPropertyTypeIds(
            UUID entitySetId,
            EnumSet<Permission> requiredPermissions ) {
        final var entitySet = entitySetManager.getEntitySet( entitySetId );
        final var properties = getAllPropertiesOnEntitySet( entitySetId );

        return ( entitySet.isLinking() )
                ? getAuthorizedPropertyTypeIdsOnLinkingEntitySet( entitySet, properties, requiredPermissions )
                : getAuthorizedPropertyTypeIdsOnNormalEntitySet( entitySetId, properties, requiredPermissions );
    }

    private Set<UUID> getAuthorizedPropertyTypeIdsOnNormalEntitySet(
            UUID entitySetId,
            Set<UUID> selectedProperties,
            EnumSet<Permission> requiredPermissions ) {
        return authz.accessChecksForPrincipals( selectedProperties.stream()
                .map( ptId -> new AccessCheck( new AclKey( entitySetId, ptId ), requiredPermissions ) ).collect(
                        Collectors.toSet() ), Principals.getCurrentPrincipals() )
                .filter( authorization -> authorization.getPermissions().values().stream().allMatch( val -> val ) )
                .map( authorization -> authorization.getAclKey().get( 1 ) )
                .collect( Collectors.toSet() );
    }

    private Set<UUID> getAuthorizedPropertyTypeIdsOnLinkingEntitySet(
            EntitySet linkingEntitySet,
            Set<UUID> selectedProperties,
            EnumSet<Permission> requiredPermissions ) {
        final var propertyPermissions = getPermissionsOnLinkingEntitySetProperties(
                linkingEntitySet.getLinkedEntitySets(), selectedProperties, Principals.getCurrentPrincipals() );

        return propertyPermissions.entrySet().stream()
                .filter( entry -> entry.getValue().containsAll( requiredPermissions ) )
                .map( entry -> entry.getKey().getId() )
                .collect( Collectors.toSet() );
    }

    /**
     * Collects the authorized property types mapped by the requested entity sets. For normal entity sets it does the
     * general checks for each of them and returns only those property types, where it has the required permissions.
     * For linking entity sets it return only those property types, where the calling user has the required permissions
     * in all of the normal entity sets.
     * Note: The returned maps keys are the requested entity set ids and not the normal entity set ids for linking
     * entity sets!
     *
     * @param entitySetIds        The entity set ids for which to get the authorized property types.
     * @param requiredPermissions The set of required permissions to check for.
     * @param principals          The set of pricipals to check the permissions against.
     * @return A Map with keys for each of the requested entity set id and values of authorized property types by their
     * id.
     */
    @Timed
    public Map<UUID, Map<UUID, PropertyType>> getAuthorizedPropertiesOnEntitySets(
            Set<UUID> entitySetIds,
            EnumSet<Permission> requiredPermissions,
            Set<Principal> principals ) {
        if ( entitySetIds.isEmpty() ) {
            return Maps.newHashMap();
        }

        Map<UUID, Map<UUID, PropertyType>> entitySetIdsToPropertyTypes = entitySetManager
                .getPropertyTypesOfEntitySets( entitySetIds );

        Map<AclKey, EnumSet<Permission>> accessChecks = Maps.newLinkedHashMapWithExpectedSize( entitySetIdsToPropertyTypes.size() );

        entitySetIdsToPropertyTypes.entrySet().forEach( entry -> {
            UUID entitySetId = entry.getKey();

            entry.getValue().keySet().forEach( propertyTypeId -> {
                accessChecks.put( new AclKey( entitySetId, propertyTypeId ), requiredPermissions );
            } );
        } );

        Map<UUID, Map<UUID, PropertyType>> authorizedEntitySetsToPropertyTypes = Maps
                .newLinkedHashMapWithExpectedSize( entitySetIdsToPropertyTypes.size() );

        authz.authorize( accessChecks, principals ).forEach( (aclKey, permissionsMap) -> {

            boolean isAuthorized = true;

            for ( boolean p : permissionsMap.values() ) {
                if ( !p ) {
                    isAuthorized = false;
                    break;
                }
            }

            if ( isAuthorized ) {
                UUID entitySetId = aclKey.get( 0 );
                UUID propertyTypeId = aclKey.get( 1 );

                Map<UUID, PropertyType> entitySetMapWithoutAuth = entitySetIdsToPropertyTypes.get( entitySetId );

                Map<UUID, PropertyType> entitySetMapWithAuth = authorizedEntitySetsToPropertyTypes.getOrDefault( entitySetId,
                        Maps.newLinkedHashMapWithExpectedSize( entitySetMapWithoutAuth.size() ) );

                entitySetMapWithAuth.put( propertyTypeId, entitySetMapWithoutAuth.get( propertyTypeId ) );

                authorizedEntitySetsToPropertyTypes.put( entitySetId, entitySetMapWithAuth );
            }
        } );

        return authorizedEntitySetsToPropertyTypes;
    }

    @Timed
    public Map<UUID, Map<UUID, PropertyType>> getAuthorizedPropertiesOnEntitySets(
            Set<UUID> entitySetIds,
            EnumSet<Permission> requiredPermissions ) {
        return getAuthorizedPropertiesOnEntitySets(
                entitySetIds, requiredPermissions, Principals.getCurrentPrincipals()
        );
    }

    @Timed
    public Set<UUID> getAuthorizedEntitySetsForPrincipals(
            Set<UUID> entitySetIds,
            EnumSet<Permission> requiredPermissions,
            Set<Principal> principals ) {
        return entitySetManager.filterToAuthorizedNormalEntitySets(entitySetIds, requiredPermissions, principals);
    }

    /**
     * Get all property types of an entity set
     *
     * @param entitySetId the id of the entity set
     * @return all the property type ids on the entity type of the entity set
     */
    @Timed
    public Set<UUID> getAllPropertiesOnEntitySet( UUID entitySetId ) {
        EntityType et = entitySetManager.getEntityTypeByEntitySetId( entitySetId );
        return et.getProperties();
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authz;
    }

    public static Map<AclKey, EnumSet<Permission>> aclKeysForAccessCheck(
            SetMultimap<UUID, UUID> rawAclKeys,
            EnumSet<Permission> requiredPermission ) {
        return rawAclKeys.entries().stream()
                .collect( Collectors.toMap( e -> new AclKey( e.getKey(), e.getValue() ), e -> requiredPermission ) );
    }
}
