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

package com.openlattice.datastore.linking.controllers;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principals;
import com.openlattice.data.EntityKey;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.set.LinkingEntitySet;
import com.openlattice.linking.HazelcastListingService;
import com.openlattice.linking.LinkingApi;
import com.openlattice.linking.requests.LinkingRequest;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.inject.Inject;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import retrofit2.http.Body;
import retrofit2.http.Path;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@RestController
@RequestMapping( LinkingApi.CONTROLLER )
public class LinkingController implements LinkingApi, AuthorizingComponent {

    @Inject
    private AuthorizationManager authorizationManager;

    @Inject
    private EdmManager edm;

    @Inject
    private HazelcastListingService listings;


    @Override
    @PostMapping(
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public UUID linkEntitySets( @RequestBody LinkingRequest linkingRequest ) {
        LinkingEntitySet linkingEntitySet = linkingRequest.getLinkingEntitySet();
        Set<Map<UUID, UUID>> linkingProperties = linkingEntitySet.getLinkingProperties();
        EntitySet entitySet = linkingEntitySet.getEntitySet();

        // Validate, compute the ownable property types after merging.
        Set<UUID> ownablePropertyTypes = validateAndGetOwnablePropertyTypes( entitySet, linkingProperties );

        Set<UUID> propertyTypesToPopulate = Sets.intersection( ownablePropertyTypes,
                linkingRequest.getResultPropertyTypeIds() );

        edm.createEntitySet( Principals.getCurrentUser(), entitySet, ownablePropertyTypes );
        UUID linkedEntitySetId = entitySet.getId();
        return linkedEntitySetId;
    }

    @Override
    public UUID linkEntities(
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_ID ) UUID entityId,
            @Body Set<EntityKey> entities ) {
        return null;
    }

    @Override
    public Void setLinkedEntities(
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_ID ) UUID entityId,
            @Body Set<EntityKey> entities ) {
        return null;
    }

    @Override
    public Void deleteLinkedEntities(
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_ID ) UUID entityId ) {
        return null;
    }

    @Override
    public Void addLinkedEntities(
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_ID ) UUID entityId,
            @Path( LINKED_ENTITY_ID ) UUID linkedEntityId ) {
        return null;
    }

    @Override
    public Void removeLinkedEntity(
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_ID ) UUID entityId,
            @Path( LINKED_ENTITY_ID ) UUID linkedEntityId ) {
        return null;
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizationManager;
    }

    private Set<UUID> validateAndGetOwnablePropertyTypes(
            EntitySet linkedEntitySet,
            Set<Map<UUID, UUID>> linkingProperties ) {

        // Validate: each map in the set should have a unique value, which is distinct across the linking properties
        // set.
        Set<UUID> linkingES = new HashSet<>();
        Set<UUID> validatedProperties = new HashSet<>();
        SetMultimap<UUID, UUID> linkIndexedByPropertyTypes = HashMultimap.create();

        linkingProperties.stream().forEach( link -> {
            Set<UUID> values = new HashSet<>( link.values() );

            Preconditions.checkArgument( values.size() == 1,
                    "Each linking map should involve a unique property type." );
            // Commented out the following check to allow easy dedupe for now.
            /**
             * Preconditions.checkArgument( link.entrySet().size() > 1, "Each linking map must be matching at least two
             * entity sets." );
             */
            // Get the value of common property type id in the linking map.
            UUID propertyId = values.iterator().next();
            Preconditions.checkArgument( !validatedProperties.contains( propertyId ),
                    "There should be only one linking map that involves property id " + propertyId );

            for ( UUID esId : link.keySet() ) {
                AclKey aclKey = new AclKey( esId, propertyId );
                ensureLinkAccess( aclKey );
                linkingES.add( esId );
                linkIndexedByPropertyTypes.put( propertyId, esId );
            }
        } );

        // Sanity check: authorized to link the entity set itself.
        linkingES.stream().forEach( entitySetId -> ensureLinkAccess( new AclKey( entitySetId ) ) );

        // Compute the ownable property types after merging. A property type is ownable if calling user has both READ
        // and LINK permissions for that property type in all the entity sets involved.
        Set<UUID> linkedPropertyTypes = edm.getEntityType( linkedEntitySet.getEntityTypeId() ).getProperties();

        SetMultimap<UUID, UUID> propertyIdESMap = HashMultimap.create();
        linkingES.stream().forEach( esId -> {
            Set<UUID> properties = edm.getEntityTypeByEntitySetId( esId ).getProperties();
            for ( UUID propertyId : properties ) {
                propertyIdESMap.put( propertyId, esId );
            }
        } );

        Set<UUID> ownablePropertyTypes = new HashSet<>();
        for ( UUID propertyId : linkedPropertyTypes ) {
            boolean ownable = propertyIdESMap.get( propertyId ).stream()
                    .map( esId -> new AclKey( esId, propertyId ) )
                    .allMatch( isAuthorized( Permission.LINK, Permission.READ ) );

            if ( ownable ) {
                ownablePropertyTypes.add( propertyId );
            }
        }
        if ( ownablePropertyTypes.isEmpty() ) {
            throw new IllegalArgumentException( "There will be no ownable properties in the linked entity set." );
        }

        return ownablePropertyTypes;
    }

}
