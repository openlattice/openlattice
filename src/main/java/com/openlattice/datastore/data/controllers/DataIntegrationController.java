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

package com.openlattice.datastore.data.controllers;

import static com.openlattice.authorization.EdmAuthorizationHelper.READ_PERMISSION;
import static com.openlattice.authorization.EdmAuthorizationHelper.WRITE_PERMISSION;
import static com.openlattice.authorization.EdmAuthorizationHelper.aclKeysForAccessCheck;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.EdmAuthorizationHelper;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principals;
import com.openlattice.controllers.exceptions.ForbiddenException;
import com.openlattice.data.DataEdgeKey;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DataIntegrationApi;
import com.openlattice.data.EntityKey;
import com.openlattice.data.IntegrationResults;
import com.openlattice.data.graph.DataGraphServiceHelper;
import com.openlattice.data.integration.Association;
import com.openlattice.data.integration.BulkDataCreation;
import com.openlattice.data.integration.Entity;
import com.openlattice.data.integration.S3EntityData;
import com.openlattice.data.storage.PostgresDataSinkService;
import com.openlattice.data.storage.aws.AwsDataSinkService;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.EntitySetService;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.set.EntitySetFlag;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.graph.query.GraphQueryState.Option;
import com.openlattice.search.SearchService;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping( DataIntegrationApi.CONTROLLER )
public class DataIntegrationController implements DataIntegrationApi, AuthorizingComponent {
    @Inject
    private EdmService dms;

    @Inject
    private DataGraphManager dgm;

    @Inject
    private PostgresDataSinkService postgresDataSinkService;

    @Inject
    private AwsDataSinkService awsDataSinkService;

    @Inject
    private AuthorizationManager authz;

    @Inject
    private EdmAuthorizationHelper authzHelper;

    @Inject
    private AuthenticationManager authProvider;

    @Inject
    private SearchService searchService;

    @Inject
    private DataGraphServiceHelper dataGraphServiceHelper;

    @Inject
    private EntitySetService entitySetService;

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authz;
    }

    @Timed
    @PostMapping( "/" + ENTITY_SET + "/" + SET_ID_PATH )
    @Override
    public IntegrationResults integrateEntities(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestParam( value = DETAILED_RESULTS, required = false, defaultValue = "false" ) boolean detailedResults,
            @RequestBody Map<String, Map<UUID, Set<Object>>> entities ) {
        //Ensure that we have read access to entity set metadata.
        ensureEntitySetsCanBeWritten( ImmutableSet.of( entitySetId ) );
        ensureReadAccess( new AclKey( entitySetId ) );
        //Load authorized property types
        final Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes( entitySetId, WRITE_PERMISSION );

        final Map<String, UUID> entityKeyIds = dgm.integrateEntities( entitySetId, entities, authorizedPropertyTypes );
        final IntegrationResults results = new IntegrationResults( entityKeyIds.size(),
                0,
                detailedResults ? Optional.of( ImmutableMap.of( entitySetId, entityKeyIds ) ) : Optional.empty(),
                Optional.empty() );
        return results;
    }

    @Timed
    @PostMapping( "/" + ASSOCIATION + "/" + SET_ID_PATH )
    @Override
    public IntegrationResults integrateAssociations(
            @RequestBody Set<Association> associations,
            @RequestParam( value = DETAILED_RESULTS, required = false, defaultValue = "false" )
                    boolean detailedResults ) {
        if ( associations.isEmpty() ) {
            return new IntegrationResults( 0, 0, Optional.empty(), Optional.empty() );
        }
        Set<UUID> associationEntitySets = performAccessChecksOnEntitiesAndAssociations( associations,
                ImmutableSet.of() );

        //Ensure that we have read access to entity set metadata.
        accessCheck( aclKeysForAccessCheck( requiredAssociationPropertyTypes( associations ), WRITE_PERMISSION ) );

        final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySet = authzHelper
                .getAuthorizedPropertiesOnEntitySets(
                        associationEntitySets,
                        WRITE_PERMISSION,
                        Principals.getCurrentPrincipals()
                );

        // Check allowed src,dst entity types
        dataGraphServiceHelper.checkAssociationEntityTypes( associations );

        final Map<UUID, Map<String, UUID>> entityKeyIds = dgm
                .integrateAssociations( associations, authorizedPropertyTypesByEntitySet );
        final IntegrationResults results = new IntegrationResults( 0,
                entityKeyIds.values().stream().mapToInt( Map::size ).sum(),
                Optional.empty(),
                detailedResults ? Optional.of( entityKeyIds ) : Optional.empty() );
        return results;
    }

    @Timed
    @PostMapping( { "/", "" } )
    @Override
    public IntegrationResults integrateEntityAndAssociationData(
            @RequestBody BulkDataCreation data,
            @RequestParam( value = DETAILED_RESULTS, required = false, defaultValue = "false" )
                    boolean detailedResults ) {
        final Set<Entity> entities = data.getEntities();
        final Set<Association> associations = data.getAssociations();

        if ( entities.isEmpty() && associations.isEmpty() ) {
            return new IntegrationResults( 0, 0, Optional.empty(), Optional.empty() );
        }

        Set<UUID> entitySetIds = performAccessChecksOnEntitiesAndAssociations( associations, entities );

        accessCheck( aclKeysForAccessCheck( requiredEntityPropertyTypes( entities ), WRITE_PERMISSION ) );
        accessCheck( aclKeysForAccessCheck( requiredAssociationPropertyTypes( associations ), WRITE_PERMISSION ) );

        final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySet =
                entitySetIds.stream().collect( Collectors.toMap( Function.identity(),
                        entitySetId -> authzHelper.getAuthorizedPropertyTypes(
                                entitySetId, WRITE_PERMISSION ) ) );
        dataGraphServiceHelper.checkAssociationEntityTypes( associations );
        return dgm.integrateEntitiesAndAssociations( entities, associations, authorizedPropertyTypesByEntitySet );
    }

    @Override
    public List<String> generatePresignedUrls( Collection<S3EntityData> data ) {
        throw new UnsupportedOperationException( "This shouldn't be invoked. Just here for the interface and efficiency" );
    }

    @Timed
    @PostMapping( "/" + S3 )
    public List<String> generatePresignedUrls(
            @RequestBody List<S3EntityData> data ) {
        final Set<UUID> entitySetIds = data.stream().map( S3EntityData::getEntitySetId ).collect(
                Collectors.toSet() );
        final SetMultimap<UUID, UUID> propertyIdsByEntitySet = HashMultimap.create();
        data.forEach( entity -> propertyIdsByEntitySet
                .put( entity.getEntitySetId(), entity.getPropertyTypeId() ) );

        //Ensure that we have read access to entity set metadata.
        entitySetIds.forEach( entitySetId -> ensureReadAccess( new AclKey( entitySetId ) ) );

        accessCheck( aclKeysForAccessCheck( propertyIdsByEntitySet, WRITE_PERMISSION ) );

        final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes =
                entitySetIds.stream()
                        .collect( Collectors.toMap( Function.identity(),
                                entitySetId -> authzHelper.getAuthorizedPropertyTypes(
                                        entitySetId, WRITE_PERMISSION ) ) );

        return awsDataSinkService.generatePresignedUrls( data, authorizedPropertyTypes );
    }

    //Just sugar to conform to API interface. While still allow efficient serialization.
    @Override
    public List<UUID> getEntityKeyIds( Set<EntityKey> entityKeys ) {
        throw new UnsupportedOperationException( "Nobody should be calling this." );
    }

    @PostMapping( "/" + ENTITY_KEY_IDS )
    @Timed
    public Set<UUID> getEntityKeyIds( @RequestBody LinkedHashSet<EntityKey> entityKeys ) {
        return dgm.getEntityKeyIds( entityKeys );
    }

    @Override
    @PutMapping( "/" + EDGES )
    public int createEdges( @RequestBody Set<DataEdgeKey> associations ) {
        final Set<UUID> entitySetIds = Sets.newHashSetWithExpectedSize( associations.size() * 3 );
        ;
        associations.forEach(
                association -> {
                    entitySetIds.add( association.getEdge().getEntitySetId() );
                    entitySetIds.add( association.getSrc().getEntitySetId() );
                    entitySetIds.add( association.getDst().getEntitySetId() );
                }
        );

        checkPermissionsOnEntitySetIds( entitySetIds, WRITE_PERMISSION );

        //Allowed entity types check
        dataGraphServiceHelper.checkEdgeEntityTypes( associations );

        return dgm.createAssociations( associations ).getNumUpdates();
    }

    private Set<UUID> performAccessChecksOnEntitiesAndAssociations(
            Set<Association> associations,
            Set<Entity> entities ) {
        final Set<UUID> entitySetIds = Sets.newHashSetWithExpectedSize( ( associations.size() * 3 ) + entities.size() );
        entities.forEach( entity -> entitySetIds.add( entity.getEntitySetId() ) );
        associations.forEach(
                association -> {
                    entitySetIds.add( association.getSrc().getEntitySetId() );
                    entitySetIds.add( association.getDst().getEntitySetId() );
                    entitySetIds.add( association.getKey().getEntitySetId() );
                }
        );

        checkPermissionsOnEntitySetIds( entitySetIds, READ_PERMISSION );

        return entitySetIds;
    }

    private void checkPermissionsOnEntitySetIds( Set<UUID> entitySetIds, EnumSet<Permission> permissions ) {
        //Ensure that we have access to entity sets.
        ensureEntitySetsCanBeWritten( entitySetIds );
        accessCheck( entitySetIds.stream().collect( Collectors.toMap( AclKey::new, id -> permissions ) ) );
    }

    private void ensureEntitySetsCanBeWritten( Set<UUID> entitySetIds ) {
        if ( entitySetService.entitySetsContainFlag( entitySetIds, EntitySetFlag.AUDIT ) ) {
            Set<UUID> auditEntitySetIds = entitySetService.getEntitySetsAsMap( entitySetIds ).values().stream()
                    .filter( it -> it.getFlags().contains( EntitySetFlag.AUDIT ) ).map( EntitySet::getId ).collect(
                            Collectors.toSet() );
            throw new ForbiddenException( "You cannot modify data of entity sets " + auditEntitySetIds.toString()
                    + " because they are audit entity sets." );
        }
    }

    private static SetMultimap<UUID, UUID> requiredAssociationPropertyTypes( Set<Association> associations ) {
        final SetMultimap<UUID, UUID> propertyTypesByEntitySet = HashMultimap.create();
        associations.forEach( association -> propertyTypesByEntitySet
                .putAll( association.getKey().getEntitySetId(), association.getDetails().keySet() ) );
        return propertyTypesByEntitySet;
    }

    private static SetMultimap<UUID, UUID> requiredEntityPropertyTypes( Set<Entity> entities ) {
        final SetMultimap<UUID, UUID> propertyTypesByEntitySet = HashMultimap.create();
        entities.forEach( entity -> propertyTypesByEntitySet
                .putAll( entity.getEntitySetId(), entity.getDetails().keySet() ) );
        return propertyTypesByEntitySet;
    }

}

