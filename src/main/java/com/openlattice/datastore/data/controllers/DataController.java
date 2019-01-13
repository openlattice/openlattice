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

import com.auth0.spring.security.api.authentication.PreAuthenticatedAuthenticationJsonWebToken;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.*;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.EdmAuthorizationHelper;
import com.openlattice.authorization.ForbiddenException;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principals;
import com.openlattice.data.DataApi;
import com.openlattice.data.DataAssociation;
import com.openlattice.data.DataEdge;
import com.openlattice.data.DataEdgeKey;
import com.openlattice.data.DataGraph;
import com.openlattice.data.DataGraphIds;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntitySetData;
import com.openlattice.data.UpdateType;
import com.openlattice.data.requests.EntitySetSelection;
import com.openlattice.data.requests.FileType;
import com.openlattice.web.mediatypes.CustomMediaType;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.SyncTicketService;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.transformValues;
import static com.openlattice.authorization.EdmAuthorizationHelper.READ_PERMISSION;
import static com.openlattice.authorization.EdmAuthorizationHelper.WRITE_PERMISSION;
import static com.openlattice.authorization.EdmAuthorizationHelper.aclKeysForAccessCheck;

@RestController
@RequestMapping( DataApi.CONTROLLER )
public class DataController implements DataApi, AuthorizingComponent {
    private static final Logger logger = LoggerFactory.getLogger( DataController.class );

    @Inject
    private SyncTicketService sts;

    @Inject
    private EdmService edmService;

    @Inject
    private DataGraphManager dgm;

    @Inject
    private AuthorizationManager authz;

    @Inject
    private EdmAuthorizationHelper authzHelper;

    @Inject
    private AuthenticationManager authProvider;

    private LoadingCache<UUID, EdmPrimitiveTypeKind>  primitiveTypeKinds;
    private LoadingCache<AuthorizationKey, Set<UUID>> authorizedPropertyCache;

    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH },
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    public EntitySetData<FullQualifiedName> loadEntitySetData(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestParam(
                    value = FILE_TYPE,
                    required = false ) FileType fileType,
            @RequestParam(
                    value = TOKEN,
                    required = false ) String token,
            HttpServletResponse response ) {
        setContentDisposition( response, entitySetId.toString(), fileType );
        setDownloadContentType( response, fileType );

        return loadEntitySetData( entitySetId, fileType, token );
    }

    @Override
    public EntitySetData<FullQualifiedName> loadEntitySetData(
            UUID entitySetId,
            FileType fileType,
            String token ) {
        if ( StringUtils.isNotBlank( token ) ) {
            Authentication authentication = authProvider
                    .authenticate( PreAuthenticatedAuthenticationJsonWebToken.usingToken( token ) );
            SecurityContextHolder.getContext().setAuthentication( authentication );
        }
        return loadEntitySetData( entitySetId, new EntitySetSelection( Optional.empty() ) );
    }

    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH },
            method = RequestMethod.POST,
            consumes = { MediaType.APPLICATION_JSON_VALUE },
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    public EntitySetData<FullQualifiedName> loadEntitySetData(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody( required = false ) EntitySetSelection selection,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {
        setContentDisposition( response, entitySetId.toString(), fileType );
        setDownloadContentType( response, fileType );
        return loadEntitySetData( entitySetId, selection, fileType );
    }

    @Override
    public EntitySetData<FullQualifiedName> loadEntitySetData(
            UUID entitySetId,
            EntitySetSelection selection,
            FileType fileType ) {
        return loadEntitySetData( entitySetId, selection );
    }

    private EntitySetData<FullQualifiedName> loadEntitySetData(
            UUID entitySetId,
            EntitySetSelection selection ) {
        if ( authz.checkIfHasPermissions( new AclKey( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            EntitySet es = edmService.getEntitySet( entitySetId );
            if ( es.isLinking() ) {
                Set<UUID> allEntitySetIds = Sets.newHashSet( es.getLinkedEntitySets() );
                checkState( !allEntitySetIds.isEmpty(),
                        "Linked entity sets are empty for linking entity set %s", entitySetId );
                return loadEntitySetData(
                        allEntitySetIds.stream().collect( Collectors.toMap(
                                Function.identity(),
                                esId -> selection.getEntityKeyIds() ) ),
                        allEntitySetIds,
                        selection,
                        true );
            } else {
                return loadEntitySetData(
                        Map.of( entitySetId, selection.getEntityKeyIds() ),
                        Set.of( entitySetId ),
                        selection,
                        false );
            }
        } else {
            throw new ForbiddenException( "Insufficient permissions to read the entity set or it doesn't exists." );
        }
    }

    private EntitySetData<FullQualifiedName> loadEntitySetData(
            Map<UUID, Optional<Set<UUID>>> entityKeyIds,
            Set<UUID> dataEntitySetIds,
            EntitySetSelection selection,
            Boolean linking ) {
        final Set<UUID> allProperties = authzHelper.getAllPropertiesOnEntitySet( dataEntitySetIds.iterator().next() );
        final Set<UUID> selectedProperties = selection.getProperties().orElse( allProperties );

        checkState( allProperties.equals( selectedProperties ) || allProperties.containsAll( selectedProperties ),
                "Selected properties are not property types of entity set %s",
                dataEntitySetIds.iterator().next() );

        final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes =
                authzHelper.getAuthorizedPropertyTypes( dataEntitySetIds,
                        selectedProperties,
                        EnumSet.of( Permission.READ ) );

        final Map<UUID, PropertyType> allAuthorizedPropertyTypes =
                authorizedPropertyTypes.values().stream()
                        .flatMap( it -> it.values().stream() ).distinct()
                        .collect( Collectors.toMap( PropertyType::getId, Function.identity() ) );

        final LinkedHashSet<String> orderedPropertyNames = new LinkedHashSet<>( allAuthorizedPropertyTypes.size() );

        selectedProperties.stream()
                .filter( allAuthorizedPropertyTypes::containsKey )
                .map( allAuthorizedPropertyTypes::get )
                .map( pt -> pt.getType().getFullQualifiedNameAsString() )
                .forEach( orderedPropertyNames::add );

        return dgm.getEntitySetData( entityKeyIds, orderedPropertyNames, authorizedPropertyTypes, linking );
    }

    @Override
    @PutMapping(
            value = "/" + ENTITY_SET + "/" + SET_ID_PATH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Integer updateEntitiesInEntitySet(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody Map<UUID, Map<UUID, Set<Object>>> entities,
            @RequestParam( value = TYPE, defaultValue = "Merge" ) UpdateType updateType ) {
        Preconditions.checkNotNull( updateType, "An invalid update type value was specified." );
        ensureReadAccess( new AclKey( entitySetId ) );
        var allAuthorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes( entitySetId, EnumSet.of( Permission.WRITE ) );
        var requiredPropertyTypes = requiredEntitySetPropertyTypes( entities );

        accessCheck( allAuthorizedPropertyTypes, requiredPropertyTypes );

        var authorizedPropertyTypes = Maps.asMap( requiredPropertyTypes, allAuthorizedPropertyTypes::get );

        switch ( updateType ) {
            case Replace:
                return dgm.replaceEntities( entitySetId, entities, authorizedPropertyTypes );
            case PartialReplace:
                return dgm.partialReplaceEntities( entitySetId, entities, authorizedPropertyTypes );
            case Merge:
                return dgm.mergeEntities( entitySetId, entities, authorizedPropertyTypes );
            default:
                return 0;
        }
    }

    @PatchMapping(
            value = "/" + ENTITY_SET + "/" + SET_ID_PATH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @Override
    public Integer replaceEntityProperties(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody Map<UUID, SetMultimap<UUID, Map<ByteBuffer, Object>>> entities ) {
        ensureReadAccess( new AclKey( entitySetId ) );

        final Set<UUID> requiredPropertyTypes = requiredReplacementPropertyTypes( entities );
        accessCheck( aclKeysForAccessCheck( ImmutableSetMultimap.<UUID, UUID>builder()
                        .putAll( entitySetId, requiredPropertyTypes ).build(),
                WRITE_PERMISSION ) );

        return dgm.replacePropertiesInEntities( entitySetId,
                entities,
                edmService.getPropertyTypesAsMap( requiredPropertyTypes ) );
    }

    @Override
    @PutMapping( value = "/" + ASSOCIATION, consumes = MediaType.APPLICATION_JSON_VALUE )
    public Integer createAssociations( @RequestBody Set<DataEdgeKey> associations ) {
        //TODO: This allows creating an edge even if you don't have access to key properties on association
        //entity set. Consider requiring access to key properties on association in order to allow creating edge.
        associations.stream()
                .flatMap( dataEdgeKey -> Stream.of( dataEdgeKey.getSrc().getEntitySetId(),
                        dataEdgeKey.getDst().getEntitySetId(),
                        dataEdgeKey.getEdge().getEntitySetId() ) );

        final Set<UUID> entitySetIds = associations.stream()
                .flatMap( edgeKey -> Stream.of(
                        edgeKey.getSrc().getEntitySetId(),
                        edgeKey.getDst().getEntitySetId(),
                        edgeKey.getEdge().getEntitySetId() ) )
                .collect( Collectors.toSet() );

        //Ensure that we have read access to entity set metadata.
        entitySetIds.forEach( entitySetId -> ensureReadAccess( new AclKey( entitySetId ) ) );

        return dgm.createAssociations( associations );
    }

    @Timed
    @Override
    @RequestMapping(
            value = "/" + ENTITY_SET + "/",
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public List<UUID> createEntities(
            @RequestParam( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody List<Map<UUID, Set<Object>>> entities ) {
        //Ensure that we have read access to entity set metadata.
        ensureReadAccess( new AclKey( entitySetId ) );
        //Load authorized property types
        final Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes( entitySetId, WRITE_PERMISSION );
        return dgm.createEntities( entitySetId, entities, authorizedPropertyTypes );
    }

    @Override
    @PutMapping(
            value = "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Integer mergeIntoEntityInEntitySet(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @RequestBody Map<UUID, Set<Object>> entity ) {
        final var entities = ImmutableMap.of( entityKeyId, entity );
        return updateEntitiesInEntitySet( entitySetId, entities, UpdateType.Merge );
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authz;
    }

    @Timed
    @Override
    @RequestMapping(
            path = { "/" + ASSOCIATION },
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public ListMultimap<UUID, UUID> createAssociations( @RequestBody ListMultimap<UUID, DataEdge> associations ) {
        //Ensure that we have read access to entity set metadata.
        associations.keySet().forEach( entitySetId -> ensureReadAccess( new AclKey( entitySetId ) ) );

        //Ensure that we can write properties.
        final SetMultimap<UUID, UUID> requiredPropertyTypes = requiredAssociationPropertyTypes( associations );
        accessCheck( aclKeysForAccessCheck( requiredPropertyTypes, WRITE_PERMISSION ) );

        final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySet =
                associations.keySet().stream()
                        .collect( Collectors.toMap( Function.identity(),
                                entitySetId -> authzHelper
                                        .getAuthorizedPropertyTypes( entitySetId, EnumSet.of( Permission.WRITE ) ) ) );
        return dgm.createAssociations( associations, authorizedPropertyTypesByEntitySet );
    }

    @Timed
    @Override
    @PatchMapping( value = "/" + ASSOCIATION )
    public Integer replaceAssociationData(
            @RequestBody Map<UUID, Map<UUID, DataEdge>> associations,
            @RequestParam( value = PARTIAL, required = false, defaultValue = "false" ) boolean partial ) {
        associations.keySet().forEach( entitySetId -> ensureReadAccess( new AclKey( entitySetId ) ) );

        //Ensure that we can write properties.
        final SetMultimap<UUID, UUID> requiredPropertyTypes = requiredAssociationPropertyTypes( associations );
        accessCheck( aclKeysForAccessCheck( requiredPropertyTypes, WRITE_PERMISSION ) );

        final Map<UUID, PropertyType> authorizedPropertyTypes = edmService
                .getPropertyTypesAsMap( ImmutableSet.copyOf( requiredPropertyTypes.values() ) );
        return associations.entrySet().stream().mapToInt( association -> {
            final UUID entitySetId = association.getKey();
            if ( partial ) {
                return dgm.partialReplaceEntities( entitySetId,
                        transformValues( association.getValue(), dataEdge -> dataEdge.getData() ),
                        authorizedPropertyTypes );
            } else {

                return dgm.replaceEntities( entitySetId,
                        transformValues( association.getValue(), assoc -> assoc.getData() ),
                        authorizedPropertyTypes );
            }
        } ).sum();
    }

    @Timed
    @Override
    @PostMapping( value = { "/", "" } )
    public DataGraphIds createEntityAndAssociationData( @RequestBody DataGraph data ) {
        final ListMultimap<UUID, UUID> entityKeyIds = ArrayListMultimap.create();
        final ListMultimap<UUID, UUID> associationEntityKeyIds;

        //First create the entities so we have entity key ids to work with
        Multimaps.asMap( data.getEntities() )
                .forEach( ( entitySetId, entities ) ->
                        entityKeyIds.putAll( entitySetId, createEntities( entitySetId, entities ) ) );
        final ListMultimap<UUID, DataEdge> toBeCreated = ArrayListMultimap.create();
        Multimaps.asMap( data.getAssociations() )
                .forEach( ( entitySetId, associations ) -> {
                    for ( DataAssociation association : associations ) {
                        final UUID srcEntitySetId = association.getSrcEntitySetId();
                        final UUID srcEntityKeyId = association
                                .getSrcEntityKeyId()
                                .orElseGet( () ->
                                        entityKeyIds.get( srcEntitySetId )
                                                .get( association.getSrcEntityIndex().get() ) );

                        final UUID dstEntitySetId = association.getDstEntitySetId();
                        final UUID dstEntityKeyId = association
                                .getDstEntityKeyId()
                                .orElseGet( () ->
                                        entityKeyIds.get( dstEntitySetId )
                                                .get( association.getDstEntityIndex().get() ) );

                        toBeCreated.put(
                                entitySetId,
                                new DataEdge(
                                        new EntityDataKey( srcEntitySetId, srcEntityKeyId ),
                                        new EntityDataKey( dstEntitySetId, dstEntityKeyId ),
                                        association.getData() ) );
                    }
                } );
        associationEntityKeyIds = createAssociations( toBeCreated );

        return new DataGraphIds( entityKeyIds, associationEntityKeyIds );
    }

    @Override
    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH },
            method = RequestMethod.DELETE )
    public Void clearEntityFromEntitySet(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        //Note this will only clear properties to which the caller has access.
        dgm.clearEntities( entitySetId,
                ImmutableSet.of( entityKeyId ),
                authzHelper.getAuthorizedPropertyTypes( entitySetId, WRITE_PERMISSION ) );
        return null;
    }

    @Override
    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITIES },
            method = RequestMethod.DELETE )
    public Integer clearAllEntitiesFromEntitySet( @PathVariable( ENTITY_SET_ID ) UUID entitySetId ) {
        ensureOwnerAccess( new AclKey( entitySetId ) );
        final EntitySet entitySet = edmService.getEntitySet( entitySetId );
        if ( entitySet.isLinking() ) {
            throw new ForbiddenException( "You cannot clear all data from a linked entity set." );
        }

        final EntityType entityType = edmService.getEntityType( entitySet.getEntityTypeId() );
        final Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes( entitySetId, EnumSet.of( Permission.OWNER ) );
        if ( !authorizedPropertyTypes.keySet().containsAll( entityType.getProperties() ) ) {
            throw new ForbiddenException(
                    "You must be an owner of all entity set properties to clear the entity set data." );
        }

        return dgm.clearEntitySet( entitySetId, authorizedPropertyTypes );
    }

    @Override public Integer deleteEntityProperties(
            UUID entitySetId, Map<UUID, Map<UUID, Set<ByteBuffer>>> entityProperties ) {
        return null;
    }

    @Override
    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH },
            method = RequestMethod.DELETE )
    public Integer clearEntitySet( @PathVariable( ENTITY_SET_ID ) UUID entitySetId ) {
        ensureOwnerAccess( new AclKey( entitySetId ) );
        return dgm
                .clearEntitySet( entitySetId, authzHelper.getAuthorizedPropertyTypes( entitySetId, WRITE_PERMISSION ) );
    }

    @Timed
    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH + "/" + NEIGHBORS },
            method = RequestMethod.DELETE
    )
    public Long clearEntityAndNeighborEntities(
            @PathVariable( ENTITY_SET_ID ) UUID vertexEntitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID vertexEntityKeyId
    ) {

        /*
         * 1 - collect all relevant EntitySets and PropertyTypes
         */

        // TODO: linking entity sets , linking id (if needed)
        // getNeighborEntitySetIds() returns source, destination, and edge EntitySet ids
        Set<UUID> neighborEntitySetIds = dgm.getNeighborEntitySetIds( ImmutableSet.of( vertexEntitySetId ) );
        Set<UUID> allEntitySetIds = ImmutableSet.<UUID>builder()
                .add( vertexEntitySetId )
                .addAll( neighborEntitySetIds )
                .build();

        Map<AclKey, EnumSet<Permission>> entitySetsAccessRequestMap = allEntitySetIds
                .stream()
                .map( AclKey::new )
                .collect( Collectors.toMap( Function.identity(), aclKey -> READ_PERMISSION ) );

        Map<UUID, Map<UUID, PropertyType>> entitySetIdToPropertyTypesMap = Maps.newHashMap();
        Map<AclKey, EnumSet<Permission>> propertyTypesAccessRequestMap = allEntitySetIds
                .stream()
                .flatMap( esId -> {
                    Map<UUID, PropertyType> propertyTypes = edmService.getPropertyTypesForEntitySet( esId );
                    entitySetIdToPropertyTypesMap.put( esId, propertyTypes );
                    return entitySetIdToPropertyTypesMap.keySet().stream().map( ptId -> new AclKey( esId, ptId ) );
                } )
                .collect( Collectors.toMap( Function.identity(), aclKey -> WRITE_PERMISSION ) );

        /*
         * 2 - check permissions for all relevant EntitySets and PropertyTypes
         */

        Map<AclKey, EnumSet<Permission>> accessRequestMap = Maps.newHashMap();
        accessRequestMap.putAll( entitySetsAccessRequestMap );
        accessRequestMap.putAll( propertyTypesAccessRequestMap );

        accessCheck( accessRequestMap );

        /*
         * 3 - collect all neighbor entities, organized by EntitySet
         */

        Map<UUID, Set<EntityDataKey>> entitySetIdToEntityDataKeysMap = dgm
                .getEdgesAndNeighborsForVertex( vertexEntitySetId, vertexEntityKeyId )
                .flatMap( edge -> Stream.of( edge.getSrc(), edge.getDst(), edge.getEdge() ) )
                .collect( Collectors.groupingBy( EntityDataKey::getEntitySetId, Collectors.toSet() ) );

        /*
         * 4 - clear all entities
         */

        if ( allEntitySetIds.containsAll( entitySetIdToEntityDataKeysMap.keySet() ) ) {
            entitySetIdToEntityDataKeysMap.forEach( ( entitySetId, entityDataKeys ) -> dgm.clearEntities(
                    entitySetId,
                    entityDataKeys.stream().map( EntityDataKey::getEntityKeyId ).collect( Collectors.toSet() ),
                    entitySetIdToPropertyTypesMap.get( entitySetId ) )
            );
            return entitySetIdToEntityDataKeysMap
                    .entrySet()
                    .stream()
                    .mapToLong( entry -> entry.getValue().size() )
                    .sum();
        } else {
            throw new ForbiddenException( "Insufficient permissions to perform operation." );
        }
    }

    @Override
    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH },
            method = RequestMethod.PUT )
    public Integer replaceEntityInEntitySet(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @RequestBody Map<UUID, Set<Object>> entity ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper.getAuthorizedPropertyTypes( entitySetId,
                WRITE_PERMISSION,
                edmService.getPropertyTypesAsMap( entity.keySet() ) );

        return dgm.replaceEntities( entitySetId, ImmutableMap.of( entityKeyId, entity ), authorizedPropertyTypes );
    }

    @Override
    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH },
            method = RequestMethod.POST )
    public Integer replaceEntityInEntitySetUsingFqns(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @RequestBody Map<FullQualifiedName, Set<Object>> entityByFqns ) {
        final Map<UUID, Set<Object>> entity = new HashMap<>();

        entityByFqns
                .forEach( ( fqn, properties ) -> entity.put( edmService.getPropertyTypeId( fqn ), properties ) );

        return replaceEntityInEntitySet( entitySetId, entityKeyId, entity );
    }

    @Override
    @RequestMapping(
            path = { "/" + SET_ID_PATH + "/" + COUNT },
            method = RequestMethod.GET )
    public long getEntitySetSize( @PathVariable( ENTITY_SET_ID ) UUID entitySetId ) {
        ensureReadAccess( new AclKey( entitySetId ) );

        EntitySet es = edmService.getEntitySet( entitySetId );
        // If entityset is linking: should return distinct count of entities corresponding to the linking entity set,
        // which is the distinct count of linking_id s
        if ( es.isLinking() ) {
            return dgm.getLinkingEntitySetSize( es.getLinkedEntitySets() );
        } else {
            return dgm.getEntitySetSize( entitySetId );
        }
    }

    @Override
    @RequestMapping(
            path = { "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH },
            method = RequestMethod.GET )
    public SetMultimap<FullQualifiedName, Object> getEntity(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        EntitySet es = edmService.getEntitySet( entitySetId );

        if ( es.isLinking() ) {
            final Set<UUID> allProperties = authzHelper.getAllPropertiesOnEntitySet(
                    es.getLinkedEntitySets().iterator().next() );
            checkState( !es.getLinkedEntitySets().isEmpty(),
                    "Linked entity sets are empty for linking entity set %s", entitySetId );

            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = authzHelper
                    .getAuthorizedPropertyTypes( es.getLinkedEntitySets(),
                            allProperties,
                            EnumSet.of( Permission.READ ) );

            return dgm.getLinkingEntity( es.getLinkedEntitySets(), entityKeyId, authorizedPropertyTypes );
        } else {
            Map<UUID, PropertyType> authorizedPropertyTypes = edmService.getPropertyTypesAsMap(
                    authzHelper.getAuthorizedPropertiesOnEntitySet( entitySetId, READ_PERMISSION ) );
            return dgm.getEntity( entitySetId, entityKeyId, authorizedPropertyTypes );
        }
    }

    @Override
    @GetMapping(
            path = "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH + "/" + PROPERTY_TYPE_ID_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Set<Object> getEntity( UUID entitySetId, UUID entityKeyId, UUID propertyTypeId ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        EntitySet es = edmService.getEntitySet( entitySetId );

        if ( es.isLinking() ) {
            checkState( !es.getLinkedEntitySets().isEmpty(),
                    "Linked entity sets are empty for linking entity set %s", entitySetId );

            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = authzHelper
                    .getAuthorizedPropertyTypes( es.getLinkedEntitySets(),
                            Set.of( propertyTypeId ),
                            EnumSet.of( Permission.READ ) );
            FullQualifiedName propertyTypeFqn = authorizedPropertyTypes.get( entitySetId ).get( propertyTypeId )
                    .getType();
            return dgm.getLinkingEntity( es.getLinkedEntitySets(), entityKeyId, authorizedPropertyTypes )
                    .get( propertyTypeFqn );
        } else {
            ensureReadAccess( new AclKey( entitySetId, propertyTypeId ) );
            Map<UUID, PropertyType> authorizedPropertyTypes = edmService
                    .getPropertyTypesAsMap( ImmutableSet.of( propertyTypeId ) );
            FullQualifiedName propertyTypeFqn = authorizedPropertyTypes.get( propertyTypeId ).getType();

            return dgm.getEntity( entitySetId, entitySetId, authorizedPropertyTypes )
                    .get( propertyTypeFqn );
        }
    }

    /**
     * Methods for setting http response header
     */

    private static void setDownloadContentType( HttpServletResponse response, FileType fileType ) {
        if ( fileType == FileType.csv ) {
            response.setContentType( CustomMediaType.TEXT_CSV_VALUE );
        } else {
            response.setContentType( MediaType.APPLICATION_JSON_VALUE );
        }
    }

    private static void setContentDisposition(
            HttpServletResponse response,
            String fileName,
            FileType fileType ) {
        if ( fileType == FileType.csv || fileType == FileType.json ) {
            response.setHeader( "Content-Disposition",
                    "attachment; filename=" + fileName + "." + fileType.toString() );
        }
    }

    private static SetMultimap<UUID, UUID> requiredAssociationPropertyTypes( ListMultimap<UUID, DataEdge> associations ) {
        final SetMultimap<UUID, UUID> propertyTypesByEntitySet = HashMultimap.create();
        associations.entries().forEach( entry -> propertyTypesByEntitySet
                .putAll( entry.getKey(), entry.getValue().getData().keySet() ) );
        return propertyTypesByEntitySet;
    }

    private static SetMultimap<UUID, UUID> requiredAssociationPropertyTypes( Map<UUID, Map<UUID, DataEdge>> associations ) {
        final SetMultimap<UUID, UUID> propertyTypesByEntitySet = HashMultimap.create();
        associations.forEach( ( esId, edges ) -> edges.values()
                .forEach( de -> propertyTypesByEntitySet.putAll( esId, de.getData().keySet() ) ) );
        return propertyTypesByEntitySet;
    }

    private static Set<UUID> requiredEntitySetPropertyTypes( Map<UUID, Map<UUID, Set<Object>>> entities ) {
        return entities.values().stream().map( Map::keySet ).flatMap( Set::stream )
                .collect( Collectors.toSet() );
    }

    private static Set<UUID> requiredReplacementPropertyTypes( Map<UUID, SetMultimap<UUID, Map<ByteBuffer, Object>>> entities ) {
        return entities.values().stream().map( SetMultimap::keySet ).flatMap( Set::stream )
                .collect( Collectors.toSet() );
    }

    private static Set<UUID> requiredPropertyAuthorizations( Collection<SetMultimap<UUID, Object>> entities ) {
        return entities.stream().map( SetMultimap::keySet ).flatMap( Set::stream ).collect( Collectors.toSet() );
    }
}
