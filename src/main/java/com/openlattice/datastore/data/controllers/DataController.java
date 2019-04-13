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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.transformValues;
import static com.openlattice.authorization.EdmAuthorizationHelper.READ_PERMISSION;
import static com.openlattice.authorization.EdmAuthorizationHelper.WRITE_PERMISSION;
import static com.openlattice.authorization.EdmAuthorizationHelper.aclKeysForAccessCheck;

import com.auth0.spring.security.api.authentication.PreAuthenticatedAuthenticationJsonWebToken;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.*;
import com.openlattice.auditing.AuditEventType;
import com.openlattice.auditing.AuditRecordEntitySetsManager;
import com.openlattice.auditing.AuditableEvent;
import com.openlattice.auditing.AuditingComponent;
import com.openlattice.authorization.*;
import com.openlattice.data.*;
import com.openlattice.controllers.exceptions.ForbiddenException;
import com.openlattice.data.DataApi;
import com.openlattice.data.DataAssociation;
import com.openlattice.data.DataEdge;
import com.openlattice.data.DataEdgeKey;
import com.openlattice.data.DataGraph;
import com.openlattice.data.DataGraphIds;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DeleteType;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntitySetData;
import com.openlattice.data.UpdateType;
import com.openlattice.data.requests.EntitySetSelection;
import com.openlattice.data.requests.FileType;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.SyncTicketService;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.graph.edge.EdgeKey;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.postgres.streams.PostgresIterable;
import com.openlattice.search.requests.EntityNeighborsFilter;
import com.openlattice.web.mediatypes.CustomMediaType;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.DeleteMapping;
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

@RestController
@RequestMapping( DataApi.CONTROLLER )
public class DataController implements DataApi, AuthorizingComponent, AuditingComponent {
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

    @Inject
    private AuditRecordEntitySetsManager auditRecordEntitySetsManager;

    @Inject
    private SecurePrincipalsManager spm;

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
            Optional<Set<UUID>> entityKeyIds = (selection == null) ? Optional.empty() : selection.getEntityKeyIds();
            Optional<Set<UUID>> propertyTypeIds = (selection == null) ? Optional.empty() : selection.getProperties();

            if ( es.isLinking() ) {
                Set<UUID> allEntitySetIds = Sets.newHashSet( es.getLinkedEntitySets() );
                checkState( !allEntitySetIds.isEmpty(),
                        "Linked entity sets are empty for linking entity set %s", entitySetId );
                return loadEntitySetData(
                        allEntitySetIds.stream().collect( Collectors.toMap(
                                Function.identity(),
                                esId -> entityKeyIds ) ),
                        allEntitySetIds,
                        propertyTypeIds,
                        true );
            } else {
                return loadEntitySetData(
                        Map.of( entitySetId, entityKeyIds ),
                        Set.of( entitySetId ),
                        propertyTypeIds,
                        false );
            }
        } else {
            throw new ForbiddenException( "Insufficient permissions to read the entity set or it doesn't exists." );
        }
    }

    private EntitySetData<FullQualifiedName> loadEntitySetData(
            Map<UUID, Optional<Set<UUID>>> entityKeyIds,
            Set<UUID> dataEntitySetIds,
            Optional<Set<UUID>> propertyTypeIds,
            Boolean linking ) {
        final Set<UUID> allProperties = authzHelper.getAllPropertiesOnEntitySet( dataEntitySetIds.iterator().next() );
        final Set<UUID> selectedProperties = propertyTypeIds.orElse( allProperties );

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

        final AuditEventType auditEventType;
        final WriteEvent writeEvent;

        switch ( updateType ) {
            case Replace:
                auditEventType = AuditEventType.REPLACE_ENTITIES;
                writeEvent = dgm.replaceEntities( entitySetId, entities, authorizedPropertyTypes );
                break;
            case PartialReplace:
                auditEventType = AuditEventType.PARTIAL_REPLACE_ENTITIES;
                writeEvent = dgm.partialReplaceEntities( entitySetId, entities, authorizedPropertyTypes );
                break;
            case Merge:
                auditEventType = AuditEventType.MERGE_ENTITIES;
                writeEvent = dgm.mergeEntities( entitySetId, entities, authorizedPropertyTypes );
                break;
            default:
                auditEventType = null;
                writeEvent = new WriteEvent( 0, 0 );
                break;
        }

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                auditEventType,
                "Entities updated using update type " + updateType.toString()
                        + " through DataApi.updateEntitiesInEntitySet",
                Optional.of( entities.keySet() ),
                ImmutableMap.of(),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) );

        return writeEvent.getNumUpdates();
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

        WriteEvent writeEvent = dgm.replacePropertiesInEntities( entitySetId,
                entities,
                edmService.getPropertyTypesAsMap( requiredPropertyTypes ) );

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.REPLACE_PROPERTIES_OF_ENTITIES,
                "Entity properties replaced through DataApi.replaceEntityProperties",
                Optional.of( entities.keySet() ),
                ImmutableMap.of(),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) );

        return writeEvent.getNumUpdates();
    }

    @Override
    @PutMapping( value = "/" + ASSOCIATION, consumes = MediaType.APPLICATION_JSON_VALUE )
    public Integer createAssociations( @RequestBody Set<DataEdgeKey> associations ) {
        associations.forEach(
                association -> {
                    UUID associationEntitySetId = association.getEdge().getEntitySetId();

                    //Ensure that we have read access to entity set metadata.
                    ensureReadAccess( new AclKey( association.getSrc().getEntitySetId() ) );
                    ensureReadAccess( new AclKey( association.getDst().getEntitySetId() ) );
                    ensureReadAccess( new AclKey( associationEntitySetId ) );

                    //Ensure that we can read key properties.
                    Set<UUID> keyPropertyTypes = edmService
                            .getEntityTypeByEntitySetId( associationEntitySetId ).getKey();
                    keyPropertyTypes.forEach( propertyType ->
                            accessCheck( new AclKey( associationEntitySetId, propertyType ), READ_PERMISSION ) );
                }
        );

        WriteEvent writeEvent = dgm.createAssociations( associations );

        Stream<Pair<EntityDataKey, Map<String, Object>>> neighborMappingsCreated = associations.stream()
                .flatMap( dataEdgeKey -> Stream.of(
                        Pair.of( dataEdgeKey.getSrc(),
                                ImmutableMap.of( "association",
                                        dataEdgeKey.getEdge(),
                                        "neighbor",
                                        dataEdgeKey.getDst(),
                                        "isSrc",
                                        true ) ),
                        Pair.of( dataEdgeKey.getDst(),
                                ImmutableMap.of( "association",
                                        dataEdgeKey.getEdge(),
                                        "neighbor",
                                        dataEdgeKey.getSrc(),
                                        "isSrc",
                                        false ) ),
                        Pair.of( dataEdgeKey.getEdge(),
                                ImmutableMap.of( "src", dataEdgeKey.getSrc(), "dst", dataEdgeKey.getDst() ) )
                ) );

        recordEvents( neighborMappingsCreated.map( pair -> new AuditableEvent(
                getCurrentUserId(),
                new AclKey( pair.getKey().getEntitySetId() ),
                AuditEventType.ASSOCIATE_ENTITIES,
                "Create associations between entities using DataApi.createAssociations",
                Optional.of( ImmutableSet.of( pair.getKey().getEntityKeyId() ) ),
                pair.getValue(),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) ).collect( Collectors.toList() ) );

        return writeEvent.getNumUpdates();
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
        Pair<List<UUID>, WriteEvent> entityKeyIdsToWriteEvent = dgm
                .createEntities( entitySetId, entities, authorizedPropertyTypes );
        List<UUID> entityKeyIds = entityKeyIdsToWriteEvent.getKey();

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.CREATE_ENTITIES,
                "Entities created through DataApi.createEntities",
                Optional.of( Sets.newHashSet( entityKeyIds ) ),
                ImmutableMap.of(),
                getDateTimeFromLong( entityKeyIdsToWriteEvent.getValue().getVersion() ),
                Optional.empty()
        ) );

        return entityKeyIds;
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

        Map<UUID, CreateAssociationEvent> associationsCreated = dgm
                .createAssociations( associations, authorizedPropertyTypesByEntitySet );

        ListMultimap<UUID, UUID> associationIds = ArrayListMultimap.create();

        UUID currentUserId = getCurrentUserId();

        Stream<AuditableEvent> associationEntitiesCreated = associationsCreated.entrySet().stream().map( entry -> {
            UUID associationEntitySetId = entry.getKey();
            OffsetDateTime writeDateTime = getDateTimeFromLong( entry.getValue().getEntityWriteEvent()
                    .getVersion() );
            associationIds.putAll( associationEntitySetId, entry.getValue().getIds() );

            return new AuditableEvent(
                    currentUserId,
                    new AclKey( associationEntitySetId ),
                    AuditEventType.CREATE_ENTITIES,
                    "Create association entities using DataApi.createAssociations",
                    Optional.of( Sets.newHashSet( entry.getValue().getIds() ) ),
                    ImmutableMap.of(),
                    writeDateTime,
                    Optional.empty()
            );
        } );

        Stream<AuditableEvent> neighborMappingsCreated = associationsCreated
                .entrySet()
                .stream()
                .flatMap( entry -> {
                    UUID associationEntitySetId = entry.getKey();
                    OffsetDateTime writeDateTime = getDateTimeFromLong( entry.getValue().getEdgeWriteEvent()
                            .getVersion() );

                    return Streams.mapWithIndex( entry.getValue().getIds().stream(),
                            ( associationEntityKeyId, index ) -> {

                                EntityDataKey associationEntityDataKey = new EntityDataKey( associationEntitySetId,
                                        associationEntityKeyId );
                                DataEdge dataEdge = associations.get( associationEntitySetId )
                                        .get( Long.valueOf( index ).intValue() );

                                return Stream.<Triple<EntityDataKey, OffsetDateTime, Map<String, Object>>>of(
                                        Triple.of( dataEdge.getSrc(),
                                                writeDateTime,
                                                ImmutableMap.of( "association",
                                                        associationEntityDataKey,
                                                        "neighbor",
                                                        dataEdge.getDst(),
                                                        "isSrc",
                                                        true ) ),
                                        Triple.of( dataEdge.getDst(),
                                                writeDateTime,
                                                ImmutableMap.of( "association",
                                                        associationEntityDataKey,
                                                        "neighbor",
                                                        dataEdge.getSrc(),
                                                        "isSrc",
                                                        false ) ),
                                        Triple.of( associationEntityDataKey,
                                                writeDateTime,
                                                ImmutableMap.of( "src",
                                                        dataEdge.getSrc(),
                                                        "dst",
                                                        dataEdge.getDst() ) ) );
                            } );
                } ).flatMap( Function.identity() ).map( triple -> new AuditableEvent(
                        currentUserId,
                        new AclKey( triple.getLeft().getEntitySetId() ),
                        AuditEventType.ASSOCIATE_ENTITIES,
                        "Create associations between entities using DataApi.createAssociations",
                        Optional.of( ImmutableSet.of( triple.getLeft().getEntityKeyId() ) ),
                        triple.getRight(),
                        triple.getMiddle(),
                        Optional.empty()
                ) );

        recordEvents( Stream.concat( associationEntitiesCreated, neighborMappingsCreated )
                .collect( Collectors.toList() ) );

        return associationIds;
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
                        transformValues( association.getValue(), DataEdge::getData ),
                        authorizedPropertyTypes ).getNumUpdates();
            } else {

                return dgm.replaceEntities( entitySetId,
                        transformValues( association.getValue(), DataEdge::getData ),
                        authorizedPropertyTypes ).getNumUpdates();
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

        /* entity and association creation will be audited by createEntities and createAssociations */

        return new DataGraphIds( entityKeyIds, associationEntityKeyIds );
    }

    @Override
    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ALL },
            method = RequestMethod.DELETE )
    public Integer deleteAllEntitiesFromEntitySet(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestParam( value = TYPE ) DeleteType deleteType) {
        WriteEvent writeEvent;
        // access checks to entity set and property types
        final Map<UUID, PropertyType> authorizedPropertyTypes =
                getAuthorizedPropertyTypesForDelete( entitySetId, Optional.empty(), deleteType );

        if ( deleteType == DeleteType.Hard ) {
            // associations need to be deleted first, because edges are deleted in DataGraphManager.deleteEntitySet call
            deleteAssociations( entitySetId, Optional.empty() );
            writeEvent = dgm.deleteEntitySet( entitySetId, authorizedPropertyTypes );
        } else {
            // associations need to be cleared first, because edges are cleared in DataGraphManager.clearEntitySet call
            clearAssociations( entitySetId, Optional.empty() );
            writeEvent = dgm.clearEntitySet( entitySetId, authorizedPropertyTypes );
        }

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.DELETE_ENTITIES,
                "All entities deleted from entity set using delete type " + deleteType.toString()
                        + " through DataApi.deleteAllEntitiesFromEntitySet",
                Optional.empty(),
                ImmutableMap.of(),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) );

        return writeEvent.getNumUpdates();
    }

    @Override
    @DeleteMapping( path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH } )
    public Integer deleteEntity(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @RequestParam( value = TYPE ) DeleteType deleteType ) {
        return deleteEntities( entitySetId, ImmutableSet.of( entityKeyId ), deleteType );
    }

    @Override
    @DeleteMapping( path = { "/" + ENTITY_SET + "/" + SET_ID_PATH } )
    public Integer deleteEntities(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody Set<UUID> entityKeyIds,
            @RequestParam( value = TYPE ) DeleteType deleteType ) {

        if ( entityKeyIds.size() > MAX_BATCH_SIZE ) {
            throw new IllegalArgumentException( "You can only delete entities in batches of up to " + MAX_BATCH_SIZE + " per request." );
        }

        WriteEvent writeEvent;

        // access checks for entity set and properties
        final Map<UUID, PropertyType> authorizedPropertyTypes =
                getAuthorizedPropertyTypesForDelete( entitySetId, Optional.empty(), deleteType );

        if ( deleteType == DeleteType.Hard ) {
            // associations need to be deleted first, because edges are deleted in DataGraphManager.deleteEntities call
            deleteAssociations( entitySetId, Optional.of( entityKeyIds ) );
            writeEvent = dgm.deleteEntities( entitySetId, entityKeyIds, authorizedPropertyTypes );
        } else {
            // associations need to be cleared first, because edges are cleared in DataGraphManager.clearEntities call
            clearAssociations( entitySetId, Optional.of( entityKeyIds ) );
            writeEvent = dgm.clearEntities( entitySetId, entityKeyIds, authorizedPropertyTypes );
        }

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.DELETE_ENTITIES,
                "Entities deleted using delete type " + deleteType.toString() + " through DataApi.deleteEntities",
                Optional.of( entityKeyIds ),
                ImmutableMap.of(),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) );

        return writeEvent.getNumUpdates();
    }

    @Override
    @DeleteMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH + "/" + PROPERTIES } )
    public Integer deleteEntityProperties(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @RequestBody Set<UUID> propertyTypeIds,
            @RequestParam( value = TYPE ) DeleteType deleteType ) {
        WriteEvent writeEvent;

        // access checks for entity set and properties
        final Map<UUID, PropertyType> authorizedPropertyTypes =
                getAuthorizedPropertyTypesForDelete( entitySetId, Optional.of( propertyTypeIds ), deleteType );

        if ( deleteType == DeleteType.Hard ) {
            writeEvent = dgm
                    .deleteEntityProperties( entitySetId, ImmutableSet.of( entityKeyId ), authorizedPropertyTypes );
        } else {
            writeEvent = dgm
                    .clearEntityProperties( entitySetId, ImmutableSet.of( entityKeyId ), authorizedPropertyTypes );
        }

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.DELETE_PROPERTIES_OF_ENTITIES,
                "Entity properties deleted using delete type " + deleteType.toString()
                        + " through DataApi.deleteEntityProperties",
                Optional.of( ImmutableSet.of( entityKeyId ) ),
                ImmutableMap.of( "propertyTypeIds", propertyTypeIds ),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) );

        return writeEvent.getNumUpdates();
    }

    private List<WriteEvent> clearAssociations( UUID entitySetId, Optional<Set<UUID>> entityKeyIds ) {
        // collect association entity key ids
        final PostgresIterable<EdgeKey> associationsEdgeKeys = collectAssociations( entitySetId, entityKeyIds );

        // access checks
        final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = new HashMap<>();
        associationsEdgeKeys.forEach( edgeKey -> {
                    if ( !authorizedPropertyTypes.containsKey( edgeKey.getEdge().getEntitySetId() ) ) {
                        Map<UUID, PropertyType> authorizedPropertyTypesOfAssociation =
                                getAuthorizedPropertyTypesForDelete(
                                        edgeKey.getEdge().getEntitySetId(), Optional.empty(), DeleteType.Soft );
                        authorizedPropertyTypes.put(
                                edgeKey.getEdge().getEntitySetId(), authorizedPropertyTypesOfAssociation );
                    }
                }
        );

        // clear associations of entity set
        return dgm.clearAssociationsBatch( entitySetId, associationsEdgeKeys, authorizedPropertyTypes );
    }

    private List<WriteEvent> deleteAssociations( UUID entitySetId, Optional<Set<UUID>> entityKeyIds ) {
        // collect association entity key ids
        final PostgresIterable<EdgeKey> associationsEdgeKeys = collectAssociations( entitySetId, entityKeyIds );

        // access checks
        final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = new HashMap<>();
        associationsEdgeKeys.stream().forEach( edgeKey -> {
                    if ( !authorizedPropertyTypes.containsKey( edgeKey.getEdge().getEntitySetId() ) ) {
                        Map<UUID, PropertyType> authorizedPropertyTypesOfAssociation =
                                getAuthorizedPropertyTypesForDelete(
                                        edgeKey.getEdge().getEntitySetId(), Optional.empty(), DeleteType.Hard );
                        authorizedPropertyTypes.put(
                                edgeKey.getEdge().getEntitySetId(), authorizedPropertyTypesOfAssociation );
                    }
                }
        );

        // delete associations of entity set
        return dgm.deleteAssociationsBatch( entitySetId, associationsEdgeKeys, authorizedPropertyTypes );
    }

    private PostgresIterable<EdgeKey> collectAssociations(
            UUID entitySetId, Optional<Set<UUID>> entityKeyIds ) {
        return ( entityKeyIds.isPresent() )
                ? dgm.getEdgesConnectedToEntities( entitySetId, entityKeyIds.get() )
                : dgm.getEdgeKeysOfEntitySet( entitySetId );
    }

    @Timed
    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + NEIGHBORS },
            method = RequestMethod.POST )
    public Long deleteEntitiesAndNeighbors(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody EntityNeighborsFilter filter,
            @RequestParam( value = TYPE ) DeleteType deleteType ) {
        // Note: this function is only useful for deleting src/dst entities and their neighboring entities
        // (along with associations connected to all of them), not associations.
        // If called with an association entity set, it will simplify down to a basic delete call.


        final Set<UUID> entityKeyIds = filter.getEntityKeyIds();

        // we don't include associations in filtering, since they will be deleted anyways with deleting the entities
        final Set<UUID> filteringNeighborEntitySetIds = Stream
                .of( filter.getSrcEntitySetIds().orElse( Set.of() ), filter.getDstEntitySetIds().orElse( Set.of() ) )
                .flatMap( Set::stream )
                .collect( Collectors.toSet() );

        // if no neighbor entity set ids are defined to delete from, it reduces down to a simple deleteEntities call
        if ( filteringNeighborEntitySetIds.isEmpty() ) {
            return deleteEntities( entitySetId, entityKeyIds, deleteType ).longValue();
        }


        /*
         * 1 - collect all relevant EntitySets and check permissions against them and for all their PropertyTypes
         */

        // getNeighborEntitySetIds() returns source, destination, and edge EntitySet ids
        final Set<UUID> neighborEntitySetIds = dgm
                .getNeighborEntitySetIds( ImmutableSet.of( entitySetId ) )
                .stream()
                .filter( filteringNeighborEntitySetIds::contains )
                .collect( Collectors.toSet() );

        final Set<UUID> allEntitySetIds = ImmutableSet.<UUID>builder()
                .add( entitySetId )
                .addAll( neighborEntitySetIds )
                .build();

        Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesOfEntitySets = allEntitySetIds
                .stream()
                .collect( Collectors.toMap(
                        Function.identity(),
                        esId -> getAuthorizedPropertyTypesForDelete(esId, Optional.empty(), deleteType) ));

        /*
         * 2 - collect all neighbor entities, organized by EntitySet
         */

        Map<UUID, Set<EntityDataKey>> entitySetIdToEntityDataKeysMap = dgm
                .getEdgesConnectedToEntities( entitySetId, entityKeyIds )
                .stream()
                .filter( edge ->
                        ( edge.getDst().getEntitySetId().equals( entitySetId )
                                && filteringNeighborEntitySetIds.contains( edge.getSrc().getEntitySetId() ) )
                                || ( edge.getSrc().getEntitySetId().equals( entitySetId )
                                && filteringNeighborEntitySetIds.contains( edge.getDst().getEntitySetId() ) ) )
                .flatMap( edge -> Stream.of( edge.getSrc(), edge.getDst() ) )
                .collect( Collectors.groupingBy( EntityDataKey::getEntitySetId, Collectors.toSet() ) );

        /*
         * 3 - delete all entities
         */

        /* Delete entity */

        long numUpdates = 0;

        WriteEvent writeEvent;
        if(deleteType == DeleteType.Hard) {
            deleteAssociations( entitySetId, Optional.of( entityKeyIds ) );
            writeEvent = dgm.deleteEntities(
                    entitySetId,
                    entityKeyIds,
                    authorizedPropertyTypesOfEntitySets.get( entitySetId ) );
        } else {
            clearAssociations( entitySetId, Optional.of( entityKeyIds ) );
            writeEvent = dgm.clearEntities(
                    entitySetId,
                    entityKeyIds,
                    authorizedPropertyTypesOfEntitySets.get( entitySetId ) );
        }

        numUpdates += writeEvent.getNumUpdates();

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.DELETE_ENTITY_AND_NEIGHBORHOOD,
                "Entities and all neighbors deleted using delete type " + deleteType.toString() +
                        " through DataApi.clearEntityAndNeighborEntities",
                Optional.of( entityKeyIds ),
                ImmutableMap.of(),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) );

        /* Delete neighbors */

        List<AuditableEvent> neighborDeleteEvents = Lists.newArrayList();

        numUpdates += entitySetIdToEntityDataKeysMap.entrySet().stream().mapToInt( entry -> {
                    final UUID neighborEntitySetId = entry.getKey();
                    final Set<EntityDataKey> neighborEntityDataKeys = entry.getValue();

                    WriteEvent neighborWriteEvent;

                    if ( deleteType == DeleteType.Hard ) {
                        deleteAssociations( neighborEntitySetId, Optional.of( entityKeyIds ) );
                        neighborWriteEvent = dgm.deleteEntities(
                                neighborEntitySetId,
                                neighborEntityDataKeys.stream().map( EntityDataKey::getEntityKeyId ).collect( Collectors.toSet() ),
                                authorizedPropertyTypesOfEntitySets.get( neighborEntitySetId ) );
                    } else {
                        clearAssociations( entitySetId, Optional.of( entityKeyIds ) );
                        neighborWriteEvent = dgm.clearEntities(
                                neighborEntitySetId,
                                neighborEntityDataKeys.stream().map( EntityDataKey::getEntityKeyId ).collect( Collectors.toSet() ),
                                authorizedPropertyTypesOfEntitySets.get( neighborEntitySetId ) );
                    }

                    neighborDeleteEvents.add( new AuditableEvent(
                            getCurrentUserId(),
                            new AclKey( neighborEntitySetId ),
                            AuditEventType.DELETE_ENTITY_AS_PART_OF_NEIGHBORHOOD,
                            "Entity deleted using delete type " + deleteType.toString() + " as part of " +
                                    "neighborhood delete through DataApi.clearEntityAndNeighborEntities",
                            Optional.of( neighborEntityDataKeys.stream().map( EntityDataKey::getEntityKeyId )
                                    .collect( Collectors.toSet() ) ),
                            ImmutableMap.of(),
                            getDateTimeFromLong( neighborWriteEvent.getVersion() ),
                            Optional.empty()
                    ) );

                   return neighborWriteEvent.getNumUpdates();
                }
        ).sum();

        recordEvents( neighborDeleteEvents );

        return numUpdates;

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
                edmService.getPropertyTypesAsMap( entity.keySet() ),
                Principals.getCurrentPrincipals() );

        WriteEvent writeEvent = dgm
                .replaceEntities( entitySetId, ImmutableMap.of( entityKeyId, entity ), authorizedPropertyTypes );

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.REPLACE_ENTITIES,
                "Entity replaced through DataApi.replaceEntityInEntitySet",
                Optional.of( ImmutableSet.of( entityKeyId ) ),
                ImmutableMap.of(),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) );

        return writeEvent.getNumUpdates();
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

    private Map<UUID, PropertyType> getAuthorizedPropertyTypesForDelete(
            UUID entitySetId,
            Optional<Set<UUID>> properties,
            DeleteType deleteType ) {

        EnumSet<Permission> propertyPermissionsToCheck;
        if ( deleteType == DeleteType.Hard ) {
            ensureOwnerAccess( new AclKey( entitySetId ) );
            propertyPermissionsToCheck = EnumSet.of( Permission.OWNER );
        } else {
            ensureReadAccess( new AclKey( entitySetId ) );
            propertyPermissionsToCheck = EnumSet.of( Permission.WRITE );
        }

        final EntitySet entitySet = edmService.getEntitySet( entitySetId );
        if ( entitySet.isLinking() ) {
            throw new IllegalArgumentException( "You cannot delete entities from a linking entity set." );
        }

        final EntityType entityType = edmService.getEntityType( entitySet.getEntityTypeId() );
        final Set<UUID> requiredProperties = properties.orElse( entityType.getProperties() );
        final Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes( ImmutableSet.of( entitySetId ),
                        requiredProperties,
                        propertyPermissionsToCheck )
                .get( entitySetId );
        if ( !authorizedPropertyTypes.keySet().containsAll( requiredProperties ) ) {
            throw new ForbiddenException(
                    "You must have " + propertyPermissionsToCheck.iterator().next() + " permission of all required " +
                            "entity set properties to delete entities from it." );
        }

        return authorizedPropertyTypes;
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

    private UUID getCurrentUserId() {
        return spm.getPrincipal( Principals.getCurrentUser().getId() ).getId();
    }

    private static OffsetDateTime getDateTimeFromLong( long epochTime ) {
        return OffsetDateTime.ofInstant( Instant.ofEpochMilli( epochTime ), ZoneId.systemDefault() );
    }

    @NotNull
    @Override
    public AuditRecordEntitySetsManager getAuditRecordEntitySetsManager() {
        return auditRecordEntitySetsManager;
    }

    @NotNull
    @Override
    public DataGraphManager getDataGraphService() {
        return dgm;
    }

}
