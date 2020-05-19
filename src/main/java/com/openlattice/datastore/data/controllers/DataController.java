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
import com.openlattice.auditing.*;
import com.openlattice.authorization.*;
import com.openlattice.controllers.exceptions.BadRequestException;
import com.openlattice.controllers.exceptions.ForbiddenException;
import com.openlattice.data.*;
import com.openlattice.data.graph.DataGraphServiceHelper;
import com.openlattice.data.requests.EntitySetSelection;
import com.openlattice.data.requests.FileType;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.EntitySetManager;
import com.openlattice.datastore.services.SyncTicketService;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.set.EntitySetFlag;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.search.requests.EntityNeighborsFilter;
import com.openlattice.web.mediatypes.CustomMediaType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.transformValues;
import static com.openlattice.authorization.EdmAuthorizationHelper.*;

@SuppressFBWarnings(
        value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
        justification = "NPEs are prevented by Preconditions.checkState but SpotBugs doesn't understand this" )
@RestController
@RequestMapping( DataApi.CONTROLLER )
public class DataController implements DataApi, AuthorizingComponent, AuditingComponent {
    private static final Logger logger = LoggerFactory.getLogger( DataController.class );

    @Inject
    private SyncTicketService sts;

    @Inject
    private EntitySetManager entitySetService;

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
    private AuditingManager auditingManager;

    @Inject
    private SecurePrincipalsManager spm;

    @Inject
    private DataGraphServiceHelper dataGraphServiceHelper;

    @Inject
    private DataDeletionManager deletionManager;

    private LoadingCache<UUID, EdmPrimitiveTypeKind> primitiveTypeKinds;

    private LoadingCache<AuthorizationKey, Set<UUID>> authorizedPropertyCache;

    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH },
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    @Timed
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
    @Timed
    public EntitySetData<FullQualifiedName> loadEntitySetData(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody( required = false ) EntitySetSelection selection,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) {
        setContentDisposition( response, entitySetId.toString(), fileType );
        setDownloadContentType( response, fileType );
        return loadSelectedEntitySetData( entitySetId, selection, fileType );
    }

    @Override
    public EntitySetData<FullQualifiedName> loadSelectedEntitySetData(
            UUID entitySetId,
            EntitySetSelection selection,
            FileType fileType ) {
        return loadEntitySetData( entitySetId, selection );
    }

    private EntitySetData<FullQualifiedName> loadEntitySetData(
            UUID entitySetId,
            EntitySetSelection selection ) {
        if ( !authz.checkIfHasPermissions(
                new AclKey( entitySetId ), Principals.getCurrentPrincipals(), READ_PERMISSION ) ) {
            throw new ForbiddenException( "Insufficient permissions to read the entity set " + entitySetId
                    + " or it doesn't exists." );
        }
        final var entitySet = entitySetService.getEntitySet( entitySetId );
        checkState( entitySet != null, "Could not find entity set with id: %s", entitySetId );

        Optional<Set<UUID>> entityKeyIds = ( selection == null ) ? Optional.empty() : selection.getEntityKeyIds();
        final var selectedProperties = getSelectedProperties( entitySetId, selection );

        final var normalEntitySetIds = ( entitySet.isLinking() )
                ? Sets.newHashSet( entitySet.getLinkedEntitySets() )
                : Set.of( entitySetId );

        final Map<UUID, Optional<Set<UUID>>> entityKeyIdsOfEntitySets = normalEntitySetIds.stream()
                .collect( Collectors.toMap( esId -> esId, esId -> entityKeyIds ) );
        final var authorizedPropertyTypesOfEntitySets = getAuthorizedPropertyTypesForEntitySetRead(
                entitySet, normalEntitySetIds, selectedProperties
        );

        final var authorizedPropertyTypes = authorizedPropertyTypesOfEntitySets.values().iterator().next();
        final LinkedHashSet<String> orderedPropertyNames = new LinkedHashSet<>( authorizedPropertyTypes.size() );
        selectedProperties.stream()
                .filter( authorizedPropertyTypes::containsKey )
                .map( authorizedPropertyTypes::get )
                .map( pt -> pt.getType().getFullQualifiedNameAsString() )
                .forEach( orderedPropertyNames::add );

        return dgm.getEntitySetData(
                entityKeyIdsOfEntitySets,
                orderedPropertyNames,
                authorizedPropertyTypesOfEntitySets,
                entitySet.isLinking() );
    }

    private Set<UUID> getSelectedProperties( UUID entitySetId, EntitySetSelection selection ) {
        Optional<Set<UUID>> propertyTypeIds = ( selection == null ) ? Optional.empty() : selection.getProperties();
        final Set<UUID> allProperties = authzHelper.getAllPropertiesOnEntitySet( entitySetId );
        final Set<UUID> selectedProperties = propertyTypeIds.orElse( allProperties );
        checkState( allProperties.equals( selectedProperties ) || allProperties.containsAll( selectedProperties ),
                "Selected properties are not property types of entity set %s", entitySetId );

        return selectedProperties;
    }

    private Map<UUID, Map<UUID, PropertyType>> getAuthorizedPropertyTypesForEntitySetRead(
            EntitySet entitySet,
            Set<UUID> normalEntitySetIds,
            Set<UUID> selectedProperties
    ) {
        Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesOfEntitySets;

        if ( entitySet.isLinking() ) {
            checkState( !normalEntitySetIds.isEmpty(),
                    "Linked entity sets are empty for linking entity set %s", entitySet.getId() );
            normalEntitySetIds.forEach( esId -> ensureReadAccess( new AclKey( esId ) ) );

            authorizedPropertyTypesOfEntitySets = authzHelper
                    .getAuthorizedPropertyTypesByNormalEntitySet( entitySet, selectedProperties, READ_PERMISSION );

        } else {
            authorizedPropertyTypesOfEntitySets = authzHelper
                    .getAuthorizedPropertyTypes( normalEntitySetIds, selectedProperties, READ_PERMISSION );
        }

        return authorizedPropertyTypesOfEntitySets;
    }

    @Override
    @PutMapping(
            value = "/" + ENTITY_SET + "/" + SET_ID_PATH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @Timed
    public Integer updateEntitiesInEntitySet(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody Map<UUID, Map<UUID, Set<Object>>> entities,
            @RequestParam( value = TYPE, defaultValue = "Merge" ) UpdateType updateType ) {
        Preconditions.checkNotNull( updateType, "An invalid update type value was specified." );
        ensureReadAccess( new AclKey( entitySetId ) );
        ensureEntitySetCanBeWritten( entitySetId );
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
                throw new BadRequestException( "Unsupported UpdateType: \"" + updateType + "\'" );
        }

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
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
    @Timed
    public Integer replaceEntityProperties(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody Map<UUID, Map<UUID, Set<Map<ByteBuffer, Object>>>> entities ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        ensureEntitySetCanBeWritten( entitySetId );

        final Set<UUID> requiredPropertyTypes = requiredReplacementPropertyTypes( entities );
        accessCheck( aclKeysForAccessCheck( ImmutableSetMultimap.<UUID, UUID>builder()
                        .putAll( entitySetId, requiredPropertyTypes ).build(),
                WRITE_PERMISSION ) );

        WriteEvent writeEvent = dgm.replacePropertiesInEntities(
                entitySetId,
                entities,
                edmService.getPropertyTypesAsMap( requiredPropertyTypes )
        );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
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
    @Timed
    @PutMapping( value = "/" + ASSOCIATION, consumes = MediaType.APPLICATION_JSON_VALUE )
    public Integer createEdges( @RequestBody Set<DataEdgeKey> associations ) {

        Set<UUID> entitySetIds = getEntitySetIdsFromCollection( associations, this::streamEntitySetIds );
        checkPermissionsOnEntitySetIds( entitySetIds, EnumSet.of( Permission.READ, Permission.WRITE ) );

        //Allowed entity types check
        dataGraphServiceHelper.checkEdgeEntityTypes( associations );

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
                spm.getCurrentUserId(),
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
        ensureEntitySetCanBeWritten( entitySetId );
        final var requiredPropertyTypes = entities.stream()
                .flatMap(  entity ->  entity.keySet().stream() )
                .collect( Collectors.toSet() );
        //Load authorized property types
        final Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes( entitySetId, WRITE_PERMISSION );
        accessCheck( authorizedPropertyTypes, requiredPropertyTypes );
        Pair<List<UUID>, WriteEvent> entityKeyIdsToWriteEvent = dgm
                .createEntities( entitySetId, entities, authorizedPropertyTypes );
        List<UUID> entityKeyIds = entityKeyIdsToWriteEvent.getKey();

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
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

    @Timed
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
        Set<UUID> entitySetIds = getEntitySetIdsFromCollection( associations.values(), this::streamEntitySetIds );
        checkPermissionsOnEntitySetIds( entitySetIds, READ_PERMISSION );

        //Ensure that we can write properties.
        final SetMultimap<UUID, UUID> requiredPropertyTypes = requiredAssociationPropertyTypes( associations );
        accessCheck( aclKeysForAccessCheck( requiredPropertyTypes, WRITE_PERMISSION ) );

        final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySet = authzHelper
                .getAuthorizedPropertiesOnEntitySets( associations.keySet(), WRITE_PERMISSION );

        dataGraphServiceHelper.checkAssociationEntityTypes( associations );
        Map<UUID, CreateAssociationEvent> associationsCreated = dgm
                .createAssociations( associations, authorizedPropertyTypesByEntitySet );

        ListMultimap<UUID, UUID> associationIds = ArrayListMultimap.create();

        UUID currentUserId = spm.getCurrentUserId();

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
        ensureEntitySetsCanBeWritten( requiredPropertyTypes.keySet() );
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

        Set<UUID> entitySetIds = getEntitySetIdsFromCollection( data.getAssociations().values(),
                this::streamEntitySetIds );
        checkPermissionsOnEntitySetIds( entitySetIds, READ_PERMISSION );

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

    @Timed
    @Override
    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ALL },
            method = RequestMethod.DELETE )
    public Integer deleteAllEntitiesFromEntitySet(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestParam( value = TYPE ) DeleteType deleteType ) {
        ensureEntitySetCanBeWritten( entitySetId );

        WriteEvent writeEvent = deletionManager
                .clearOrDeleteEntitySetIfAuthorized( entitySetId, deleteType, Principals.getCurrentPrincipals() );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
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

    @Timed
    @Override
    @DeleteMapping( path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH } )
    public Integer deleteEntity(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @RequestParam( value = TYPE ) DeleteType deleteType ) {
        return deleteEntities( entitySetId, ImmutableSet.of( entityKeyId ), deleteType );
    }

    @Timed
    @Override
    @DeleteMapping( path = { "/" + ENTITY_SET + "/" + SET_ID_PATH } )
    public Integer deleteEntities(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody Set<UUID> entityKeyIds,
            @RequestParam( value = TYPE ) DeleteType deleteType ) {

        ensureEntitySetCanBeWritten( entitySetId );

        WriteEvent writeEvent = deletionManager
                .clearOrDeleteEntitiesIfAuthorized( entitySetId,
                        entityKeyIds,
                        deleteType,
                        Principals.getCurrentPrincipals() );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
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

    @Timed
    @Override
    @DeleteMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH + "/" + PROPERTIES } )
    public Integer deleteEntityProperties(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @RequestBody Set<UUID> propertyTypeIds,
            @RequestParam( value = TYPE ) DeleteType deleteType ) {

        ensureEntitySetCanBeWritten( entitySetId );

        WriteEvent writeEvent = deletionManager.clearOrDeleteEntityProperties( entitySetId,
                ImmutableSet.of( entityKeyId ),
                deleteType,
                propertyTypeIds,
                Principals.getCurrentPrincipals() );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
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

        ensureEntitySetCanBeWritten( entitySetId );

        WriteEvent writeEvent = deletionManager.clearOrDeleteEntitiesAndNeighborsIfAuthorized(
                entitySetId,
                filter.getEntityKeyIds(),
                filter.getSrcEntitySetIds().orElse( ImmutableSet.of() ),
                filter.getDstEntitySetIds().orElse( ImmutableSet.of() ),
                deleteType,
                Principals.getCurrentPrincipals()
        );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.DELETE_ENTITY_AND_NEIGHBORHOOD,
                "Entities and all neighbors deleted using delete type " + deleteType.toString() +
                        " through DataApi.clearEntityAndNeighborEntities",
                Optional.of( filter.getEntityKeyIds() ),
                ImmutableMap.of(),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) );

        return (long) writeEvent.getNumUpdates();

    }

    @Timed
    @Override
    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH },
            method = RequestMethod.PUT )
    public Integer replaceEntityInEntitySet(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @RequestBody Map<UUID, Set<Object>> entity ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        ensureEntitySetCanBeWritten( entitySetId );

        Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper.getAuthorizedPropertyTypes( entitySetId,
                WRITE_PERMISSION,
                edmService.getPropertyTypesAsMap( entity.keySet() ),
                Principals.getCurrentPrincipals() );

        WriteEvent writeEvent = dgm
                .replaceEntities( entitySetId, ImmutableMap.of( entityKeyId, entity ), authorizedPropertyTypes );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
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

    @Timed
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

    @Timed
    @Override
    @RequestMapping(
            path = { "/" + SET_ID_PATH + "/" + COUNT },
            method = RequestMethod.GET )
    public long getEntitySetSize( @PathVariable( ENTITY_SET_ID ) UUID entitySetId ) {
        ensureReadAccess( new AclKey( entitySetId ) );

        // If entityset is linking: should return distinct count of entities corresponding to the linking entity set,
        // which is the distinct count of linking_id s
        return dgm.getEntitySetSize( entitySetId );
    }

    @Timed
    @Override
    @RequestMapping(
            path = { "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH },
            method = RequestMethod.GET )
    public Map<FullQualifiedName, Set<Object>> getEntity(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        EntitySet entitySet = entitySetService.getEntitySet( entitySetId );

        checkState( entitySet != null, "Could not find entity set with id: " + entitySetId.toString() );

        if ( entitySet.isLinking() ) {
            checkState( !entitySet.getLinkedEntitySets().isEmpty(),
                    "Linked entity sets are empty for linking entity set %s", entitySetId );
            entitySet.getLinkedEntitySets().forEach( esId -> ensureReadAccess( new AclKey( esId ) ) );

            final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = authzHelper
                    .getAuthorizedPropertiesByNormalEntitySets( entitySet, EnumSet.of( Permission.READ ) );

            return dgm.getLinkingEntity( entitySet.getLinkedEntitySets(), entityKeyId, authorizedPropertyTypes );
        } else {
            final Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper
                    .getAuthorizedPropertyTypes( entitySetId, READ_PERMISSION );
            return dgm.getEntity( entitySetId, entityKeyId, authorizedPropertyTypes );
        }
    }

    @Timed
    @Override
    @GetMapping(
            path = "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH + "/" + PROPERTY_TYPE_ID_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Set<Object> getEntityPropertyValues(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @PathVariable( PROPERTY_TYPE_ID ) UUID propertyTypeId ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        final EntitySet entitySet = entitySetService.getEntitySet( entitySetId );

        checkState( entitySet != null, "Could not find entity set with id: " + entitySetId.toString() );

        if ( entitySet.isLinking() ) {
            checkState( !entitySet.getLinkedEntitySets().isEmpty(),
                    "Linked entity sets are empty for linking entity set %s", entitySetId );

            entitySet.getLinkedEntitySets().forEach( esId -> ensureReadAccess( new AclKey( esId ) ) );

            final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = authzHelper
                    .getAuthorizedPropertyTypesByNormalEntitySet(
                            entitySet,
                            Set.of( propertyTypeId ),
                            EnumSet.of( Permission.READ ) );

            // if any of its normal entitysets don't have read permission on property type, reading is not allowed
            if ( authorizedPropertyTypes.values().iterator().next().isEmpty() ) {
                throw new ForbiddenException( "Not authorized to read property type " + propertyTypeId
                        + " in one or more normal entity sets of linking entity set " + entitySetId );
            }

            final var propertyTypeFqn = authorizedPropertyTypes.values().iterator().next().get( propertyTypeId )
                    .getType();

            return dgm.getLinkingEntity(
                    entitySet.getLinkedEntitySets(),
                    entityKeyId,
                    authorizedPropertyTypes )
                    .get( propertyTypeFqn );
        } else {
            ensureReadAccess( new AclKey( entitySetId, propertyTypeId ) );
            final Map<UUID, PropertyType> authorizedPropertyTypes = edmService
                    .getPropertyTypesAsMap( ImmutableSet.of( propertyTypeId ) );

            final var propertyTypeFqn = authorizedPropertyTypes.get( propertyTypeId ).getType();

            return dgm.getEntity( entitySetId, entityKeyId, authorizedPropertyTypes )
                    .get( propertyTypeFqn );
        }
    }

    @Timed
    @Override
    @PostMapping(
            path = "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + DETAILED,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public Map<UUID, Map<UUID, Map<UUID, Map<FullQualifiedName, Set<Object>>>>> loadLinkedEntitySetBreakdown(
            @PathVariable( ENTITY_SET_ID ) UUID linkedEntitySetId,
            @RequestBody EntitySetSelection selection ) {
        ensureReadAccess( new AclKey( linkedEntitySetId ) );

        final var entitySet = entitySetService.getEntitySet( linkedEntitySetId );
        checkState(
                entitySet != null, "Could not find entity set with id: %s", linkedEntitySetId
        );

        final var selectedProperties = getSelectedProperties( linkedEntitySetId, selection );
        final var normalEntitySetIds = Sets.newHashSet( entitySet.getLinkedEntitySets() );
        final var authorizedPropertyTypesOfEntitySets = getAuthorizedPropertyTypesForEntitySetRead(
                entitySet, normalEntitySetIds, selectedProperties
        );

        Optional<Set<UUID>> entityKeyIds = ( selection == null ) ? Optional.empty() : selection.getEntityKeyIds();
        final var entityKeyIdsOfEntitySets = normalEntitySetIds.stream()
                .collect( Collectors.toMap( esId -> esId, esId -> entityKeyIds ) );

        return dgm.getLinkedEntitySetBreakDown( entityKeyIdsOfEntitySets, authorizedPropertyTypesOfEntitySets );
    }

    @NotNull
    @Override
    public AuditingManager getAuditingManager() {
        return auditingManager;
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

    private static Set<UUID> requiredReplacementPropertyTypes( Map<UUID, Map<UUID, Set<Map<ByteBuffer, Object>>>> entities ) {
        return entities.values().stream().flatMap( m -> m.keySet().stream() ).collect( Collectors.toSet() );
    }

    private static OffsetDateTime getDateTimeFromLong( long epochTime ) {
        return OffsetDateTime.ofInstant( Instant.ofEpochMilli( epochTime ), ZoneId.systemDefault() );
    }

    private Stream<UUID> streamEntitySetIds( DataAssociation association ) {
        return Stream.of( association.getSrcEntitySetId(), association.getDstEntitySetId() );
    }

    private Stream<UUID> streamEntitySetIds( DataEdge dataEdge ) {
        return Stream.of( dataEdge.getSrc().getEntitySetId(), dataEdge.getDst().getEntitySetId() );
    }

    private Stream<UUID> streamEntitySetIds( DataEdgeKey dataEdgeKey ) {
        return Stream.of( dataEdgeKey.getEdge().getEntitySetId(),
                dataEdgeKey.getSrc().getEntitySetId(),
                dataEdgeKey.getDst().getEntitySetId() );
    }

    private <T> Set<UUID> getEntitySetIdsFromCollection(
            Collection<T> items,
            Function<T, Stream<UUID>> transformation ) {
        return items.stream().flatMap( transformation ).collect( Collectors.toSet() );
    }

    private void checkPermissionsOnEntitySetIds( Set<UUID> entitySetIds, EnumSet<Permission> permissions ) {
        //Ensure that we have write access to entity sets.
        ensureEntitySetsCanBeWritten( entitySetIds );
        accessCheck( entitySetIds.stream().collect( Collectors.toMap( AclKey::new, id -> permissions ) ) );
    }

    private void ensureEntitySetCanBeWritten( UUID entitySetId ) {
        ensureEntitySetsCanBeWritten( ImmutableSet.of( entitySetId ) );
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

}
