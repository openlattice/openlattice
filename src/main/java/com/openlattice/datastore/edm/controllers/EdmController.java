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

package com.openlattice.datastore.edm.controllers;

import com.auth0.spring.security.api.authentication.PreAuthenticatedAuthenticationJsonWebToken;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.openlattice.auditing.AuditEventType;
import com.openlattice.auditing.AuditableEvent;
import com.openlattice.auditing.AuditingComponent;
import com.openlattice.auditing.AuditingManager;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.controllers.exceptions.BadRequestException;
import com.openlattice.data.PropertyUsageSummary;
import com.openlattice.data.requests.FileType;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.EntitySetManager;
import com.openlattice.edm.*;
import com.openlattice.edm.requests.EdmDetailsSelector;
import com.openlattice.edm.requests.EdmRequest;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.edm.type.AssociationDetails;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.EntityTypePropertyMetadata;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.web.mediatypes.CustomMediaType;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping( EdmApi.CONTROLLER )
public class EdmController implements EdmApi, AuthorizingComponent, AuditingComponent {

    @Inject
    private EdmManager modelService;

    @Inject
    private EntitySetManager entitySetManager;

    @Inject
    private HazelcastSchemaManager schemaManager;

    @Inject
    private AuthorizationManager authorizations;

    @Inject
    private PostgresEdmManager postgresEdmManager;

    @Inject
    private AuthenticationManager authenticationManager;

    @Inject
    private SecurePrincipalsManager spm;

    @Inject
    private AuditingManager auditingManager;

    @Timed
    @RequestMapping(
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_YAML_VALUE } )
    @ResponseStatus( HttpStatus.OK )
    public EntityDataModel getEntityDataModel(
            @RequestParam(
                    value = FILE_TYPE,
                    required = false ) FileType fileType,
            HttpServletResponse response ) {
        setContentDisposition( response, "EntityDataModel", fileType );
        setDownloadContentType( response, fileType );
        return getEntityDataModel();
    }

    @Override
    public EntityDataModel getEntityDataModel() {
        return modelService.getEntityDataModel();
    }

    @Timed
    @RequestMapping(
            path = DIFF_PATH,
            method = RequestMethod.POST,
            consumes = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_YAML_VALUE },
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_YAML_VALUE } )
    @ResponseStatus( HttpStatus.OK )
    public EntityDataModelDiff getEntityDataModelDiff(
            @RequestParam(
                    value = FILE_TYPE,
                    required = false ) FileType fileType,
            @RequestBody EntityDataModel edm,
            HttpServletResponse response ) {
        setContentDisposition( response, "EntityDataModel", fileType );
        setDownloadContentType( response, fileType );
        return getEntityDataModelDiff( edm );
    }

    @Override
    public EntityDataModelDiff getEntityDataModelDiff( EntityDataModel edm ) {
        return modelService.getEntityDataModelDiff( edm );
    }

    @Timed
    @Override
    @RequestMapping(
            method = RequestMethod.PATCH,
            consumes = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_YAML_VALUE } )
    @ResponseStatus( HttpStatus.OK )
    public Void updateEntityDataModel( @RequestBody EntityDataModel edm ) {
        ensureAdminAccess();
        modelService.setEntityDataModel( edm );
        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public EdmDetails getEdmDetails( @RequestBody Set<EdmDetailsSelector> selectors ) {
        final Set<UUID> propertyTypeIds = new HashSet<>();
        final Set<UUID> entityTypeIds = new HashSet<>();
        final Set<UUID> entitySetIds = new HashSet<>();

        selectors.forEach( selector -> {
            switch ( selector.getType() ) {
                case PropertyTypeInEntitySet:
                    updatePropertyTypeIdsToGet( selector, propertyTypeIds );
                    break;
                case EntityType:
                    updateEntityTypeIdsToGet( selector, propertyTypeIds, entityTypeIds );
                    break;
                case EntitySet:
                    updateEntitySetIdsToGet( selector, propertyTypeIds, entityTypeIds, entitySetIds );
                    break;
                default:
                    throw new BadRequestException(
                            "Unsupported Securable Object Type when retrieving Edm Details: " + selector.getType() );
            }
        } );

        accessCheck( entitySetIds.stream()
                .collect( Collectors.toMap( AclKey::new, id -> EnumSet.of( Permission.READ ) ) ) );

        return new EdmDetails(
                modelService.getPropertyTypesAsMap( propertyTypeIds ),
                modelService.getEntityTypesAsMap( entityTypeIds ),
                entitySetManager.getEntitySetsAsMap( entitySetIds ) );
    }

    private void updatePropertyTypeIdsToGet( EdmDetailsSelector selector, Set<UUID> propertyTypeIds ) {
        if ( selector.getIncludedFields().contains( SecurableObjectType.PropertyTypeInEntitySet ) ) {
            propertyTypeIds.add( selector.getId() );
        }
    }

    private void updateEntityTypeIdsToGet(
            EdmDetailsSelector selector,
            Set<UUID> propertyTypeIds,
            Set<UUID> entityTypeIds ) {
        if ( selector.getIncludedFields().contains( SecurableObjectType.EntityType ) ) {
            entityTypeIds.add( selector.getId() );
        }
        if ( selector.getIncludedFields().contains( SecurableObjectType.PropertyTypeInEntitySet ) ) {
            EntityType et = modelService.getEntityType( selector.getId() );
            if ( et != null ) {
                propertyTypeIds.addAll( et.getProperties() );
            }
        }
    }

    private void updateEntitySetIdsToGet(
            EdmDetailsSelector selector,
            Set<UUID> propertyTypeIds,
            Set<UUID> entityTypeIds,
            Set<UUID> entitySetIds ) {
        boolean setRetrieved = false;
        EntitySet es = null;
        if ( selector.getIncludedFields().contains( SecurableObjectType.EntitySet ) ) {
            entitySetIds.add( selector.getId() );
        }
        if ( selector.getIncludedFields().contains( SecurableObjectType.EntityType ) ) {
            // TODO should non-existing es id be allowed?
            es = entitySetManager.getEntitySet( selector.getId() );
            setRetrieved = true;
            if ( es != null ) {
                entityTypeIds.add( es.getEntityTypeId() );
            }
        }
        if ( selector.getIncludedFields().contains( SecurableObjectType.PropertyTypeInEntitySet ) ) {
            if ( !setRetrieved ) {
                es = entitySetManager.getEntitySet( selector.getId() );
            }
            if ( es != null ) {
                EntityType et = modelService.getEntityType( es.getEntityTypeId() );
                if ( et != null ) {
                    propertyTypeIds.addAll( et.getProperties() );
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see com.openlattice.datastore.edm.controllers.EdmAPI#getSchemas()
     */
    @Timed
    @Override
    @RequestMapping(
            path = SCHEMA_PATH,
            method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Schema> getSchemas() {
        return schemaManager.getAllSchemas();
    }

    @Timed
    @Override
    @RequestMapping(
            path = SCHEMA_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Schema getSchemaContents(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name ) {
        return schemaManager.getSchema( namespace, name );
    }

    @Timed
    @RequestMapping(
            path = SCHEMA_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_YAML_VALUE } )
    public Schema getSchemaContentsFormatted(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @RequestParam( value = FILE_TYPE ) FileType fileType,
            @RequestParam( value = TOKEN, required = false ) String token,
            HttpServletResponse response ) {
        setContentDisposition( response, namespace + "." + name, fileType );
        setDownloadContentType( response, fileType );

        return getSchemaContentsFormatted( namespace, name, fileType, token );
    }

    @Override
    public Schema getSchemaContentsFormatted(
            String namespace,
            String name,
            FileType fileType,
            String token ) {
        if ( StringUtils.isNotBlank( token ) ) {
            Authentication authentication = authenticationManager
                    .authenticate( PreAuthenticatedAuthenticationJsonWebToken.usingToken( token ) );
            SecurityContextHolder.getContext().setAuthentication( authentication );
        }
        return schemaManager.getSchema( namespace, name );
    }

    @Timed
    @Override
    @RequestMapping(
            path = SCHEMA_PATH + NAMESPACE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Schema> getSchemasInNamespace( @PathVariable( NAMESPACE ) String namespace ) {
        return schemaManager.getSchemasInNamespace( namespace );
    }

    @Override
    @RequestMapping(
            path = SCHEMA_PATH,
            method = RequestMethod.POST )
    @ResponseStatus( HttpStatus.OK )
    public Void createSchemaIfNotExists( @RequestBody Schema schema ) {
        ensureAdminAccess();
        schemaManager.createOrUpdateSchemas( schema );
        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = SCHEMA_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.PUT )
    @ResponseStatus( HttpStatus.OK )
    public Void createEmptySchema( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        ensureAdminAccess();
        schemaManager.upsertSchemas( ImmutableSet.of( new FullQualifiedName( namespace, name ) ) );
        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = SUMMARY_PATH,
            method = RequestMethod.GET )
    public Map<UUID, Iterable<PropertyUsageSummary>> getAllPropertyUsageSummaries() {
        ensureAdminAccess();
        Set<UUID> propertyTypeIds = modelService.getAllPropertyTypeIds();
        Map<UUID, Iterable<PropertyUsageSummary>> allPropertySummaries = Maps
                .newHashMapWithExpectedSize( propertyTypeIds.size() );
        for ( UUID propertyTypeId : propertyTypeIds ) {
            allPropertySummaries.put( propertyTypeId, postgresEdmManager.getPropertyUsageSummary( propertyTypeId ) );
        }
        return allPropertySummaries;
    }

    @Timed
    @Override
    @RequestMapping(
            path = SUMMARY_PATH + ID_PATH,
            method = RequestMethod.GET )
    public Iterable<PropertyUsageSummary> getPropertyUsageSummary( @PathVariable( ID ) UUID propertyTypeId ) {
        ensureAdminAccess();
        return postgresEdmManager.getPropertyUsageSummary( propertyTypeId );
    }

    @Timed
    @Override
    @RequestMapping(
            path = SCHEMA_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.PATCH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void updateSchema(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @RequestBody EdmRequest request ) {
        ensureAdminAccess();

        final Set<UUID> propertyTypes = request.getPropertyTypes();
        final Set<UUID> entityTypes = request.getEntityTypes();
        final FullQualifiedName schemaName = new FullQualifiedName( namespace, name );
        switch ( request.getAction() ) {
            case ADD:
                schemaManager.addEntityTypesToSchema( entityTypes, schemaName );
                schemaManager.addPropertyTypesToSchema( propertyTypes, schemaName );
                break;
            case REMOVE:
                schemaManager.removeEntityTypesFromSchema( entityTypes, schemaName );
                schemaManager.removePropertyTypesFromSchema( propertyTypes, schemaName );
                break;
            case REPLACE:
                final Set<UUID> existingPropertyTypes = schemaManager.getAllPropertyTypesInSchema( schemaName );
                final Set<UUID> existingEntityTypes = schemaManager.getAllEntityTypesInSchema( schemaName );

                final Set<UUID> propertyTypesToAdd = Sets.difference( propertyTypes, existingPropertyTypes );
                final Set<UUID> propertyTypesToRemove = Sets.difference( existingPropertyTypes, propertyTypes );
                schemaManager.removePropertyTypesFromSchema( propertyTypesToRemove, schemaName );
                schemaManager.addPropertyTypesToSchema( propertyTypesToAdd, schemaName );

                final Set<UUID> entityTypesToAdd = Sets.difference( entityTypes, existingEntityTypes );
                final Set<UUID> entityTypesToRemove = Sets.difference( existingEntityTypes, entityTypes );
                schemaManager.removeEntityTypesFromSchema( entityTypesToAdd, schemaName );
                schemaManager.addEntityTypesToSchema( entityTypesToRemove, schemaName );
                break;
        }
        return null;
    }

    @Timed
    @Override
    @GetMapping(
            path = ENTITY_TYPE_PATH + ID_PATH + HIERARCHY_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Set<EntityType> getEntityTypeHierarchy( @PathVariable( ID ) UUID entityTypeId ) {
        return modelService.getEntityTypeHierarchy( entityTypeId );
    }

    @Timed
    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public UUID createEntityType( @RequestBody EntityType entityType ) {
        ensureValidEntityType( entityType );
        modelService.createEntityType( entityType );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( entityType.getId() ), // TODO should this be written as an AclKey?
                AuditEventType.CREATE_ENTITY_TYPE,
                "Entity type created through EdmApi.createEntityType",
                Optional.empty(),
                ImmutableMap.of( "entityType", entityType ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return entityType.getId();
    }

    @Timed
    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<EntityType> getEntityTypes() {
        return modelService.getEntityTypes();
    }

    @Timed
    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ENTITY_TYPE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<EntityType> getAssociationEntityTypes() {
        return modelService.getAssociationEntityTypes();
    }

    @Timed
    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<AssociationType> getAssociationTypes() {
        return modelService.getAssociationTypes();
    }

    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH + ID_PATH,
            method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public EntityType getEntityType( @PathVariable( ID ) UUID entityTypeId ) {
        return Preconditions.checkNotNull( modelService.getEntityType( entityTypeId ),
                "Unable to find entity type: " + entityTypeId );
    }

    @Timed
    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH + ENTITY_TYPE_ID_PATH + PROPERTY_TYPE_ID_PATH,
            method = RequestMethod.PUT )
    @ResponseStatus( HttpStatus.OK )
    public Void addPropertyTypeToEntityType(
            @PathVariable( ENTITY_TYPE_ID ) UUID entityTypeId,
            @PathVariable( PROPERTY_TYPE_ID ) UUID propertyTypeId ) {
        ensureAdminAccess();
        modelService.addPropertyTypesToEntityType( entityTypeId, ImmutableSet.of( propertyTypeId ) );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( entityTypeId ), // TODO should this be written as an AclKey?
                AuditEventType.ADD_PROPERTY_TYPE_TO_ENTITY_TYPE,
                "Property type added to entity type through EdmApi.addPropertyTypeToEntityType",
                Optional.empty(),
                ImmutableMap.of( "propertyTypeId", propertyTypeId ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH + ENTITY_TYPE_ID_PATH + PROPERTY_TYPE_ID_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void removePropertyTypeFromEntityType(
            @PathVariable( ENTITY_TYPE_ID ) UUID entityTypeId,
            @PathVariable( PROPERTY_TYPE_ID ) UUID propertyTypeId ) {
        ensureAdminAccess();
        modelService.removePropertyTypesFromEntityType( entityTypeId, ImmutableSet.of( propertyTypeId ) );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( entityTypeId ), // TODO should this be written as an AclKey?
                AuditEventType.REMOVE_PROPERTY_TYPE_FROM_ENTITY_TYPE,
                "Property type removed from entity type through EdmApi.removePropertyTypesFromEntityType",
                Optional.empty(),
                ImmutableMap.of( "propertyTypeId", propertyTypeId ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH + KEY_PATH + ENTITY_TYPE_ID_PATH + PROPERTY_TYPE_ID_PATH,
            method = RequestMethod.PUT )
    @ResponseStatus( HttpStatus.OK )
    public Void addPrimaryKeyToEntityType(
            @PathVariable( ENTITY_TYPE_ID ) UUID entityTypeId,
            @PathVariable( PROPERTY_TYPE_ID ) UUID propertyTypeId ) {
        ensureAdminAccess();

        modelService.addPrimaryKeysToEntityType( entityTypeId, ImmutableSet.of( propertyTypeId ) );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( entityTypeId ), // TODO should this be written as an AclKey?
                AuditEventType.UPDATE_ENTITY_TYPE,
                "Primary key added to entity type through EdmApi.addPrimaryKeyToEntityType",
                Optional.empty(),
                ImmutableMap.of( "propertyTypeId", propertyTypeId ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH + KEY_PATH + ENTITY_TYPE_ID_PATH + PROPERTY_TYPE_ID_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void removePrimaryKeyFromEntityType(
            @PathVariable( ENTITY_TYPE_ID ) UUID entityTypeId,
            @PathVariable( PROPERTY_TYPE_ID ) UUID propertyTypeId ) {
        ensureAdminAccess();

        modelService.removePrimaryKeysFromEntityType( entityTypeId, ImmutableSet.of( propertyTypeId ) );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( entityTypeId ), // TODO should this be written as an AclKey?
                AuditEventType.UPDATE_ENTITY_TYPE,
                "Primary key removed from entity type through EdmApi.removePrimaryKeyFromEntityType",
                Optional.empty(),
                ImmutableMap.of( "propertyTypeId", propertyTypeId ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH + ENTITY_TYPE_ID_PATH + PROPERTY_TYPE_ID_PATH + FORCE_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void forceRemovePropertyTypeFromEntityType(
            @PathVariable( ENTITY_TYPE_ID ) UUID entityTypeId,
            @PathVariable( PROPERTY_TYPE_ID ) UUID propertyTypeId ) {
        ensureAdminAccess();
        modelService.forceRemovePropertyTypesFromEntityType( entityTypeId, ImmutableSet.of( propertyTypeId ) );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( entityTypeId ), // TODO should this be written as an AclKey?
                AuditEventType.REMOVE_PROPERTY_TYPE_FROM_ENTITY_TYPE,
                "Property type forcibly removed from entity type through EdmApi.forceRemovePropertyTypeFromEntityType",
                Optional.empty(),
                ImmutableMap.of( "propertyTypeId", propertyTypeId ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH + ENTITY_TYPE_ID_PATH + PROPERTY_TYPE_PATH,
            method = RequestMethod.PATCH )
    @ResponseStatus( HttpStatus.OK )
    public Void reorderPropertyTypesInEntityType(
            @PathVariable( ENTITY_TYPE_ID ) UUID entityTypeId,
            @RequestBody LinkedHashSet<UUID> propertyTypeIds ) {
        ensureAdminAccess();
        modelService.reorderPropertyTypesInEntityType( entityTypeId, propertyTypeIds );
        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH + ID_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void deleteEntityType( @PathVariable( ID ) UUID entityTypeId ) {
        ensureAdminAccess();
        ensureObjectCanBeDeleted( entityTypeId );
        modelService.ensureEntityTypeExists( entityTypeId );
        modelService.deleteEntityType( entityTypeId );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( entityTypeId ), // TODO should this be written as an AclKey?
                AuditEventType.DELETE_ENTITY_TYPE,
                "Entity type deleted through EdmApi.deleteEntityType",
                Optional.empty(),
                ImmutableMap.of(),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PropertyType> getPropertyTypes() {
        return modelService.getPropertyTypes();
    }

    @Timed
    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public UUID createPropertyType( @RequestBody PropertyType propertyType ) {
        ensureAdminAccess();
        modelService.createPropertyTypeIfNotExists( propertyType );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( propertyType.getId() ), // TODO should this be written as an AclKey?
                AuditEventType.CREATE_PROPERTY_TYPE,
                "Property type created through EdmApi.createPropertyType",
                Optional.empty(),
                ImmutableMap.of( "propertyType", propertyType ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return propertyType.getId();
    }

    @Timed
    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_PATH + ID_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void deletePropertyType(
            @PathVariable( ID ) UUID propertyTypeId ) {
        ensureAdminAccess();
        ensureObjectCanBeDeleted( propertyTypeId );
        modelService.ensurePropertyTypeExists( propertyTypeId );
        modelService.deletePropertyType( propertyTypeId );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( propertyTypeId ), // TODO should this be written as an AclKey?
                AuditEventType.DELETE_PROPERTY_TYPE,
                "Property type deleted through EdmApi.deletePropertyType",
                Optional.empty(),
                ImmutableMap.of(),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_PATH + ID_PATH + FORCE_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void forceDeletePropertyType(
            @PathVariable( ID ) UUID propertyTypeId ) {
        ensureAdminAccess();
        ensureObjectCanBeDeleted( propertyTypeId );
        modelService.ensurePropertyTypeExists( propertyTypeId );
        modelService.forceDeletePropertyType( propertyTypeId );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( propertyTypeId ), // TODO should this be written as an AclKey?
                AuditEventType.DELETE_PROPERTY_TYPE,
                "Property type forcibly deleted through EdmApi.forceDeletePropertyType",
                Optional.empty(),
                ImmutableMap.of(),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_PATH + ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public PropertyType getPropertyType( @PathVariable( ID ) UUID propertyTypeId ) {
        return modelService.getPropertyType( propertyTypeId );
    }

    @Timed
    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_PATH + "/" + NAMESPACE + NAMESPACE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<PropertyType> getPropertyTypesInNamespace( @PathVariable( NAMESPACE ) String namespace ) {
        return modelService.getPropertyTypesInNamespace( namespace );
    }

    @Timed
    @Override
    @RequestMapping(
            path = IDS_PATH + PROPERTY_TYPE_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public UUID getPropertyTypeId( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        FullQualifiedName fqn = new FullQualifiedName( namespace, name );
        return Preconditions.checkNotNull( modelService.getTypeAclKey( fqn ),
                "Property Type %s does not exists.",
                fqn.getFullQualifiedNameAsString() );
    }

    @Timed
    @Override
    @RequestMapping(
            path = IDS_PATH + ENTITY_TYPE_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public UUID getEntityTypeId( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        FullQualifiedName fqn = new FullQualifiedName( namespace, name );
        return getEntityTypeId( fqn );
    }

    @Timed
    @Override
    @RequestMapping(
            path = IDS_PATH + ENTITY_TYPE_PATH + FULLQUALIFIED_NAME_PATH_REGEX,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public UUID getEntityTypeId( @PathVariable( FULLQUALIFIED_NAME ) FullQualifiedName fullQualifiedName ) {
        return Preconditions.checkNotNull( modelService.getTypeAclKey( fullQualifiedName ),
                "Entity Type %s does not exists.",
                fullQualifiedName.getFullQualifiedNameAsString() );
    }

    @Timed
    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_PATH + ID_PATH,
            method = RequestMethod.PATCH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void updatePropertyTypeMetadata(
            @PathVariable( ID ) UUID propertyTypeId,
            @RequestBody MetadataUpdate update ) {
        ensureAdminAccess();
        modelService.updatePropertyTypeMetadata( propertyTypeId, update );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( propertyTypeId ), // TODO should this be written as an AclKey?
                AuditEventType.UPDATE_PROPERTY_TYPE,
                "Property type metadata updated through EdmApi.updatePropertyTypeMetadata",
                Optional.empty(),
                ImmutableMap.of( "update", update ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH + ID_PATH,
            method = RequestMethod.PATCH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void updateEntityTypeMetadata( @PathVariable( ID ) UUID entityTypeId, @RequestBody MetadataUpdate update ) {
        ensureAdminAccess();
        modelService.updateEntityTypeMetadata( entityTypeId, update );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( entityTypeId ), // TODO should this be written as an AclKey?
                AuditEventType.UPDATE_ENTITY_TYPE,
                "Entity type metadata updated through EdmApi.updateEntityTypeMetadata",
                Optional.empty(),
                ImmutableMap.of( "update", update ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return null;
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

    private void ensureValidEntityType( EntityType entityType ) {
        Preconditions.checkArgument( modelService.checkPropertyTypesExist( entityType.getProperties() ),
                "Some properties do not exists" );
    }

    @Timed
    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH,
            method = RequestMethod.POST,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public UUID createAssociationType( @RequestBody AssociationType associationType ) {
        ensureAdminAccess();
        EntityType entityType = associationType.getAssociationEntityType();
        if ( entityType == null ) {
            throw new IllegalArgumentException( "You cannot create an edge type without specifying its entity type" );
        }
        Preconditions.checkArgument( entityType.getCategory().equals( SecurableObjectType.AssociationType ),
                "You cannot create an edge type with not an AssociationType category" );
        createEntityType( entityType );
        modelService.createAssociationType( associationType, entityType.getId() );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( associationType.getAssociationEntityType().getId() ),
                AuditEventType.CREATE_ASSOCIATION_TYPE,
                "Association type created through EdmApi.createAssociationType",
                Optional.empty(),
                ImmutableMap.of( "associationType", associationType ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return entityType.getId();
    }

    @Timed
    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ID_PATH,
            method = RequestMethod.DELETE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Void deleteAssociationType( @PathVariable( ID ) UUID associationTypeId ) {
        ensureAdminAccess();
        ensureObjectCanBeDeleted( associationTypeId );
        modelService.deleteAssociationType( associationTypeId );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( associationTypeId ),
                AuditEventType.DELETE_ASSOCIATION_TYPE,
                "Association type deleted through EdmApi.deleteAssociationType",
                Optional.empty(),
                ImmutableMap.of(),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ASSOCIATION_TYPE_ID_PATH + SRC_PATH + ENTITY_TYPE_ID_PATH,
            method = RequestMethod.PUT )
    public Void addSrcEntityTypeToAssociationType(
            @PathVariable( ASSOCIATION_TYPE_ID ) UUID associationTypeId,
            @PathVariable( ENTITY_TYPE_ID ) UUID entityTypeId ) {
        ensureAdminAccess();
        modelService.addSrcEntityTypesToAssociationType( associationTypeId, ImmutableSet.of( entityTypeId ) );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( associationTypeId ),
                AuditEventType.ADD_ENTITY_TYPE_TO_ASSOCIATION_TYPE,
                "Src entity type added to association type through EdmApi.addSrcEntityTypeToAssociationType",
                Optional.empty(),
                ImmutableMap.of( "src", entityTypeId ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ASSOCIATION_TYPE_ID_PATH + DST_PATH + ENTITY_TYPE_ID_PATH,
            method = RequestMethod.PUT )
    public Void addDstEntityTypeToAssociationType(
            @PathVariable( ASSOCIATION_TYPE_ID ) UUID associationTypeId,
            @PathVariable( ENTITY_TYPE_ID ) UUID entityTypeId ) {
        ensureAdminAccess();
        modelService.addDstEntityTypesToAssociationType( associationTypeId, ImmutableSet.of( entityTypeId ) );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( associationTypeId ),
                AuditEventType.ADD_ENTITY_TYPE_TO_ASSOCIATION_TYPE,
                "Dst entity type added to association type through EdmApi.addDstEntityTypeToAssociationType",
                Optional.empty(),
                ImmutableMap.of( "dst", entityTypeId ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ASSOCIATION_TYPE_ID_PATH + SRC_PATH + ENTITY_TYPE_ID_PATH,
            method = RequestMethod.DELETE )
    public Void removeSrcEntityTypeFromAssociationType(
            @PathVariable( ASSOCIATION_TYPE_ID ) UUID associationTypeId,
            @PathVariable( ENTITY_TYPE_ID ) UUID entityTypeId ) {
        ensureAdminAccess();
        modelService.removeSrcEntityTypesFromAssociationType( associationTypeId, ImmutableSet.of( entityTypeId ) );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( associationTypeId ),
                AuditEventType.REMOVE_ENTITY_TYPE_FROM_ASSOCIATION_TYPE,
                "Src entity type removed from association type through EdmApi.removeSrcEntityTypeFromAssociationType",
                Optional.empty(),
                ImmutableMap.of( "src", entityTypeId ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ASSOCIATION_TYPE_ID_PATH + DST_PATH + ENTITY_TYPE_ID_PATH,
            method = RequestMethod.DELETE )
    public Void removeDstEntityTypeFromAssociationType(
            @PathVariable( ASSOCIATION_TYPE_ID ) UUID associationTypeId,
            @PathVariable( ENTITY_TYPE_ID ) UUID entityTypeId ) {
        ensureAdminAccess();
        modelService.removeDstEntityTypesFromAssociationType( associationTypeId, ImmutableSet.of( entityTypeId ) );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( associationTypeId ),
                AuditEventType.REMOVE_ENTITY_TYPE_FROM_ASSOCIATION_TYPE,
                "Dst entity type removed from association type through EdmApi.removeDstEntityTypeFromAssociationType",
                Optional.empty(),
                ImmutableMap.of( "dst", entityTypeId ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return null;
    }

    @Timed
    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public AssociationType getAssociationTypeById( @PathVariable( ID ) UUID associationTypeId ) {
        return modelService.getAssociationType( associationTypeId );
    }

    @Timed
    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ID_PATH + DETAILED_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public AssociationDetails getAssociationDetailsForType( @PathVariable( ID ) UUID associationTypeId ) {
        return modelService.getAssociationDetails( associationTypeId );
    }

    @Timed
    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ID_PATH + AVAILABLE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<EntityType> getAvailableAssociationTypesForEntityType( @PathVariable( ID ) UUID entityTypeId ) {
        return modelService.getAvailableAssociationTypesForEntityType( entityTypeId );
    }

    @NotNull @Override public AuditingManager getAuditingManager() {
        return auditingManager;
    }

    @Timed
    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH + ID_PATH + PROPERTY_TYPE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<UUID, EntityTypePropertyMetadata> getAllEntityTypePropertyMetadata(
            @PathVariable( ID ) UUID entityTypeId ) {
        return modelService.getAllEntityTypePropertyMetadata( entityTypeId );
    }

    @Timed
    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH + ID_PATH + PROPERTY_TYPE_PATH + PROPERTY_TYPE_ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public EntityTypePropertyMetadata getEntityTypePropertyMetadata(
            @PathVariable( ID ) UUID entityTypeId,
            @PathVariable( PROPERTY_TYPE_ID ) UUID propertyTypeId ) {
        return modelService.getEntityTypePropertyMetadata( entityTypeId, propertyTypeId );
    }

    @Timed
    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH + ID_PATH + PROPERTY_TYPE_PATH + PROPERTY_TYPE_ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void updateEntityTypePropertyMetadata(
            @PathVariable( ID ) UUID entityTypeId,
            @PathVariable( PROPERTY_TYPE_ID ) UUID propertyTypeId,
            @RequestBody MetadataUpdate update ) {
        ensureAdminAccess();
        modelService.updateEntityTypePropertyMetadata( entityTypeId, propertyTypeId, update );

        recordEvent( new AuditableEvent(
                spm.getCurrentUserId(),
                new AclKey( entityTypeId ),
                AuditEventType.UPDATE_ENTITY_TYPE,
                "Entity type property metadata updated through EdmApi.updateEntityTypePropertyMetadata",
                Optional.empty(),
                ImmutableMap.of( "update", update ),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        return null;
    }


    private static void setDownloadContentType( HttpServletResponse response, FileType fileType ) {

        if ( fileType == null ) {
            response.setContentType( MediaType.APPLICATION_JSON_VALUE );
            return;
        }

        switch ( fileType ) {
            case csv:
                response.setContentType( CustomMediaType.TEXT_CSV_VALUE );
                break;
            case yaml:
                response.setContentType( CustomMediaType.TEXT_YAML_VALUE );
                break;
            case json:
            default:
                response.setContentType( MediaType.APPLICATION_JSON_VALUE );
                break;
        }
    }

    private static void setContentDisposition(
            HttpServletResponse response,
            String fileName,
            FileType fileType ) {
        if ( fileType == FileType.yaml || fileType == FileType.json ) {
            response.setHeader(
                    "Content-Disposition",
                    "attachment; filename=" + fileName + "." + fileType.toString()
            );
        }
    }
}
