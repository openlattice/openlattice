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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.openlattice.authorization.AbstractSecurableObjectResolveTypeService;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.EdmAuthorizationHelper;
import com.openlattice.authorization.ForbiddenException;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principals;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.authorization.util.AuthorizationUtils;
import com.openlattice.data.DatasourceManager;
import com.openlattice.data.EntityDatastore;
import com.openlattice.data.requests.FileType;
import com.openlattice.datastore.constants.CustomMediaType;
import com.openlattice.datastore.exceptions.BadRequestException;
import com.openlattice.datastore.exceptions.BatchException;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.PostgresEntitySetManager;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.EdmDetails;
import com.openlattice.edm.EntityDataModel;
import com.openlattice.edm.EntityDataModelDiff;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.Schema;
import com.openlattice.edm.requests.EdmDetailsSelector;
import com.openlattice.edm.requests.EdmRequest;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.edm.type.AssociationDetails;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.ComplexType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.EnumType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.exceptions.ApiExceptions;
import com.openlattice.exceptions.ErrorsDTO;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping( EdmApi.CONTROLLER )
public class EdmController implements EdmApi, AuthorizingComponent {

    @Inject
    private EdmManager modelService;

    @Inject
    private HazelcastSchemaManager schemaManager;

    @Inject
    private AuthorizationManager authorizations;

    @Inject
    private PostgresEntitySetManager entitySetManager;

    @Inject
    private AbstractSecurableObjectResolveTypeService securableObjectTypes;

    @Inject
    private AuthenticationManager authenticationManager;

    @Inject
    private EntityDatastore dataManager;

    @Inject
    private DatasourceManager datasourceManager;

    @Inject
    private EdmAuthorizationHelper authzHelper;

    @RequestMapping(
            path = CLEAR_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public void clearAllData() {
        ensureAdminAccess();
        modelService.clearTables();
    }

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

    @Override
    @RequestMapping(
            method = RequestMethod.PATCH,
            consumes = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_YAML_VALUE } )
    @ResponseStatus( HttpStatus.OK )
    public void updateEntityDataModel( @RequestBody EntityDataModel edm ) {
        ensureAdminAccess();
        modelService.setEntityDataModel( edm );
    }

    @Override
    @RequestMapping(
            path = VERSION_PATH,
            method = RequestMethod.GET,
            consumes = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_YAML_VALUE } )
    @ResponseStatus( HttpStatus.OK )
    public UUID getEntityDataModelVersion() {
        return modelService.getCurrentEntityDataModelVersion();
    }

    @Override
    @RequestMapping(
            path = VERSION_PATH + NEW_PATH,
            method = RequestMethod.GET,
            consumes = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_YAML_VALUE } )
    @ResponseStatus( HttpStatus.OK )
    public UUID generateNewEntityDataModelVersion() {
        ensureAdminAccess();
        return modelService.generateNewEntityDataModelVersion();
    }

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
        return new EdmDetails(
                modelService.getPropertyTypesAsMap( propertyTypeIds ),
                modelService.getEntityTypesAsMap( entityTypeIds ),
                modelService.getEntitySetsAsMap( entitySetIds ) );
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
            es = modelService.getEntitySet( selector.getId() );
            setRetrieved = true;
            if ( es != null ) {
                entityTypeIds.add( es.getEntityTypeId() );
            }
        }
        if ( selector.getIncludedFields().contains( SecurableObjectType.PropertyTypeInEntitySet ) ) {
            if ( !setRetrieved ) {
                es = modelService.getEntitySet( selector.getId() );
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
    @Override
    @RequestMapping(
            path = SCHEMA_PATH,
            method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Schema> getSchemas() {
        return schemaManager.getAllSchemas();
    }

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

    @RequestMapping(
            path = SCHEMA_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_YAML_VALUE } )
    public Schema getSchemaContentsFormatted(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @RequestParam(
                    value = FILE_TYPE,
                    required = true ) FileType fileType,
            @RequestParam(
                    value = TOKEN,
                    required = false ) String token,
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

    @Override
    @RequestMapping(
            path = ENTITY_SETS_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, UUID> createEntitySets( @RequestBody Set<EntitySet> entitySets ) {
        ErrorsDTO dto = new ErrorsDTO();

        Map<String, UUID> createdEntitySets = Maps.newHashMapWithExpectedSize( entitySets.size() );
        // TODO: Add access check to make sure user can create entity sets.
        for ( EntitySet entitySet : entitySets ) {
            try {
                ensureValidEntitySet( entitySet );
                modelService.createEntitySet( Principals.getCurrentUser(), entitySet );
                createdEntitySets.put( entitySet.getName(), entitySet.getId() );
            } catch ( Exception e ) {
                dto.addError( ApiExceptions.OTHER_EXCEPTION, entitySet.getName() + ": " + e.getMessage() );
            }
        }

        if ( !dto.isEmpty() ) {
            throw new BatchException( dto );
        }
        return createdEntitySets;
    }

    @Override
    @RequestMapping(
            path = ENTITY_SETS_PATH,
            method = RequestMethod.GET )
    public Iterable<EntitySet> getEntitySets() {
        return authorizations.getAuthorizedObjectsOfType(
                Principals.getCurrentPrincipals(),
                SecurableObjectType.EntitySet,
                EnumSet.of( Permission.READ ) )
                .map( AuthorizationUtils::getLastAclKeySafely )
                .map( modelService::getEntitySet )::iterator;
    }

    @Override
    @RequestMapping(
            path = ENTITY_SETS_PATH + ID_PATH,
            method = RequestMethod.GET )
    public EntitySet getEntitySet( @PathVariable( ID ) UUID entitySetId ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        return modelService.getEntitySet( entitySetId );
    }

    @Override
    @RequestMapping(
            path = ENTITY_SETS_PATH + ID_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void deleteEntitySet( @PathVariable( ID ) UUID entitySetId ) {
        ensureOwnerAccess( new AclKey( entitySetId ) );
        final EntitySet entitySet = modelService.getEntitySet( entitySetId );
        final EntityType entityType = modelService.getEntityType( entitySet.getEntityTypeId() );
        final Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes( entitySetId, EnumSet.of( Permission.OWNER ) );
        if ( !authorizedPropertyTypes.keySet().containsAll( entityType.getProperties() ) ) {
            throw new ForbiddenException( "You shall not pass!" );
        }

        modelService.deleteEntitySet( entitySetId );
        securableObjectTypes.deleteSecurableObjectType( new AclKey( entitySetId ) );
        dataManager.deleteEntitySetData( entitySetId, authorizedPropertyTypes );

        return null;
    }

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

    @Override
    @PostMapping(
            path = ENUM_TYPE_PATH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public UUID createEnumType( @RequestBody EnumType enumType ) {
        ensureAdminAccess();
        modelService.createEnumTypeIfNotExists( enumType );
        return enumType.getId();
    }

    @Override
    @GetMapping(
            path = ENUM_TYPE_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<EnumType> getEnumTypes() {
        return modelService.getEnumTypes()::iterator;
    }

    @Override
    @GetMapping(
            path = ENUM_TYPE_PATH + ID_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public EnumType getEnumType( @PathVariable( ID ) UUID enumTypeId ) {
        return modelService.getEnumType( enumTypeId );
    }

    @Override
    @GetMapping(
            path = ENTITY_TYPE_PATH + ID_PATH + HIERARCHY_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Set<EntityType> getEntityTypeHierarchy( @PathVariable( ID ) UUID entityTypeId ) {
        return modelService.getEntityTypeHierarchy( entityTypeId );
    }

    @Override
    @DeleteMapping(
            path = ENUM_TYPE_PATH + ID_PATH )
    public Void deleteEnumType( @PathVariable( ID ) UUID enumTypeId ) {
        ensureAdminAccess();
        modelService.deleteEnumType( enumTypeId );
        return null;
    }

    @Override
    @GetMapping(
            path = COMPLEX_TYPE_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<ComplexType> getComplexTypes() {
        return modelService.getComplexTypes()::iterator;
    }

    @Override
    @PostMapping(
            path = COMPLEX_TYPE_PATH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public UUID createComplexType( @RequestBody ComplexType complexType ) {
        ensureAdminAccess();
        modelService.createComplexTypeIfNotExists( complexType );
        return complexType.getId();
    }

    @Override
    @GetMapping(
            path = COMPLEX_TYPE_PATH + ID_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public ComplexType getComplexType( @PathVariable( ID ) UUID complexTypeId ) {
        return modelService.getComplexType( complexTypeId );
    }

    @Override
    @GetMapping(
            path = COMPLEX_TYPE_PATH + ID_PATH + HIERARCHY_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Set<ComplexType> getComplexTypeHierarchy( @PathVariable( ID ) UUID complexTypeId ) {
        return modelService.getComplexTypeHierarchy( complexTypeId );
    }

    @Override
    @DeleteMapping(
            path = COMPLEX_TYPE_PATH + ID_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Void deleteComplexType( @PathVariable( ID ) UUID complexTypeId ) {
        ensureAdminAccess();
        modelService.deleteComplexType( complexTypeId );
        return null;
    }

    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public UUID createEntityType( @RequestBody EntityType entityType ) {
        ensureValidEntityType( entityType );
        modelService.createEntityType( entityType );
        return entityType.getId();
    }

    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<EntityType> getEntityTypes() {
        return modelService.getEntityTypes()::iterator;
    }

    public Iterable<EntityType> getEntityTypesStrict() {
        return modelService.getEntityTypesStrict()::iterator;
    }

    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ENTITY_TYPE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<EntityType> getAssociationEntityTypes() {
        return modelService.getAssociationEntityTypes()::iterator;
    }

    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<AssociationType> getAssociationTypes() {
        return modelService.getAssociationTypes()::iterator;
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
        return null;
    }

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
        return null;
    }

    @Override public Void addPrimaryKeyToEntityType( UUID entityTypeId, UUID propertyTypeId ) {
        throw new NotImplementedException( "Not yet ported" );
    }

    @Override public Void removePrimaryKeyFromEntityType( UUID entityTypeId, UUID propertyTypeId ) {
        throw new NotImplementedException( "Not yet ported" );
    }

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
        return null;
    }

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

    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH + ID_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void deleteEntityType( @PathVariable( ID ) UUID entityTypeId ) {
        ensureAdminAccess();
        modelService.deleteEntityType( entityTypeId );
        return null;
    }

    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PropertyType> getPropertyTypes() {
        return modelService.getPropertyTypes()::iterator;
    }

    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public UUID createPropertyType( @RequestBody PropertyType propertyType ) {
        ensureAdminAccess();
        modelService.createPropertyTypeIfNotExists( propertyType );
        return propertyType.getId();
    }

    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_PATH + ID_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void deletePropertyType(
            @PathVariable( ID ) UUID propertyTypeId ) {
        ensureAdminAccess();
        modelService.deletePropertyType( propertyTypeId );
        return null;
    }

    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_PATH + ID_PATH + FORCE_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void forceDeletePropertyType(
            @PathVariable( ID ) UUID propertyTypeId ) {
        ensureAdminAccess();
        modelService.forceDeletePropertyType( propertyTypeId );
        return null;
    }

    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_PATH + ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public PropertyType getPropertyType( @PathVariable( ID ) UUID propertyTypeId ) {
        return modelService.getPropertyType( propertyTypeId );
    }

    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_PATH + "/" + NAMESPACE + NAMESPACE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<PropertyType> getPropertyTypesInNamespace( @PathVariable( NAMESPACE ) String namespace ) {
        return modelService.getPropertyTypesInNamespace( namespace );
    }

    @Override
    @RequestMapping(
            path = IDS_PATH + ENTITY_SETS_PATH + NAME_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public UUID getEntitySetId( @PathVariable( NAME ) String entitySetName ) {
        EntitySet es = entitySetManager.getEntitySet( entitySetName );
        Preconditions.checkNotNull( es, "Entity Set %s does not exists.", entitySetName );
        return es.getId();
    }

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

    @Override
    @RequestMapping(
            path = IDS_PATH + ENTITY_TYPE_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public UUID getEntityTypeId( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        FullQualifiedName fqn = new FullQualifiedName( namespace, name );
        return Preconditions.checkNotNull( modelService.getTypeAclKey( fqn ),
                "Entity Type %s does not exists.",
                fqn.getFullQualifiedNameAsString() );
    }

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
        return null;
    }

    @Override
    @RequestMapping(
            path = ENTITY_TYPE_PATH + ID_PATH,
            method = RequestMethod.PATCH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void updateEntityTypeMetadata( @PathVariable( ID ) UUID entityTypeId, @RequestBody MetadataUpdate update ) {
        ensureAdminAccess();
        modelService.updateEntityTypeMetadata( entityTypeId, update );
        return null;
    }

    @Override
    @RequestMapping(
            path = ENTITY_SETS_PATH + ID_PATH,
            method = RequestMethod.PATCH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void updateEntitySetMetadata( @PathVariable( ID ) UUID entitySetId, @RequestBody MetadataUpdate update ) {
        ensureOwnerAccess( new AclKey( entitySetId ) );
        modelService.updateEntitySetMetadata( entitySetId, update );
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

    private void ensureValidEntitySet( EntitySet entitySet ) {
        Preconditions.checkArgument( modelService.checkEntityTypeExists( entitySet.getEntityTypeId() ),
                "Entity Set Type does not exists." );
    }

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
        createEntityType( entityType );
        modelService.createAssociationType( associationType, entityType.getId() );
        return entityType.getId();
    }

    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ID_PATH,
            method = RequestMethod.DELETE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Void deleteAssociationType( @PathVariable( ID ) UUID associationTypeId ) {
        ensureAdminAccess();
        modelService.deleteAssociationType( associationTypeId );
        return null;
    }

    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ASSOCIATION_TYPE_ID_PATH + SRC_PATH + ENTITY_TYPE_ID_PATH,
            method = RequestMethod.PUT )
    public Void addSrcEntityTypeToAssociationType(
            @PathVariable( ASSOCIATION_TYPE_ID ) UUID associationTypeId,
            @PathVariable( ENTITY_TYPE_ID ) UUID entityTypeId ) {
        ensureAdminAccess();
        modelService.addSrcEntityTypesToAssociationType( associationTypeId, ImmutableSet.of( entityTypeId ) );
        return null;
    }

    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ASSOCIATION_TYPE_ID_PATH + DST_PATH + ENTITY_TYPE_ID_PATH,
            method = RequestMethod.PUT )
    public Void addDstEntityTypeToAssociationType(
            @PathVariable( ASSOCIATION_TYPE_ID ) UUID associationTypeId,
            @PathVariable( ENTITY_TYPE_ID ) UUID entityTypeId ) {
        ensureAdminAccess();
        modelService.addDstEntityTypesToAssociationType( associationTypeId, ImmutableSet.of( entityTypeId ) );
        return null;
    }

    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ASSOCIATION_TYPE_ID_PATH + SRC_PATH + ENTITY_TYPE_ID_PATH,
            method = RequestMethod.DELETE )
    public Void removeSrcEntityTypeFromAssociationType(
            @PathVariable( ASSOCIATION_TYPE_ID ) UUID associationTypeId,
            @PathVariable( ENTITY_TYPE_ID ) UUID entityTypeId ) {
        ensureAdminAccess();
        modelService.removeSrcEntityTypesFromAssociationType( associationTypeId, ImmutableSet.of( entityTypeId ) );
        return null;
    }

    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ASSOCIATION_TYPE_ID_PATH + DST_PATH + ENTITY_TYPE_ID_PATH,
            method = RequestMethod.DELETE )
    public Void removeDstEntityTypeFromAssociationType(
            @PathVariable( ASSOCIATION_TYPE_ID ) UUID associationTypeId,
            @PathVariable( ENTITY_TYPE_ID ) UUID entityTypeId ) {
        ensureAdminAccess();
        modelService.removeDstEntityTypesFromAssociationType( associationTypeId, ImmutableSet.of( entityTypeId ) );
        return null;
    }

    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public AssociationType getAssociationTypeById( @PathVariable( ID ) UUID associationTypeId ) {
        return modelService.getAssociationType( associationTypeId );
    }

    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ID_PATH + DETAILED_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public AssociationDetails getAssociationDetailsForType( @PathVariable( ID ) UUID associationTypeId ) {
        return modelService.getAssociationDetails( associationTypeId );
    }

    @Override
    @RequestMapping(
            path = ASSOCIATION_TYPE_PATH + ID_PATH + AVAILABLE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<EntityType> getAvailableAssociationTypesForEntityType( @PathVariable( ID ) UUID entityTypeId ) {
        return modelService.getAvailableAssociationTypesForEntityType( entityTypeId );
    }

    @Override
    @RequestMapping(
            path = ENTITY_SETS_PATH + ID_PATH + PROPERTY_TYPE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<UUID, EntitySetPropertyMetadata> getAllEntitySetPropertyMetadata(
            @PathVariable( ID ) UUID entitySetId ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        Set<UUID> authorizedPropertyTypes = modelService.getEntityTypeByEntitySetId( entitySetId ).getProperties()
                .stream().filter( propertyTypeId -> authorizations
                        .checkIfHasPermissions( new AclKey( entitySetId, propertyTypeId ),
                                Principals.getCurrentPrincipals(),
                                EnumSet.of( Permission.READ ) )
                ).collect( Collectors.toSet() );
        return modelService.getAllEntitySetPropertyMetadata( entitySetId, authorizedPropertyTypes );
    }

    @Override
    @RequestMapping(
            path = ENTITY_SETS_PATH + ID_PATH + PROPERTY_TYPE_PATH + PROPERTY_TYPE_ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public EntitySetPropertyMetadata getEntitySetPropertyMetadata(
            @PathVariable( ID ) UUID entitySetId,
            @PathVariable( PROPERTY_TYPE_ID ) UUID propertyTypeId ) {
        ensureReadAccess( new AclKey( entitySetId, propertyTypeId ) );
        return modelService.getEntitySetPropertyMetadata( entitySetId, propertyTypeId );
    }

    @Override
    @RequestMapping(
            path = ENTITY_SETS_PATH + ID_PATH + PROPERTY_TYPE_PATH + PROPERTY_TYPE_ID_PATH,
            method = RequestMethod.POST,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Void updateEntitySetPropertyMetadata(
            @PathVariable( ID ) UUID entitySetId,
            @PathVariable( PROPERTY_TYPE_ID ) UUID propertyTypeId,
            @RequestBody MetadataUpdate update ) {
        ensureOwnerAccess( new AclKey( entitySetId, propertyTypeId ) );
        modelService.updateEntitySetPropertyMetadata( entitySetId, propertyTypeId, update );
        return null;
    }

    private static void setDownloadContentType( HttpServletResponse response, FileType fileType ) {
        if ( fileType == FileType.json ) {
            response.setContentType( MediaType.APPLICATION_JSON_VALUE );
        } else {
            response.setContentType( CustomMediaType.TEXT_YAML_VALUE );
        }
    }

    private static void setContentDisposition(
            HttpServletResponse response,
            String fileName,
            FileType fileType ) {
        if ( fileType == FileType.yaml || fileType == FileType.json ) {
            response.setHeader( "Content-Disposition",
                    "attachment; filename=" + fileName + "." + fileType.toString() );
        }
    }
}
