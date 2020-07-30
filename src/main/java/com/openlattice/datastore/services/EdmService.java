

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

package com.openlattice.datastore.services;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.query.Predicates;
import com.openlattice.assembler.events.MaterializedEntitySetEdmChangeEvent;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.HazelcastAclKeyReservationService;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.controllers.exceptions.TypeExistsException;
import com.openlattice.controllers.exceptions.TypeNotFoundException;
import com.openlattice.datastore.util.Util;
import com.openlattice.edm.EntityDataModel;
import com.openlattice.edm.EntityDataModelDiff;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.Schema;
import com.openlattice.edm.events.*;
import com.openlattice.edm.properties.PostgresTypeManager;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.edm.set.EntitySetPropertyKey;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.edm.type.AssociationDetails;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.EntityTypePropertyKey;
import com.openlattice.edm.type.EntityTypePropertyMetadata;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.edm.types.processors.*;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.hazelcast.HazelcastUtils;
import com.openlattice.postgres.mapstores.EntitySetMapstore;
import com.openlattice.postgres.mapstores.EntityTypeMapstore;
import com.openlattice.postgres.mapstores.EntityTypePropertyMetadataMapstore;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

public class EdmService implements EdmManager {

    private static final Logger logger = LoggerFactory.getLogger( EdmService.class );

    private final IMap<UUID, PropertyType>                                propertyTypes;
    private final IMap<UUID, EntityType>                                  entityTypes;
    private final IMap<UUID, EntitySet>                                   entitySets;
    private final IMap<String, UUID>                                      aclKeys;
    private final IMap<UUID, String>                                      names;
    private final IMap<UUID, AssociationType>                             associationTypes;
    private final IMap<EntitySetPropertyKey, EntitySetPropertyMetadata>   entitySetPropertyMetadata;
    private final IMap<EntityTypePropertyKey, EntityTypePropertyMetadata> entityTypePropertyMetadata;

    private final HazelcastAclKeyReservationService aclKeyReservations;
    private final AuthorizationManager              authorizations;
    private final PostgresTypeManager               entityTypeManager;
    private final HazelcastSchemaManager            schemaManager;

    @Inject
    private EventBus eventBus;

    public EdmService(
            HazelcastInstance hazelcastInstance,
            HazelcastAclKeyReservationService aclKeyReservations,
            AuthorizationManager authorizations,
            PostgresTypeManager entityTypeManager,
            HazelcastSchemaManager schemaManager ) {

        this.authorizations = authorizations;
        this.entityTypeManager = entityTypeManager;
        this.schemaManager = schemaManager;
        this.propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap( hazelcastInstance );
        this.entityTypes = HazelcastMap.ENTITY_TYPES.getMap( hazelcastInstance );
        this.entitySets = HazelcastMap.ENTITY_SETS.getMap( hazelcastInstance );
        this.names = HazelcastMap.NAMES.getMap( hazelcastInstance );
        this.aclKeys = HazelcastMap.ACL_KEYS.getMap( hazelcastInstance );
        this.associationTypes = HazelcastMap.ASSOCIATION_TYPES.getMap( hazelcastInstance );
        this.entitySetPropertyMetadata = HazelcastMap.ENTITY_SET_PROPERTY_METADATA.getMap( hazelcastInstance );
        this.entityTypePropertyMetadata = HazelcastMap.ENTITY_TYPE_PROPERTY_METADATA.getMap( hazelcastInstance );
        this.aclKeyReservations = aclKeyReservations;
        propertyTypes.values().forEach( propertyType -> logger.debug( "Property type read: {}", propertyType ) );
        entityTypes.values().forEach( entityType -> logger.debug( "Object type read: {}", entityType ) );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#createPropertyType(com.kryptnostic.types.PropertyType) update
     * propertyType (and return true upon success) if exists, return false otherwise
     */
    @Override
    public void createPropertyTypeIfNotExists( PropertyType propertyType ) {
        try {
            aclKeyReservations.reserveIdAndValidateType( propertyType );
        } catch ( TypeExistsException e ) {
            logger.error( "A type with this name already exists." );
            return;
        }

        /*
         * Create property type if it doesn't exists. The reserveAclKeyAndValidateType call should ensure that
         */

        PropertyType dbRecord = propertyTypes.putIfAbsent( propertyType.getId(), propertyType );

        if ( dbRecord == null ) {
            propertyType.getSchemas().forEach( schemaManager.propertyTypesSchemaAdder( propertyType.getId() ) );

            eventBus.post( new PropertyTypeCreatedEvent( propertyType ) );
        } else {
            logger.error(
                    "Inconsistency encountered in database. Verify that existing property types have all their acl keys reserved." );
        }
    }

    @Override
    public void deleteEntityType( UUID entityTypeId ) {
        /*
         * Entity types should only be deleted if there are no entity sets of that type in the system.
         */
        if ( Iterables.isEmpty( getEntitySetIdsOfType( entityTypeId ) ) ) {
            entityTypeManager.getAssociationIdsForEntityType( entityTypeId ).forEach( associationTypeId -> {
                AssociationType association = getAssociationType( associationTypeId );
                if ( association.getSrc().contains( entityTypeId ) ) {
                    removeSrcEntityTypesFromAssociationType( associationTypeId, ImmutableSet.of( entityTypeId ) );
                }
                if ( association.getDst().contains( entityTypeId ) ) {
                    removeDstEntityTypesFromAssociationType( associationTypeId, ImmutableSet.of( entityTypeId ) );
                }
            } );

            entityTypes.delete( entityTypeId );
            aclKeyReservations.release( entityTypeId );
            eventBus.post( new EntityTypeDeletedEvent( entityTypeId ) );
        } else {
            throw new IllegalArgumentException(
                    "Unable to delete entity type because it is associated with an entity set." );
        }
    }

    @Override
    public void deletePropertyType( UUID propertyTypeId ) {
        Stream<EntityType> entityTypes = entityTypeManager
                .getEntityTypesContainingPropertyTypesAsStream( ImmutableSet
                        .of( propertyTypeId ) );
        if ( entityTypes.allMatch( et -> Iterables.isEmpty( getEntitySetIdsOfType( et.getId() ) ) ) ) {
            forceDeletePropertyType( propertyTypeId );
        } else {
            throw new IllegalArgumentException(
                    "Unable to delete property type because it is associated with an entity set." );
        }
    }

    @Override
    public void forceDeletePropertyType( UUID propertyTypeId ) {
        final var entityTypes = getEntityTypesContainPropertyType( propertyTypeId );
        entityTypes.forEach( et ->
                forceRemovePropertyTypesFromEntityType( et.getId(), ImmutableSet.of( propertyTypeId ) )
        );

        propertyTypes.delete( propertyTypeId );
        aclKeyReservations.release( propertyTypeId );

        eventBus.post( new PropertyTypeDeletedEvent( propertyTypeId ) );
    }

    private Collection<EntityType> getEntityTypesContainPropertyType( UUID propertyTypeId ) {
        return entityTypes.values( Predicates.equal( EntityTypeMapstore.PROPERTIES_INDEX, propertyTypeId ) );
    }

    private EntityType getEntityTypeWithBaseType( EntityType entityType ) {
        EntityType baseType = getEntityType( entityType.getBaseType().get() );
        LinkedHashSet<UUID> properties = new LinkedHashSet<>();
        properties.addAll( baseType.getProperties() );
        properties.addAll( entityType.getProperties() );
        LinkedHashSet<UUID> key = new LinkedHashSet<>();
        key.addAll( baseType.getKey() );
        key.addAll( entityType.getKey() );
        key.forEach( keyId -> Preconditions.checkArgument( properties.contains( keyId ),
                "Properties must include all the key property types" ) );
        return new EntityType(
                Optional.ofNullable( entityType.getId() ),
                entityType.getType(),
                entityType.getTitle(),
                Optional.ofNullable( entityType.getDescription() ),
                entityType.getSchemas(),
                key,
                properties,
                Optional.of( entityType.getPropertyTags() ),
                entityType.getBaseType(),
                Optional.ofNullable( entityType.getCategory() ),
                Optional.of( entityType.getShards() ) );

    }

    public void createEntityType( EntityType entityTypeRaw ) {
        /*
         * This is really create or replace and should be noted as such.
         */
        EntityType entityType = ( entityTypeRaw.getBaseType().isPresent() )
                ? getEntityTypeWithBaseType( entityTypeRaw ) : entityTypeRaw;
        aclKeyReservations.reserveIdAndValidateType( entityType );
        // Only create entity table if insert transaction succeeded.
        final EntityType existing = entityTypes.putIfAbsent( entityType.getId(), entityType );
        if ( existing == null ) {
            setupDefaultEntityTypePropertyMetadata( entityType.getId() );
            /*
             * As long as schemas are registered with upsertSchema, the schema query service should pick up the schemas
             * directly from the entity types and property types tables. Longer term, we should be more explicit about
             * the magic schema registration that happens when an entity type or property type is written since the
             * services are loosely coupled in a way that makes it easy to break accidentally.
             */
            schemaManager.upsertSchemas( entityType.getSchemas() );
            if ( !entityType.getCategory().equals( SecurableObjectType.AssociationType ) ) {
                eventBus.post( new EntityTypeCreatedEvent( entityType ) );
            }
        } else {
            /*
             * Only allow updates if entity type is not already in use.
             */
            if ( Iterables.isEmpty( getEntitySetIdsOfType( entityType.getId() ) ) ) {
                // Retrieve properties known to user
                Set<UUID> currentPropertyTypes = existing.getProperties();
                // Remove the removable property types in database properly; this step takes care of removal of
                // permissions
                Set<UUID> removablePropertyTypesInEntityType = Sets.difference( currentPropertyTypes,
                        entityType.getProperties() );
                removePropertyTypesFromEntityType( existing.getId(), removablePropertyTypesInEntityType );
                // Add the new property types in
                Set<UUID> newPropertyTypesInEntityType = Sets.difference( entityType.getProperties(),
                        currentPropertyTypes );
                addPropertyTypesToEntityType( entityType.getId(), newPropertyTypesInEntityType );

                // Update Schema
                final Set<FullQualifiedName> currentSchemas = existing.getSchemas();
                final Set<FullQualifiedName> removableSchemas = Sets.difference( currentSchemas,
                        entityType.getSchemas() );
                final Set<UUID> entityTypeSingleton = getEntityTypeUuids( ImmutableSet.of( existing.getType() ) );

                removableSchemas
                        .forEach( schema -> schemaManager.removeEntityTypesFromSchema( entityTypeSingleton, schema ) );

                Set<FullQualifiedName> newSchemas = Sets.difference( entityType.getSchemas(), currentSchemas );
                newSchemas.forEach( schema -> schemaManager.addEntityTypesToSchema( entityTypeSingleton, schema ) );
            }
        }
    }

    @Override
    public Set<UUID> getAllPropertyTypeIds() {
        return propertyTypes.keySet();
    }

    private void setupDefaultEntityTypePropertyMetadata( UUID entityTypeId ) {
        final var et = getEntityType( entityTypeId );
        final var propertyTags = et.getPropertyTags();

        Map<UUID, PropertyType> propertyTypeMap = getPropertyTypesAsMap( et.getProperties() );
        et.getProperties().forEach( propertyTypeId -> {
            EntityTypePropertyKey key = new EntityTypePropertyKey( entityTypeId, propertyTypeId );
            PropertyType property = propertyTypeMap.get( propertyTypeId );
            EntityTypePropertyMetadata metadata = new EntityTypePropertyMetadata(
                    property.getTitle(),
                    property.getDescription(),
                    new LinkedHashSet<>( propertyTags.get( propertyTypeId ) ),
                    true
            );
            entityTypePropertyMetadata.put( key, metadata );
        } );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public Set<UUID> getAclKeys( Set<?> fqnsOrNames ) {
        if ( fqnsOrNames.isEmpty() ) {
            return ImmutableSet.of();
        }

        Object o = fqnsOrNames.iterator().next();
        Set<String> names;
        if ( String.class.isAssignableFrom( o.getClass() ) ) {
            names = (Set<String>) fqnsOrNames;
        } else if ( FullQualifiedName.class.isAssignableFrom( o.getClass() ) ) {
            names = Util.fqnToString( (Set<FullQualifiedName>) fqnsOrNames );
        } else {
            throw new IllegalArgumentException(
                    "Unable to retrieve Acl Keys for this type: " + o.getClass().getSimpleName() );
        }
        return ImmutableSet.copyOf( aclKeys.getAll( names ).values() );
    }

    @Override
    public Map<String, UUID> getAclKeyIds( Set<String> aclNames ) {
        return aclKeys.getAll( aclNames );
    }

    @Override
    public Set<UUID> getEntityTypeUuids( Set<FullQualifiedName> fqns ) {
        return aclKeys.getAll( Util.fqnToString( fqns ) ).values().stream()
                .filter( Objects::nonNull )
                .collect( Collectors.toSet() );
    }

    @Override
    public UUID getPropertyTypeId( FullQualifiedName fqn ) {
        return aclKeys.get( fqn.getFullQualifiedNameAsString() );
    }

    @Override
    public Set<UUID> getPropertyTypeUuids( Set<FullQualifiedName> fqns ) {
        return aclKeys.getAll( Util.fqnToString( fqns ) ).values().stream()
                .filter( Objects::nonNull )
                .collect( Collectors.toSet() );
    }

    @Override
    public Set<EntityType> getEntityTypeHierarchy( UUID entityTypeId ) {
        return getTypeHierarchy( entityTypeId, HazelcastUtils.getter( entityTypes ), EntityType::getBaseType );
    }

    private <T> Set<T> getTypeHierarchy(
            UUID enumTypeId,
            Function<UUID, T> typeGetter,
            Function<T, Optional<UUID>> baseTypeSupplier ) {
        Set<T> typeHierarchy = new LinkedHashSet<>();
        T entityType;
        Optional<UUID> baseType = Optional.of( enumTypeId );

        do {
            entityType = typeGetter.apply( baseType.get() );
            if ( entityType == null ) {
                throw new TypeNotFoundException( "Unable to find type " + baseType.get() );
            }
            baseType = baseTypeSupplier.apply( entityType );
            typeHierarchy.add( entityType );
        } while ( baseType.isPresent() );

        return typeHierarchy;
    }

    @Override
    public AssociationType getAssociationType( FullQualifiedName typeFqn ) {
        UUID entityTypeId = getTypeAclKey( typeFqn );
        checkNotNull( entityTypeId,
                "Entity type %s does not exists.",
                typeFqn.getFullQualifiedNameAsString() );
        return getAssociationType( entityTypeId );
    }

    @Override
    public EntityType getEntityType( FullQualifiedName typeFqn ) {
        final var entityType = getEntityTypeSafe( typeFqn );
        checkNotNull( entityType,
                "Entity type %s does not exists.",
                typeFqn.getFullQualifiedNameAsString() );
        return entityType;
    }

    @Override
    public @Nullable EntityType getEntityTypeSafe( FullQualifiedName typeFqn ) {
        UUID entityTypeId = getTypeAclKey( typeFqn );
        return ( entityTypeId == null ) ? null : getEntityType( entityTypeId );
    }

    @Override
    public EntityType getEntityType( UUID entityTypeId ) {
        return checkNotNull(
                getEntityTypeSafe( entityTypeId ),
                "Entity type of id %s does not exists.",
                entityTypeId.toString() );
    }

    @Override
    public EntityType getEntityTypeSafe( UUID entityTypeId ) {
        return Util.getSafely( entityTypes, entityTypeId );
    }

    public Iterable<EntityType> getEntityTypes() {
        return entityTypeManager.getEntityTypes();
    }

    public Iterable<EntityType> getEntityTypesStrict() {
        return entityTypeManager.getEntityTypesStrict();
    }

    @Override
    public Iterable<EntityType> getAssociationEntityTypes() {
        return entityTypeManager.getAssociationEntityTypes();
    }

    @Override
    public Iterable<AssociationType> getAssociationTypes() {
        return StreamSupport.stream( entityTypeManager.getAssociationTypeIds().spliterator(), false )
                .map( this::getAssociationType ).collect( Collectors.toList() );
    }

    @Override
    public EntityType getEntityType( String namespace, String name ) {
        return getEntityType( new FullQualifiedName( namespace, name ) );
    }

    @Override
    public PropertyType getPropertyType( FullQualifiedName propertyType ) {
        return checkNotNull(
                Util.getSafely( propertyTypes, Util.getSafely( aclKeys, Util.fqnToString( propertyType ) ) ),
                "Property type %s does not exists",
                propertyType.getFullQualifiedNameAsString() );
    }

    @Override
    public Iterable<PropertyType> getPropertyTypesInNamespace( String namespace ) {
        return entityTypeManager.getPropertyTypesInNamespace( namespace );
    }

    @Override
    public Iterable<PropertyType> getPropertyTypes() {
        return entityTypeManager.getPropertyTypes();
    }

    @Override
    public void addPropertyTypesToEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeIds ), "Some properties do not exist." );

        List<PropertyType> newPropertyTypes = Lists.newArrayList( propertyTypes.getAll( propertyTypeIds ).values() );
        Stream<UUID> childrenIds = entityTypeManager.getEntityTypeChildrenIdsDeep( entityTypeId );
        Map<UUID, Boolean> childrenIdsToLocks = childrenIds
                .collect( Collectors.toMap( Function.identity(), propertyTypes::tryLock ) );
        childrenIdsToLocks.values().forEach( locked -> {
            if ( !locked ) {
                childrenIdsToLocks.forEach( ( key, value ) -> {
                    if ( value ) { propertyTypes.unlock( key ); }
                } );
                throw new IllegalStateException(
                        "Unable to modify the entity data model right now--please try again." );
            }
        } );
        final var propertyTags = entityTypes.get( entityTypeId ).getPropertyTags();
        childrenIdsToLocks.keySet().forEach( id -> {
            entityTypes.executeOnKey( id, new AddPropertyTypesToEntityTypeProcessor( propertyTypeIds ) );
            List<PropertyType> allPropertyTypes = Lists.newArrayList(
                    propertyTypes.getAll( getEntityType( id ).getProperties() ).values() );

            for ( EntitySet entitySet : getEntitySetsOfType( id ) ) {
                UUID esId = entitySet.getId();
                Map<UUID, PropertyType> propertyTypes = propertyTypeIds.stream().collect( Collectors.toMap(
                        propertyTypeId -> propertyTypeId, this::getPropertyType ) );
                Iterable<Principal> owners = authorizations.getSecurableObjectOwners( new AclKey( esId ) );
                for ( Principal owner : owners ) {
                    propertyTypeIds.stream()
                            .map( propertyTypeId -> new AclKey( esId, propertyTypeId ) )
                            .forEach( aclKey -> {
                                authorizations.setSecurableObjectType( aclKey,
                                        SecurableObjectType.PropertyTypeInEntitySet );

                                authorizations.addPermission(
                                        aclKey,
                                        owner,
                                        EnumSet.allOf( Permission.class ) );

                                PropertyType pt = propertyTypes.get( aclKey.get( 1 ) );
                                EntitySetPropertyMetadata defaultMetadata = new EntitySetPropertyMetadata(
                                        pt.getTitle(),
                                        pt.getDescription(),
                                        new LinkedHashSet<>( propertyTags.get( pt.getId() ) ),
                                        true );

                                entitySetPropertyMetadata.put(
                                        new EntitySetPropertyKey( aclKey.get( 0 ), aclKey.get( 1 ) ), defaultMetadata );
                            } );
                }

                markMaterializedEntitySetDirtyWithEdmChanges( esId );  // add edm_unsync flag for materialized views

                eventBus.post( new PropertyTypesInEntitySetUpdatedEvent( esId, allPropertyTypes, false ) );
                eventBus.post( new PropertyTypesAddedToEntitySetEvent(
                        entitySet,
                        newPropertyTypes,
                        ( entitySet.isLinking() )
                                ? Optional.of( entitySet.getLinkedEntitySets() ) : Optional.empty() ) );
            }

            EntityType entityType = getEntityType( id );
            eventBus.post( new PropertyTypesAddedToEntityTypeEvent( entityType, newPropertyTypes ) );
            if ( !entityType.getCategory().equals( SecurableObjectType.AssociationType ) ) {
                eventBus.post( new EntityTypeCreatedEvent( entityType ) );
            } else {
                eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( id ) ) );
            }
        } );
        childrenIdsToLocks.forEach( ( key, value ) -> {
            if ( value ) { propertyTypes.unlock( key ); }
        } );
    }

    @Override
    public void removePropertyTypesFromEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeIds ), "Some properties do not exist." );

        List<UUID> childrenIds = entityTypeManager.getEntityTypeChildrenIdsDeep( entityTypeId )
                .collect( Collectors.toList() );
        childrenIds.forEach( id -> {
            Preconditions.checkArgument( Sets.intersection( getEntityType( id ).getKey(), propertyTypeIds ).isEmpty(),
                    "Key property types cannot be removed." );
            Preconditions.checkArgument( !getEntitySetIdsOfType( id ).iterator().hasNext(),
                    "Property types cannot be removed from entity types that have already been associated with an entity set." );
        } );

        forceRemovePropertyTypesFromEntityType( entityTypeId, propertyTypeIds );
    }

    @Override
    public void forceRemovePropertyTypesFromEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeIds ), "Some properties do not exist." );
        EntityType entityType = getEntityType( entityTypeId );

        if ( entityType.getBaseType().isPresent() ) {
            EntityType baseType = getEntityType( entityType.getBaseType().get() );
            Preconditions.checkArgument( Sets.intersection( propertyTypeIds, baseType.getProperties() ).isEmpty(),
                    "Inherited property types cannot be removed." );
        }

        List<UUID> childrenIds = entityTypeManager
                .getEntityTypeChildrenIdsDeep( entityTypeId )
                .collect( Collectors.toList() );

        Map<UUID, Boolean> childrenIdsToLocks = childrenIds.stream()
                .collect( Collectors.toMap( Function.identity(), propertyTypes::tryLock ) );
        childrenIdsToLocks.values().forEach( locked -> {
            if ( !locked ) {
                childrenIdsToLocks.forEach( ( key, value ) -> {
                    if ( value ) { propertyTypes.unlock( key ); }
                } );
                throw new IllegalStateException(
                        "Unable to modify the entity data model right now--please try again." );
            }
        } );

        childrenIds.forEach( id -> {
            entityTypes.executeOnKey( id, new RemovePropertyTypesFromEntityTypeProcessor( propertyTypeIds ) );
            EntityType childEntityType = getEntityType( id );
            if ( !childEntityType.getCategory().equals( SecurableObjectType.AssociationType ) ) {
                eventBus.post( new EntityTypeCreatedEvent( childEntityType ) );
            } else {
                eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( id ) ) );
            }
            final var entitySetIdsOfEntityType = getEntitySetIdsOfType( id );
            entitySetIdsOfEntityType.forEach( this::markMaterializedEntitySetDirtyWithEdmChanges );
        } );
        childrenIds.forEach( propertyTypes::unlock );

    }

    @Override
    public void reorderPropertyTypesInEntityType( UUID entityTypeId, LinkedHashSet<UUID> propertyTypeIds ) {
        entityTypes.executeOnKey( entityTypeId, new ReorderPropertyTypesInEntityTypeProcessor( propertyTypeIds ) );
        EntityType entityType = getEntityType( entityTypeId );
        if ( entityType.getCategory().equals( SecurableObjectType.AssociationType ) ) {
            eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( entityTypeId ) ) );
        } else {
            eventBus.post( new EntityTypeCreatedEvent( entityType ) );
        }
    }

    @Override
    public void addPrimaryKeysToEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeIds ), "Some properties do not exist." );
        EntityType entityType = entityTypes.get( entityTypeId );
        checkNotNull( entityType, "No entity type with id {}", entityTypeId );
        Preconditions.checkArgument( entityType.getProperties().containsAll( propertyTypeIds ),
                "Entity type does not contain all the requested primary key property types." );

        entityTypes.executeOnKey( entityTypeId, new AddPrimaryKeysToEntityTypeProcessor( propertyTypeIds ) );

        entityType = entityTypes.get( entityTypeId );
        if ( entityType.getCategory().equals( SecurableObjectType.AssociationType ) ) {
            eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( entityTypeId ) ) );
        } else {
            eventBus.post( new EntityTypeCreatedEvent( entityType ) );
        }
    }

    @Override
    public void removePrimaryKeysFromEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeIds ), "Some properties do not exist." );
        EntityType entityType = entityTypes.get( entityTypeId );
        checkNotNull( entityType, "No entity type with id {}", entityTypeId );
        Preconditions.checkArgument( entityType.getProperties().containsAll( propertyTypeIds ),
                "Entity type does not contain all the requested primary key property types." );

        entityTypes.executeOnKey( entityTypeId, new RemovePrimaryKeysFromEntityTypeProcessor( propertyTypeIds ) );

        entityType = entityTypes.get( entityTypeId );
        if ( entityType.getCategory().equals( SecurableObjectType.AssociationType ) ) {
            eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( entityTypeId ) ) );
        } else {
            eventBus.post( new EntityTypeCreatedEvent( entityType ) );
        }
    }

    @Override
    public void addSrcEntityTypesToAssociationType( UUID associationTypeId, Set<UUID> entityTypeIds ) {
        Preconditions.checkArgument( checkEntityTypesExist( entityTypeIds ) );
        associationTypes.executeOnKey( associationTypeId,
                new AddSrcEntityTypesToAssociationTypeProcessor( entityTypeIds ) );
        eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( associationTypeId ) ) );
    }

    @Override
    public void addDstEntityTypesToAssociationType( UUID associationTypeId, Set<UUID> entityTypeIds ) {
        Preconditions.checkArgument( checkEntityTypesExist( entityTypeIds ) );
        associationTypes.executeOnKey( associationTypeId,
                new AddDstEntityTypesToAssociationTypeProcessor( entityTypeIds ) );
        eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( associationTypeId ) ) );
    }

    @Override
    public void removeSrcEntityTypesFromAssociationType( UUID associationTypeId, Set<UUID> entityTypeIds ) {
        Preconditions.checkArgument( checkEntityTypesExist( entityTypeIds ) );
        associationTypes.executeOnKey( associationTypeId,
                new RemoveSrcEntityTypesFromAssociationTypeProcessor( entityTypeIds ) );
        eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( associationTypeId ) ) );
    }

    @Override
    public void removeDstEntityTypesFromAssociationType( UUID associationTypeId, Set<UUID> entityTypeIds ) {
        Preconditions.checkArgument( checkEntityTypesExist( entityTypeIds ) );
        associationTypes.executeOnKey( associationTypeId,
                new RemoveDstEntityTypesFromAssociationTypeProcessor( entityTypeIds ) );
        eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( associationTypeId ) ) );
    }

    @Override
    public void updatePropertyTypeMetadata( UUID propertyTypeId, MetadataUpdate update ) {
        PropertyType propertyType = getPropertyType( propertyTypeId );
        boolean isFqnUpdated = update.getType().isPresent();

        if ( isFqnUpdated ) {
            aclKeyReservations.renameReservation( propertyTypeId, update.getType().get() );
            eventBus.post( new PropertyTypeCreatedEvent( propertyType ) );
        }
        propertyTypes.executeOnKey( propertyTypeId, new UpdatePropertyTypeMetadataProcessor( update ) );
        // get all entity sets containing the property type, and re-index them.
        entityTypeManager
                .getEntityTypesContainingPropertyTypesAsStream( ImmutableSet.of( propertyTypeId ) ).forEach( et -> {
            List<PropertyType> properties = Lists
                    .newArrayList( propertyTypes.getAll( et.getProperties() ).values() );
            getEntitySetIdsOfType( et.getId() ).forEach( entitySetId -> {
                if ( isFqnUpdated ) {
                    // add edm_unsync flag for materialized views
                    markMaterializedEntitySetDirtyWithEdmChanges( entitySetId );
                }
                eventBus.post( new PropertyTypesInEntitySetUpdatedEvent( entitySetId, properties, isFqnUpdated ) );
            } );
        } );

        eventBus.post( new PropertyTypeMetaDataUpdatedEvent( propertyType,
                update ) ); // currently not picked up by anything
    }

    @Override
    public void updateEntityTypeMetadata( UUID entityTypeId, MetadataUpdate update ) {
        if ( update.getType().isPresent() ) {
            aclKeyReservations.renameReservation( entityTypeId, update.getType().get() );
        }
        entityTypes.executeOnKey( entityTypeId, new UpdateEntityTypeMetadataProcessor( update ) );
        if ( !getEntityType( entityTypeId ).getCategory().equals( SecurableObjectType.AssociationType ) ) {
            eventBus.post( new EntityTypeCreatedEvent( getEntityType( entityTypeId ) ) );
        } else {
            eventBus.post( new AssociationTypeCreatedEvent( getAssociationType( entityTypeId ) ) );
        }
    }

    private void markMaterializedEntitySetDirtyWithEdmChanges( UUID entitySetId ) {
        eventBus.post( new MaterializedEntitySetEdmChangeEvent( entitySetId ) );
    }

    /**************
     * Validation
     **************/
    @Override
    public boolean checkPropertyTypesExist( Set<UUID> properties ) {
        return properties.stream().allMatch( propertyTypes::containsKey );
    }

    @Override
    public boolean checkPropertyTypeExists( FullQualifiedName fqn ) {
        final var typeId = getTypeAclKey( fqn );
        if ( typeId == null ) {
            return false;
        } else {
            return propertyTypes.containsKey( typeId );
        }
    }

    @Override
    public boolean checkPropertyTypeExists( UUID propertyTypeId ) {
        return propertyTypes.containsKey( propertyTypeId );
    }

    @Override
    public boolean checkEntityTypesExist( Set<UUID> entityTypeIds ) {
        return entityTypeIds.stream().allMatch( entityTypes::containsKey );
    }

    @Override
    public boolean checkEntityTypeExists( FullQualifiedName fqn ) {
        final var typeId = getTypeAclKey( fqn );
        if ( typeId == null ) {
            return false;
        } else {
            return entityTypes.containsKey( typeId );
        }
    }

    @Override
    public boolean checkEntityTypeExists( UUID entityTypeId ) {
        return entityTypes.containsKey( entityTypeId );
    }

    @Override
    public Collection<PropertyType> getPropertyTypes( Set<UUID> propertyIds ) {
        return propertyTypes.getAll( propertyIds ).values();
    }

    @Override
    public UUID getTypeAclKey( FullQualifiedName type ) {
        return Util.getSafely( aclKeys, Util.fqnToString( type ) );
    }

    @Override
    public PropertyType getPropertyType( UUID propertyTypeId ) {
        return Util.getSafely( propertyTypes, propertyTypeId );
    }

    @Override
    public FullQualifiedName getPropertyTypeFqn( UUID propertyTypeId ) {
        return Util.stringToFqn( Util.getSafely( names, propertyTypeId ) );
    }

    @Override
    public FullQualifiedName getEntityTypeFqn( UUID entityTypeId ) {
        return Util.stringToFqn( Util.getSafely( names, entityTypeId ) );
    }

    @Override
    public Map<FullQualifiedName, UUID> getFqnToIdMap( Set<FullQualifiedName> propertyTypeFqns ) {
        return aclKeys.getAll( Util.fqnToString( propertyTypeFqns ) ).entrySet().stream()
                .collect( Collectors.toMap(
                        e -> new FullQualifiedName( e.getKey() ),
                        e -> {
                            if ( e.getValue() == null ) {
                                throw new NullPointerException( "Property type " + e.getKey() + " does not exist" );
                            } else {
                                return e.getValue();
                            }
                        } )
                );
    }

    @Override
    public Map<UUID, PropertyType> getPropertyTypesAsMap( Set<UUID> propertyTypeIds ) {
        return propertyTypes.getAll( propertyTypeIds );
    }

    @Override
    public Map<UUID, EntityType> getEntityTypesAsMap( Set<UUID> entityTypeIds ) {
        return entityTypes.getAll( entityTypeIds );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <V> Map<UUID, V> fromPropertyTypes( Set<UUID> propertyTypeIds, EntryProcessor<UUID, PropertyType, V> ep ) {
        return propertyTypes.executeOnKeys( propertyTypeIds, ep );
    }

    @Override
    public Set<UUID> getPropertyTypeIdsOfEntityType( UUID entityTypeId ) {
        return getEntityType( entityTypeId ).getProperties();
    }

    @Override public Map<UUID, PropertyType> getPropertyTypesOfEntityType( UUID entityTypeId ) {
        return propertyTypes.getAll( getEntityType( entityTypeId ).getProperties() );
    }

    @Override
    public Set<UUID> getPropertyTypeIdsOfEntityTypeWithPIIField( UUID entityTypeId ) {
        return getPropertyTypeIdsOfEntityType( entityTypeId ).stream()
                .filter( ptId -> getPropertyType( ptId ).isPii() ).collect( Collectors.toSet() );
    }

    @Override
    public UUID createAssociationType( AssociationType associationType, UUID entityTypeId ) {
        final AssociationType existing = associationTypes.putIfAbsent( entityTypeId, associationType );

        if ( existing != null ) {
            logger.error(
                    "Inconsistency encountered in database. Verify that existing association types have all their acl keys reserved." );
        }

        eventBus.post( new AssociationTypeCreatedEvent( associationType ) );
        return entityTypeId;
    }

    @Override
    public AssociationType getAssociationType( UUID associationTypeId ) {
        AssociationType associationDetails = getAssociationTypeDetails( associationTypeId );
        Optional<EntityType> entityType = Optional.ofNullable(
                Util.getSafely( entityTypes, associationTypeId ) );
        return new AssociationType(
                entityType,
                associationDetails.getSrc(),
                associationDetails.getDst(),
                associationDetails.isBidirectional() );
    }

    private AssociationType getAssociationTypeDetails( UUID associationTypeId ) {
        return checkNotNull(
                Util.getSafely( associationTypes, associationTypeId ),
                "Association type of id %s does not exists.",
                associationTypeId.toString() );
    }

    @Override
    public AssociationType getAssociationTypeSafe( UUID associationTypeId ) {
        Optional<AssociationType> associationDetails = Optional
                .ofNullable( Util.getSafely( associationTypes, associationTypeId ) );
        Optional<EntityType> entityType = Optional.ofNullable(
                Util.getSafely( entityTypes, associationTypeId ) );
        if ( associationDetails.isEmpty() || entityType.isEmpty() ) { return null; }
        return new AssociationType(
                entityType,
                associationDetails.get().getSrc(),
                associationDetails.get().getDst(),
                associationDetails.get().isBidirectional() );
    }

    @Override
    public void deleteAssociationType( UUID associationTypeId ) {
        AssociationType associationType = getAssociationType( associationTypeId );
        if ( associationType.getAssociationEntityType() == null ) {
            logger.error( "Inconsistency found: association type of id {} has no associated entity type",
                    associationTypeId );
            throw new IllegalStateException( "Failed to delete association type of id " + associationTypeId );
        }
        deleteEntityType( associationType.getAssociationEntityType().getId() );
        associationTypes.delete( associationTypeId );
        eventBus.post( new AssociationTypeDeletedEvent( associationTypeId ) );
    }

    @Override
    public AssociationDetails getAssociationDetails( UUID associationTypeId ) {
        AssociationType associationType = getAssociationTypeDetails( associationTypeId );
        LinkedHashSet<EntityType> srcEntityTypes = associationType.getSrc()
                .stream()
                .map( this::getEntityType )
                .collect( Collectors.toCollection( LinkedHashSet::new ) );
        LinkedHashSet<EntityType> dstEntityTypes = associationType.getDst()
                .stream()
                .map( this::getEntityType )
                .collect( Collectors.toCollection( LinkedHashSet::new ) );
        return new AssociationDetails( srcEntityTypes, dstEntityTypes, associationType.isBidirectional() );
    }

    @Override
    public Iterable<EntityType> getAvailableAssociationTypesForEntityType( UUID entityTypeId ) {
        return entityTypeManager.getAssociationIdsForEntityType( entityTypeId ).map( entityTypes::get )
                .collect( Collectors.toList() );
    }

    private void createOrUpdatePropertyTypeWithFqn( PropertyType pt, FullQualifiedName fqn ) {
        PropertyType existing = getPropertyType( pt.getId() );
        if ( existing == null ) { createPropertyTypeIfNotExists( pt ); } else {
            Optional<String> optionalTitleUpdate = ( pt.getTitle().equals( existing.getTitle() ) )
                    ? Optional.empty() : Optional.of( pt.getTitle() );
            Optional<String> optionalDescriptionUpdate = ( pt.getDescription().equals( existing.getDescription() ) )
                    ? Optional.empty() : Optional.of( pt.getDescription() );
            Optional<FullQualifiedName> optionalFqnUpdate = ( fqn.equals( existing.getType() ) )
                    ? Optional.empty() : Optional.of( fqn );
            Optional<Boolean> optionalPiiUpdate = ( pt.isPii() == existing.isPii() )
                    ? Optional.empty() : Optional.of( pt.isPii() );
            updatePropertyTypeMetadata( existing.getId(), new MetadataUpdate(
                    optionalTitleUpdate,
                    optionalDescriptionUpdate,
                    Optional.empty(),
                    Optional.empty(),
                    optionalFqnUpdate,
                    optionalPiiUpdate,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty() ) );
        }
    }

    private void createOrUpdatePropertyType( PropertyType pt ) {
        createOrUpdatePropertyTypeWithFqn( pt, pt.getType() );
    }

    private void createOrUpdateEntityTypeWithFqn( EntityType et, FullQualifiedName fqn ) {
        EntityType existing = getEntityTypeSafe( et.getId() );
        if ( existing == null ) { createEntityType( et ); } else {
            Optional<String> optionalTitleUpdate = ( et.getTitle().equals( existing.getTitle() ) )
                    ? Optional.empty() : Optional.of( et.getTitle() );
            Optional<String> optionalDescriptionUpdate = ( et.getDescription().equals( existing.getDescription() ) )
                    ? Optional.empty() : Optional.of( et.getDescription() );
            Optional<FullQualifiedName> optionalFqnUpdate = ( fqn.equals( existing.getType() ) )
                    ? Optional.empty() : Optional.of( fqn );
            Optional<LinkedHashMultimap<UUID, String>> optionalPropertyTagsUpdate = ( et.getPropertyTags()
                    .equals( existing.getPropertyTags() ) )
                    ? Optional.empty() : Optional.of( existing.getPropertyTags() );
            updateEntityTypeMetadata( existing.getId(), new MetadataUpdate(
                    optionalTitleUpdate,
                    optionalDescriptionUpdate,
                    Optional.empty(),
                    Optional.empty(),
                    optionalFqnUpdate,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    optionalPropertyTagsUpdate,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty() ) );
            if ( !et.getProperties().equals( existing.getProperties() ) ) {
                addPropertyTypesToEntityType( existing.getId(), et.getProperties() );
            }
        }
    }

    private void createOrUpdateEntityType( EntityType et ) {
        createOrUpdateEntityTypeWithFqn( et, et.getType() );
    }

    private void createOrUpdateAssociationTypeWithFqn( AssociationType at, FullQualifiedName fqn ) {
        EntityType et = at.getAssociationEntityType();
        AssociationType existing = getAssociationTypeSafe( et.getId() );
        if ( existing == null ) {
            createOrUpdateEntityTypeWithFqn( et, fqn );
            createAssociationType( at, getTypeAclKey( et.getType() ) );
        } else {
            if ( !existing.getSrc().equals( at.getSrc() ) ) {
                addSrcEntityTypesToAssociationType( existing.getAssociationEntityType().getId(), at.getSrc() );
            }
            if ( !existing.getDst().equals( at.getDst() ) ) {
                addDstEntityTypesToAssociationType( existing.getAssociationEntityType().getId(), at.getDst() );
            }
        }
    }

    private void createOrUpdateAssociationType( AssociationType at ) {
        createOrUpdateAssociationTypeWithFqn( at, at.getAssociationEntityType().getType() );
    }

    private void resolveFqnCycles(
            UUID id,
            SecurableObjectType objectType,
            Map<UUID, PropertyType> propertyTypesById,
            Map<UUID, EntityType> entityTypesById,
            Map<UUID, AssociationType> associationTypesById ) {

        switch ( objectType ) {
            case PropertyTypeInEntitySet:
                createOrUpdatePropertyType( propertyTypesById.get( id ) );
                break;
            case EntityType:
                createOrUpdateEntityType( entityTypesById.get( id ) );
                break;
            case AssociationType:
                createOrUpdateAssociationType( associationTypesById.get( id ) );
                break;
            default:
                break;
        }
    }

    @Override
    public EntityDataModelDiff getEntityDataModelDiff( EntityDataModel edm ) {
        return getEntityDataModelDiffAndFqnLists( edm ).getLeft();
    }

    private Pair<EntityDataModelDiff, Set<List<UUID>>> getEntityDataModelDiffAndFqnLists( EntityDataModel edm ) {
        ConcurrentSkipListSet<PropertyType> conflictingPropertyTypes = new ConcurrentSkipListSet<>( Comparator
                .comparing( propertyType -> propertyType.getType().toString() ) );
        ConcurrentSkipListSet<EntityType> conflictingEntityTypes = new ConcurrentSkipListSet<>( Comparator
                .comparing( entityType -> entityType.getType().toString() ) );
        ConcurrentSkipListSet<AssociationType> conflictingAssociationTypes = new ConcurrentSkipListSet<>( Comparator
                .comparing( associationType -> associationType.getAssociationEntityType().getType().toString() ) );

        ConcurrentSkipListSet<PropertyType> updatedPropertyTypes = new ConcurrentSkipListSet<>( Comparator
                .comparing( propertyType -> propertyType.getType().toString() ) );
        ConcurrentSkipListSet<EntityType> updatedEntityTypes = new ConcurrentSkipListSet<>( Comparator
                .comparing( entityType -> entityType.getType().toString() ) );
        ConcurrentSkipListSet<AssociationType> updatedAssociationTypes = new ConcurrentSkipListSet<>( Comparator
                .comparing( associationType -> associationType.getAssociationEntityType().getType().toString() ) );
        ConcurrentSkipListSet<Schema> updatedSchemas = new ConcurrentSkipListSet<>( Comparator
                .comparing( schema -> schema.getFqn().toString() ) );

        Map<UUID, FullQualifiedName> idsToFqns = Maps.newHashMap();
        Map<UUID, SecurableObjectType> idsToTypes = Maps.newHashMap();
        Map<UUID, PropertyType> propertyTypesById = Maps.newHashMap();
        Map<UUID, EntityType> entityTypesById = Maps.newHashMap();
        Map<UUID, AssociationType> associationTypesById = Maps.newHashMap();

        edm.getPropertyTypes().forEach( pt -> {
            PropertyType existing = getPropertyType( pt.getId() );
            if ( existing == null ) { updatedPropertyTypes.add( pt ); } else if ( !existing.equals( pt ) ) {
                if ( !pt.getDatatype().equals( existing.getDatatype() )
                        || !pt.getAnalyzer().equals( existing.getAnalyzer() ) ) {
                    conflictingPropertyTypes.add( pt );
                } else if ( !pt.getType().equals( existing.getType() ) ) {
                    idsToTypes.put( pt.getId(), SecurableObjectType.PropertyTypeInEntitySet );
                    idsToFqns.put( pt.getId(), pt.getType() );
                    propertyTypesById.put( pt.getId(), pt );
                } else if ( !pt.getTitle().equals( existing.getTitle() )
                        || !pt.getDescription().equals( existing.getDescription() )
                        || !pt.isPii() == existing.isPii() ) { updatedPropertyTypes.add( pt ); }
            }
        } );

        edm.getEntityTypes().forEach( et -> {
            EntityType existing = getEntityTypeSafe( et.getId() );
            if ( existing == null ) { updatedEntityTypes.add( et ); } else if ( !existing.equals( et ) ) {
                if ( !et.getBaseType().equals( existing.getBaseType() )
                        || !et.getCategory().equals( existing.getCategory() )
                        || !et.getKey().equals( existing.getKey() ) ) {
                    conflictingEntityTypes.add( et );
                } else if ( !et.getType().equals( existing.getType() ) ) {
                    idsToTypes.put( et.getId(), SecurableObjectType.EntityType );
                    idsToFqns.put( et.getId(), et.getType() );
                    entityTypesById.put( et.getId(), et );
                } else if ( !et.getTitle().equals( existing.getTitle() )
                        || !et.getDescription().equals( existing.getDescription() )
                        || !et.getProperties().equals( existing.getProperties() ) ) { updatedEntityTypes.add( et ); }
            }
        } );

        edm.getAssociationTypes().forEach( at -> {
            EntityType atEntityType = at.getAssociationEntityType();
            AssociationType existing = getAssociationTypeSafe( atEntityType.getId() );
            if ( existing == null ) { updatedAssociationTypes.add( at ); } else if ( !existing.equals( at ) ) {
                if ( !at.isBidirectional() == existing.isBidirectional()
                        || !atEntityType.getBaseType().equals( existing.getAssociationEntityType().getBaseType() )
                        || !atEntityType.getCategory().equals( existing.getAssociationEntityType().getCategory() )
                        || !atEntityType.getKey().equals( existing.getAssociationEntityType().getKey() ) ) {
                    conflictingAssociationTypes.add( at );
                } else if ( !atEntityType.getType().equals( existing.getAssociationEntityType().getType() ) ) {
                    idsToTypes.put( atEntityType.getId(), SecurableObjectType.AssociationType );
                    idsToFqns.put( atEntityType.getId(), atEntityType.getType() );
                    associationTypesById.put( atEntityType.getId(), at );
                } else if ( !atEntityType.getTitle().equals( existing.getAssociationEntityType().getTitle() )
                        || !atEntityType.getDescription().equals( existing.getAssociationEntityType().getDescription() )
                        || !atEntityType.getProperties().equals( existing.getAssociationEntityType().getProperties() )
                        || !at.getSrc().equals( existing.getSrc() )
                        || !at.getDst().equals( existing.getDst() ) ) { updatedAssociationTypes.add( at ); }
            }
        } );
        edm.getSchemas().forEach( schema -> {
            Schema existing = null;
            if ( schemaManager.checkSchemaExists( schema.getFqn() ) ) {
                existing = schemaManager.getSchema( schema.getFqn().getNamespace(), schema.getFqn().getName() );
            }
            if ( !schema.equals( existing ) ) { updatedSchemas.add( schema ); }
        } );

        List<Set<List<UUID>>> cyclesAndConflicts = checkFqnDiffs( idsToFqns );
        Map<UUID, Boolean> idsToOutcome = Maps.newHashMap();
        cyclesAndConflicts.get( 0 ).forEach( idList -> idList.forEach( id -> idsToOutcome.put( id, true ) ) );
        cyclesAndConflicts.get( 1 ).forEach( idList -> idList.forEach( id -> idsToOutcome.put( id, false ) ) );

        idsToOutcome.forEach( ( id, shouldResolve ) -> {
            switch ( idsToTypes.get( id ) ) {
                case PropertyTypeInEntitySet:
                    if ( shouldResolve ) { updatedPropertyTypes.add( propertyTypesById.get( id ) ); } else {
                        conflictingPropertyTypes.add( propertyTypesById.get( id ) );
                    }
                    break;
                case EntityType:
                    if ( shouldResolve ) { updatedEntityTypes.add( entityTypesById.get( id ) ); } else {
                        conflictingEntityTypes.add( entityTypesById.get( id ) );
                    }
                    break;
                case AssociationType:
                    if ( shouldResolve ) { updatedAssociationTypes.add( associationTypesById.get( id ) ); } else {
                        conflictingAssociationTypes.add( associationTypesById.get( id ) );
                    }
                    break;
                default:
                    break;
            }
        } );

        EntityDataModel edmDiff = new EntityDataModel(
                Sets.newHashSet(),
                updatedSchemas,
                updatedEntityTypes,
                updatedAssociationTypes,
                updatedPropertyTypes );

        EntityDataModel conflicts = null;

        if ( !conflictingPropertyTypes.isEmpty() || !conflictingEntityTypes.isEmpty()
                || !conflictingAssociationTypes.isEmpty() ) {
            conflicts = new EntityDataModel(
                    Sets.newHashSet(),
                    Sets.newHashSet(),
                    conflictingEntityTypes,
                    conflictingAssociationTypes,
                    conflictingPropertyTypes );
        }

        EntityDataModelDiff diff = new EntityDataModelDiff( edmDiff, Optional.ofNullable( conflicts ) );
        Set<List<UUID>> cycles = cyclesAndConflicts.get( 0 );
        return Pair.of( diff, cycles );

    }

    private List<Set<List<UUID>>> checkFqnDiffs( Map<UUID, FullQualifiedName> idToType ) {
        Set<UUID> conflictingIdsToFqns = Sets.newHashSet();
        Map<UUID, FullQualifiedName> updatedIdToFqn = Maps.newHashMap();
        SetMultimap<FullQualifiedName, UUID> internalFqnToId = HashMultimap.create();
        Map<FullQualifiedName, UUID> externalFqnToId = Maps.newHashMap();

        idToType.forEach( ( id, fqn ) -> {
            UUID conflictId = aclKeys.get( fqn.toString() );
            updatedIdToFqn.put( id, fqn );
            internalFqnToId.put( fqn, id );
            conflictingIdsToFqns.add( id );
            if ( conflictId != null ) { externalFqnToId.put( fqn, conflictId ); }
        } );

        return resolveFqnCyclesLists( conflictingIdsToFqns, updatedIdToFqn, internalFqnToId, externalFqnToId );
    }

    private List<Set<List<UUID>>> resolveFqnCyclesLists(
            Set<UUID> conflictingIdsToFqns,
            Map<UUID, FullQualifiedName> updatedIdToFqn,
            SetMultimap<FullQualifiedName, UUID> internalFqnToId,
            Map<FullQualifiedName, UUID> externalFqnToId ) {

        Set<List<UUID>> result = Sets.newHashSet();
        Set<List<UUID>> conflicts = Sets.newHashSet();

        while ( !conflictingIdsToFqns.isEmpty() ) {
            UUID initialId = conflictingIdsToFqns.iterator().next();
            List<UUID> conflictingIdsViewed = Lists.newArrayList();

            UUID id = initialId;

            boolean shouldReject = false;
            boolean shouldResolve = false;
            while ( !shouldReject && !shouldResolve ) {
                conflictingIdsViewed.add( 0, id );
                FullQualifiedName fqn = updatedIdToFqn.get( id );
                Set<UUID> idsForFqn = internalFqnToId.get( fqn );
                if ( idsForFqn.size() > 1 ) { shouldReject = true; } else {
                    id = externalFqnToId.get( fqn );
                    if ( id == null || id.equals( initialId ) ) { shouldResolve = true; } else if ( !updatedIdToFqn
                            .containsKey( id ) ) { shouldReject = true; }
                }
            }

            if ( shouldReject ) { conflicts.add( conflictingIdsViewed ); } else { result.add( conflictingIdsViewed ); }
            conflictingIdsToFqns.removeAll( conflictingIdsViewed );
        }
        return Lists.newArrayList( result, conflicts );
    }

    @Override
    public Map<UUID, EntityTypePropertyMetadata> getAllEntityTypePropertyMetadata( UUID entityTypeId ) {
        return entityTypePropertyMetadata
                .entrySet( Predicates.equal( EntityTypePropertyMetadataMapstore.ENTITY_TYPE_INDEX, entityTypeId ) )
                .stream()
                .collect( Collectors.toMap( entry -> entry.getKey().getPropertyTypeId(), Map.Entry::getValue ) );
    }

    @Override
    public EntityTypePropertyMetadata getEntityTypePropertyMetadata( UUID entityTypeId, UUID propertyTypeId ) {
        EntityTypePropertyKey key = new EntityTypePropertyKey( entityTypeId, propertyTypeId );
        if ( !entityTypePropertyMetadata.containsKey( key ) ) {
            setupDefaultEntityTypePropertyMetadata( entityTypeId );
        }
        return entityTypePropertyMetadata.get( key );
    }

    @Override
    public void updateEntityTypePropertyMetadata( UUID entitySetId, UUID propertyTypeId, MetadataUpdate update ) {
        EntityTypePropertyKey key = new EntityTypePropertyKey( entitySetId, propertyTypeId );
        entityTypePropertyMetadata.executeOnKey( key, new UpdateEntityTypePropertyMetadataProcessor( update ) );
    }

    @Override public EntityDataModel getEntityDataModel() {
        final List<Schema> schemas = Lists.newArrayList( schemaManager.getAllSchemas() );
        final List<EntityType> entityTypes = Lists.newArrayList( getEntityTypesStrict() );
        final List<AssociationType> associationTypes = Lists.newArrayList( getAssociationTypes() );
        final List<PropertyType> propertyTypes = Lists.newArrayList( getPropertyTypes() );
        final Set<String> namespaces = new TreeSet<>();
        getEntityTypes().forEach( entityType -> namespaces.add( entityType.getType().getNamespace() ) );
        getPropertyTypes().forEach( propertyType -> namespaces.add( propertyType.getType().getNamespace() ) );

        schemas.sort( Comparator.comparing( schema -> schema.getFqn().toString() ) );
        entityTypes.sort( Comparator.comparing( entityType -> entityType.getType().toString() ) );
        associationTypes.sort( Comparator
                .comparing( associationType -> associationType.getAssociationEntityType().getType().toString() ) );
        propertyTypes.sort( Comparator.comparing( propertyType -> propertyType.getType().toString() ) );

        return new EntityDataModel(
                namespaces,
                schemas,
                entityTypes,
                associationTypes,
                propertyTypes );
    }

    @Override
    public void setEntityDataModel( EntityDataModel edm ) {
        Pair<EntityDataModelDiff, Set<List<UUID>>> diffAndFqnCycles = getEntityDataModelDiffAndFqnLists( edm );
        EntityDataModelDiff diff = diffAndFqnCycles.getLeft();
        Set<List<UUID>> fqnCycles = diffAndFqnCycles.getRight();
        if ( diff.getConflicts().isPresent() ) {
            throw new IllegalArgumentException(
                    "Unable to update entity data model: please resolve conflicts before importing." );
        }

        Map<UUID, SecurableObjectType> idToType = Maps.newHashMap();
        Map<UUID, PropertyType> propertyTypesById = Maps.newHashMap();
        Map<UUID, EntityType> entityTypesById = Maps.newHashMap();
        Map<UUID, AssociationType> associationTypesById = Maps.newHashMap();

        diff.getDiff().getPropertyTypes().forEach( pt -> {
            idToType.put( pt.getId(), SecurableObjectType.PropertyTypeInEntitySet );
            propertyTypesById.put( pt.getId(), pt );
        } );
        diff.getDiff().getEntityTypes().forEach( et -> {
            idToType.put( et.getId(), SecurableObjectType.EntityType );
            entityTypesById.put( et.getId(), et );
        } );
        diff.getDiff().getAssociationTypes().forEach( at -> {
            idToType.put( at.getAssociationEntityType().getId(), SecurableObjectType.AssociationType );
            associationTypesById.put( at.getAssociationEntityType().getId(), at );
        } );

        Set<UUID> updatedIds = Sets.newHashSet();

        fqnCycles.forEach( cycle ->
                cycle.forEach( id -> {
                    resolveFqnCycles( id,
                            idToType.get( id ),
                            propertyTypesById,
                            entityTypesById,
                            associationTypesById );
                    updatedIds.add( id );
                } )
        );

        diff.getDiff().getSchemas().forEach( schemaManager::createOrUpdateSchemas );

        diff.getDiff().getPropertyTypes().forEach( pt -> {
            if ( !updatedIds.contains( pt.getId() ) ) {
                createOrUpdatePropertyType( pt );
                eventBus.post( new PropertyTypeCreatedEvent( pt ) );
            }
        } );

        diff.getDiff().getEntityTypes().forEach( et -> {
            if ( !updatedIds.contains( et.getId() ) ) {
                createOrUpdateEntityType( et );
                eventBus.post( new EntityTypeCreatedEvent( et ) );
            }
        } );

        diff.getDiff().getAssociationTypes().forEach( at -> {
            if ( !updatedIds.contains( at.getAssociationEntityType().getId() ) ) {
                createOrUpdateAssociationType( at );
                eventBus.post( new AssociationTypeCreatedEvent( at ) );
            }
        } );
    }

    @Override
    public Set<UUID> getAllLinkingEntitySetIdsForEntitySet( UUID entitySetId ) {
        return entitySets.keySet( Predicates.equal( EntitySetMapstore.LINKED_ENTITY_SET_INDEX, entitySetId ) );
    }

    /* Entity set related functions */
    private Collection<UUID> getEntitySetIdsOfType( UUID entityTypeId ) {
        return entitySets.keySet( Predicates.equal( EntitySetMapstore.ENTITY_TYPE_ID_INDEX, entityTypeId ) );
    }

    private Collection<EntitySet> getEntitySetsOfType( UUID entityTypeId ) {
        return entitySets.values( Predicates.equal( EntitySetMapstore.ENTITY_TYPE_ID_INDEX, entityTypeId ) );
    }
}
