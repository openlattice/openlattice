

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

import com.hazelcast.map.EntryProcessor;
import com.openlattice.edm.EntityDataModel;
import com.openlattice.edm.EntityDataModelDiff;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.edm.type.AssociationDetails;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.EntityTypePropertyMetadata;
import com.openlattice.edm.type.PropertyType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface EdmManager {

    PropertyType getPropertyType( FullQualifiedName propertyTypeFqn );

    PropertyType getPropertyType( UUID propertyTypeId );

    void createPropertyTypeIfNotExists( PropertyType propertyType );

    void deletePropertyType( UUID propertyTypeId );

    void forceDeletePropertyType( UUID propertyTypeId );

    Iterable<PropertyType> getPropertyTypesInNamespace( String namespace );

    Iterable<PropertyType> getPropertyTypes();

    Set<UUID> getAllPropertyTypeIds();

    void createEntityType( EntityType objectType );

    EntityType getEntityType( String namespace, String name );

    Iterable<EntityType> getEntityTypes();

    Iterable<EntityType> getEntityTypesStrict();

    Iterable<EntityType> getAssociationEntityTypes();

    Iterable<AssociationType> getAssociationTypes();

    void deleteEntityType( UUID entityTypeId );

    EntityType getEntityType( UUID entityTypeId );

    EntityType getEntityTypeSafe( UUID entityTypeId );

    void addPropertyTypesToEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds );

    void removePropertyTypesFromEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds );

    void addPrimaryKeysToEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds );

    void removePrimaryKeysFromEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds );

    void forceRemovePropertyTypesFromEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds );

    void reorderPropertyTypesInEntityType( UUID entityTypeId, LinkedHashSet<UUID> propertyTypeIds );

    void addSrcEntityTypesToAssociationType( UUID associationTypeId, Set<UUID> entityTypeIds );

    void addDstEntityTypesToAssociationType( UUID associationTypeId, Set<UUID> entityTypeIds );

    void removeSrcEntityTypesFromAssociationType( UUID associationTypeId, Set<UUID> entityTypeIds );

    void removeDstEntityTypesFromAssociationType( UUID associationTypeId, Set<UUID> entityTypeIds );

    void updatePropertyTypeMetadata( UUID typeId, MetadataUpdate update );

    void updateEntityTypeMetadata( UUID typeId, MetadataUpdate update );

    // Helper methods to check existence
    boolean checkPropertyTypesExist( Set<UUID> properties );

    boolean checkPropertyTypeExists( FullQualifiedName fqn );

    boolean checkPropertyTypeExists( UUID propertyTypeId );

    boolean checkEntityTypesExist( Set<UUID> entityTypeIds );

    boolean checkEntityTypeExists( FullQualifiedName fqn );

    boolean checkEntityTypeExists( UUID entityTypeId );

    Collection<PropertyType> getPropertyTypes( Set<UUID> properties );

    Set<UUID> getAclKeys( Set<?> fqnsOrNames );

    Map<String, UUID> getAclKeyIds( Set<String> aclNames );

    UUID getTypeAclKey( FullQualifiedName fqns );

    Set<UUID> getEntityTypeUuids( Set<FullQualifiedName> fqns );

    UUID getPropertyTypeId( FullQualifiedName fqn );

    Set<UUID> getPropertyTypeUuids( Set<FullQualifiedName> fqns );

    AssociationType getAssociationType( FullQualifiedName typeFqn );

    EntityType getEntityType( FullQualifiedName type );

    @Nullable
    EntityType getEntityTypeSafe( FullQualifiedName typeFqn );

    FullQualifiedName getPropertyTypeFqn( UUID propertyTypeId );

    FullQualifiedName getEntityTypeFqn( UUID entityTypeId );

    Map<FullQualifiedName, UUID> getFqnToIdMap( Set<FullQualifiedName> propertyTypeFqns );

    Map<UUID, PropertyType> getPropertyTypesAsMap( Set<UUID> propertyTypeIds );

    Map<UUID, EntityType> getEntityTypesAsMap( Set<UUID> entityTypeIds );

    <V> Map<UUID, V> fromPropertyTypes( Set<UUID> propertyTypeIds, EntryProcessor<UUID, PropertyType, V> ep );

    Set<UUID> getPropertyTypeIdsOfEntityType( UUID entityTypeId );

    Map<UUID, PropertyType> getPropertyTypesOfEntityType( UUID entityTypeId );

    Set<UUID> getPropertyTypeIdsOfEntityTypeWithPIIField( UUID entityTypeId );

    Set<EntityType> getEntityTypeHierarchy( UUID entityTypeId );

    UUID createAssociationType( AssociationType associationType, UUID entityTypeId );

    AssociationType getAssociationType( UUID associationTypeId );

    AssociationType getAssociationTypeSafe( UUID associationTypeId );

    void deleteAssociationType( UUID associationTypeId );

    AssociationDetails getAssociationDetails( UUID associationTypeId );

    Iterable<EntityType> getAvailableAssociationTypesForEntityType( UUID entityTypeId );

    EntityDataModelDiff getEntityDataModelDiff( EntityDataModel edm );

    EntityDataModel getEntityDataModel();

    void setEntityDataModel( EntityDataModel edm );

    void updateEntityTypePropertyMetadata( UUID entityTypeId, UUID propertyTypeId, MetadataUpdate update );

    EntityTypePropertyMetadata getEntityTypePropertyMetadata( UUID entityTypeId, UUID propertyTypeId );

    Map<UUID, EntityTypePropertyMetadata> getAllEntityTypePropertyMetadata( UUID entityTypeId );

    Set<UUID> getAllLinkingEntitySetIdsForEntitySet( UUID entitySetId );

    void ensureEntityTypeExists( UUID entityTypeId );

    void ensurePropertyTypeExists( UUID propertyTypeId );
}