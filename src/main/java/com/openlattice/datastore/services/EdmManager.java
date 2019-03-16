

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

import com.codahale.metrics.annotation.Timed;
import com.hazelcast.map.EntryProcessor;
import com.openlattice.auditing.AuditRecordEntitySetsManager;
import com.openlattice.authorization.Principal;
import com.openlattice.data.PropertyUsageSummary;
import com.openlattice.edm.EntityDataModel;
import com.openlattice.edm.EntityDataModelDiff;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.edm.type.AssociationDetails;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.ComplexType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.EnumType;
import com.openlattice.edm.type.PropertyType;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

public interface EdmManager {
    void clearTables();

    PropertyType getPropertyType( FullQualifiedName propertyTypeFqn );

    PropertyType getPropertyType( UUID propertyTypeId );

    void createPropertyTypeIfNotExists( PropertyType propertyType );

    void deletePropertyType( UUID propertyTypeId );

    void forceDeletePropertyType( UUID propertyTypeId );

    Iterable<PropertyType> getPropertyTypesInNamespace( String namespace );

    Iterable<PropertyType> getPropertyTypes();

    @Timed
    Map<UUID, PropertyType> getPropertyTypesForEntitySet( UUID entitySetId );

    Set<UUID> getAllPropertyTypeIds();

    Iterable<PropertyUsageSummary> getPropertyUsageSummary( UUID propertyTypeId );

    void createEntitySet( Principal principal, EntitySet entitySet );

    // Warning: This method is used only in creating linked entity set, where entity set owner may not own all the
    // property types.
    void createEntitySet( Principal principal, EntitySet entitySet, Set<UUID> ownablePropertyTypes );

    EntitySet getEntitySet( UUID entitySetId );

    Iterable<EntitySet> getEntitySets();

    void deleteEntitySet( UUID entitySetId );

    int addLinkedEntitySets( UUID entitySetId, Set<UUID> linkedEntitySets );

    int removeLinkedEntitySets( UUID entitySetId, Set<UUID> linkedEntitySets );

    Set<EntitySet> getLinkedEntitySets( UUID entitySetId );

    Set<UUID> getLinkedEntitySetIds( UUID entitySetId );

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

    void updateEntitySetMetadata( UUID typeId, MetadataUpdate update );

    // Helper methods to check existence
    boolean checkPropertyTypesExist( Set<UUID> properties );

    boolean checkPropertyTypeExists( FullQualifiedName fqn );

    boolean checkPropertyTypeExists( UUID propertyTypeId );

    boolean checkEntityTypesExist( Set<UUID> entityTypeIds );

    boolean checkEntityTypeExists( FullQualifiedName fqn );

    boolean checkEntityTypeExists( UUID entityTypeId );

    boolean checkEntitySetExists( String name );

    Collection<PropertyType> getPropertyTypes( Set<UUID> properties );

    Set<UUID> getAclKeys( Set<?> fqnsOrNames );

    Map<String, UUID> getAclKeyIds( Set<String> aclNames );

    UUID getTypeAclKey( FullQualifiedName fqns );

    Set<UUID> getEntityTypeUuids( Set<FullQualifiedName> fqns );

    UUID getPropertyTypeId( FullQualifiedName fqn );

    Set<UUID> getPropertyTypeUuids( Set<FullQualifiedName> fqns );

    EntityType getEntityType( FullQualifiedName type );

    EntitySet getEntitySet( String entitySetName );

    FullQualifiedName getPropertyTypeFqn( UUID propertyTypeId );

    FullQualifiedName getEntityTypeFqn( UUID entityTypeId );

    Map<FullQualifiedName, UUID> getFqnToIdMap( Set<FullQualifiedName> propertyTypeFqns );

    Map<UUID, PropertyType> getPropertyTypesAsMap( Set<UUID> propertyTypeIds );

    Map<UUID, EntityType> getEntityTypesAsMap( Set<UUID> entityTypeIds );

    Map<UUID, EntitySet> getEntitySetsAsMap( Set<UUID> entitySetIds );

    <V> Map<UUID, V> fromPropertyTypes( Set<UUID> propertyTypeIds, EntryProcessor<UUID, PropertyType> ep );

    Set<UUID> getPropertyTypeIdsOfEntityType( UUID entityTypeId );

    Map<UUID, PropertyType> getPropertyTypesOfEntityType( UUID entityTypeId );

    Set<UUID> getPropertyTypeIdsOfEntityTypeWithPIIField( UUID entityTypeId );

    EntityType getEntityTypeByEntitySetId( UUID entitySetId );

    void createEnumTypeIfNotExists( EnumType enumType );

    Stream<EnumType> getEnumTypes();

    EnumType getEnumType( UUID enumTypeId );

    void deleteEnumType( UUID enumTypeId );

    void createComplexTypeIfNotExists( ComplexType complexType );

    Stream<ComplexType> getComplexTypes();

    ComplexType getComplexType( UUID complexTypeId );

    void deleteComplexType( UUID complexTypeId );

    Set<EntityType> getEntityTypeHierarchy( UUID entityTypeId );

    Set<ComplexType> getComplexTypeHierarchy( UUID complexTypeId );

    UUID createAssociationType( AssociationType associationType, UUID entityTypeId );

    AssociationType getAssociationType( UUID associationTypeId );

    AssociationType getAssociationTypeSafe( UUID associationTypeId );

    void deleteAssociationType( UUID associationTypeId );

    AssociationDetails getAssociationDetails( UUID associationTypeId );

    Iterable<EntityType> getAvailableAssociationTypesForEntityType( UUID entityTypeId );

    EntityDataModelDiff getEntityDataModelDiff( EntityDataModel edm );

    Map<UUID, EntitySetPropertyMetadata> getAllEntitySetPropertyMetadata( UUID entitySetId );

    Map<UUID, Map<UUID, EntitySetPropertyMetadata>> getAllEntitySetPropertyMetadataForIds( Set<UUID> entitySetIds );

    EntitySetPropertyMetadata getEntitySetPropertyMetadata( UUID entitySetId, UUID propertyTypeId );

    void updateEntitySetPropertyMetadata( UUID entitySetId, UUID propertyTypeId, MetadataUpdate update );

    EntityDataModel getEntityDataModel();

    void setEntityDataModel( EntityDataModel edm );

    Collection<EntitySet> getEntitySetsOfType( UUID entityTypeId );

    Set<UUID> getEntitySetsForOrganization( UUID organizationId );

    AuditRecordEntitySetsManager getAuditRecordEntitySetsManager();
}