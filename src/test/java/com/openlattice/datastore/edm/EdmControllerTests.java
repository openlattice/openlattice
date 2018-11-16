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

package com.openlattice.datastore.edm;

import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.graph.query.GraphQueryState.Option;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.openlattice.datastore.IntegrationTestsBootstrap;

import com.openlattice.edm.type.ComplexType;
import com.openlattice.edm.type.EnumType;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openlattice.datastore.authentication.AuthenticatedRestCallsTest;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.EntityDataModel;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.mapstores.TestDataFactory;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class EdmControllerTests extends IntegrationTestsBootstrap {
    private final static Logger logger = LoggerFactory.getLogger( AuthenticatedRestCallsTest.class );
    private final        EdmApi edm    = getApiAdmin( EdmApi.class );

    public PropertyType createPropertyType() {
        PropertyType expected = TestDataFactory.propertyType();
        UUID propertyTypeId = edm.createPropertyType( expected );

        Assert.assertNotNull( "Property type creation returned null value.", propertyTypeId );

        PropertyType actual = edm.getPropertyType( propertyTypeId );

        Assert.assertNotNull( "Property type retrieval returned null value.", actual );
        Assert.assertEquals( "Created and retrieved property type don't match", expected, actual );

        return actual;
    }

    public EntityType createEntityType() {
        PropertyType p1 = createPropertyType();
        PropertyType k = createPropertyType();
        PropertyType p2 = createPropertyType();

        EntityType expected = TestDataFactory.entityType( k );
        expected.removePropertyTypes( expected.getProperties() );
        expected.addPropertyTypes( ImmutableSet.of( k.getId(), p1.getId(), p2.getId() ) );
        UUID entityTypeId = edm.createEntityType( expected );
        Assert.assertNotNull( "Entity type creation shouldn't return null UUID.", entityTypeId );

        return expected;
    }

    public EntityType createEntityTypeWithBaseType( UUID baseTypeId ) {
        PropertyType p1 = createPropertyType();
        PropertyType k = createPropertyType();
        PropertyType p2 = createPropertyType();

        EntityType expected = TestDataFactory.childEntityType( baseTypeId, SecurableObjectType.EntityType, k );
        expected.removePropertyTypes( expected.getProperties() );
        expected.addPropertyTypes( ImmutableSet.of( k.getId(), p1.getId(), p2.getId() ) );

        UUID entityTypeId = edm.createEntityType( expected );
        Assert.assertNotNull( "Entity type creation shouldn't return null UUID.", entityTypeId );

        return expected;
    }

    public EntityType createEntityTypeWithBaseTypeAndCreatedProperties(
            UUID baseTypeId,
            Set<UUID> propertyTypes,
            PropertyType... key ) {
        EntityType expected = TestDataFactory.childEntityTypeWithPropertyType( baseTypeId,propertyTypes,SecurableObjectType.EntityType,key );


        UUID entityTypeId = edm.createEntityType( expected );
        Assert.assertNotNull( "Entity type creation shouldn't return null UUID.", entityTypeId );

        return expected;
    }

    public ComplexType createComplexType() {
        PropertyType p1 = createPropertyType();
        PropertyType p2 = createPropertyType();
        ComplexType expected = TestDataFactory.complexType();
        expected.removePropertyTypes( expected.getProperties() );
        expected.addPropertyTypes( ImmutableSet.of( p1.getId(), p2.getId() ) );
        UUID complexTypeId = edm.createComplexType( expected );

        Assert.assertNotNull( "Complex type creation shouldn't return null UUID", complexTypeId );
        Assert.assertEquals( expected.getId(), complexTypeId );

        return expected;
    }

    @Test
    public void testCreateComplexType() {
        createComplexType();
    }

    public EnumType createEnumType() {
        EnumType expected = TestDataFactory.enumType();
        UUID enumTypeId = edm.createEnumType( expected );

        Assert.assertNotNull( "Enum type id shouldn't return null UUID", enumTypeId );
        Assert.assertEquals( expected.getId(), enumTypeId );
        return expected;
    }

    public EntitySet createEntitySet() {
        return createEntitySetForEntityType( createEntityType().getId() );
    }

    public EntitySet createEntitySetForEntityType( UUID entityTypeId ) {

        EntitySet es = new EntitySet(
                UUID.randomUUID(),
                entityTypeId,
                TestDataFactory.name(),
                "foobar",
                Optional.<String>of( "barred" ),
                ImmutableSet.of( "foo@bar.com", "foobar@foo.net" ) );

        Set<EntitySet> ees = ImmutableSet.copyOf( edm.getEntitySets() );

        Assert.assertFalse( ees.contains( es ) );

        Map<String, UUID> entitySetIds = edm.createEntitySets( ImmutableSet.of( es ) );
        entitySetIds.values().contains( es.getId() );

        Set<EntitySet> aes = ImmutableSet.copyOf( edm.getEntitySets() );

        Assert.assertTrue( aes.contains( es ) );

        return es;
    }

    @Test
    public void testCreatePropertyType() {
        createPropertyType();
    }

    @Test
    public void testLookupPropertyTypeByFqn() {
        PropertyType propertyType = createPropertyType();
        UUID maybePropertyTypeId = edm.getPropertyTypeId(
                propertyType.getType().getNamespace(),
                propertyType.getType().getName() );
        Assert.assertNotNull( maybePropertyTypeId );
        Assert.assertEquals( propertyType.getId(), maybePropertyTypeId );
    }

    @Test
    public void testCreateEntityType() {
        createEntityType();
    }

    @Test
    public void testLookupEntityTypeByFqn() {
        EntityType entityType = createEntityType();
        UUID maybeEntityTypeId = edm.getEntityTypeId(
                entityType.getType().getNamespace(),
                entityType.getType().getName() );
        Assert.assertNotNull( maybeEntityTypeId );
        Assert.assertEquals( entityType.getId(), maybeEntityTypeId );
    }

    @Test
    public void testEntityDataModel() {
        EntityDataModel dm = edm.getEntityDataModel();
        Assert.assertNotNull( dm );
    }

    @Test
    public void testCreateEntitySet() {
        createEntitySet();
    }

    @Test
    public void testGetPropertyTypes() {
        Set<PropertyType> epts = ImmutableSet.copyOf( edm.getPropertyTypes() );
        PropertyType propertyType = createPropertyType();
        Assert.assertFalse( epts.contains( propertyType ) );
        Set<PropertyType> apts = ImmutableSet.copyOf( edm.getPropertyTypes() );
        Assert.assertTrue( apts.contains( propertyType ) );
    }

    @Test
    public void testUpdatePropertyTypeNonPK() {
        PropertyType pt = createPropertyType();

        String newTitle = RandomStringUtils.randomAlphanumeric( 5 );
        String newDescription = RandomStringUtils.randomAlphanumeric( 5 );

        edm.updatePropertyTypeMetadata( pt.getId(),
                new MetadataUpdate(
                        Optional.of( newTitle ),
                        Optional.of( newDescription ),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty() ) );

        PropertyType updatedPt = edm.getPropertyType( pt.getId() );
        Assert.assertEquals( newTitle, updatedPt.getTitle() );
        Assert.assertEquals( newDescription, updatedPt.getDescription() );
    }

    @Test
    public void testUpdatePropertyTypePK() {
        PropertyType pt = createPropertyType();

        FullQualifiedName newPtFqn = TestDataFactory.fqn();

        edm.updatePropertyTypeMetadata( pt.getId(),
                new MetadataUpdate(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of( newPtFqn ),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty() ) );

        PropertyType updatedPt = edm.getPropertyType( pt.getId() );
        Assert.assertEquals( newPtFqn, updatedPt.getType() );
    }

    @Test
    public void testUpdateEntityTypeNonPK() {
        EntityType et = createEntityType();

        String newTitle = RandomStringUtils.randomAlphanumeric( 5 );
        String newDescription = RandomStringUtils.randomAlphanumeric( 5 );

        edm.updateEntityTypeMetadata( et.getId(),
                new MetadataUpdate(
                        Optional.of( newTitle ),
                        Optional.of( newDescription ),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty() ) );

        EntityType updatedEt = edm.getEntityType( et.getId() );
        Assert.assertEquals( newTitle, updatedEt.getTitle() );
        Assert.assertEquals( newDescription, updatedEt.getDescription() );
    }

    @Test
    public void testUpdateEntityTypePK() {
        EntityType et = createEntityType();

        FullQualifiedName newEtFqn = TestDataFactory.fqn();

        edm.updateEntityTypeMetadata( et.getId(),
                new MetadataUpdate(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of( newEtFqn ),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty() ) );

        EntityType updatedEt = edm.getEntityType( et.getId() );
        Assert.assertEquals( newEtFqn, updatedEt.getType() );
    }

    @Test
    public void testUpdateEntitySetNonPK() {
        EntitySet es = createEntitySet();

        String newTitle = RandomStringUtils.randomAlphanumeric( 5 );
        String newDescription = RandomStringUtils.randomAlphanumeric( 5 );
        Set<String> newContacts = new HashSet<>(
                Arrays.asList( RandomStringUtils.randomAlphanumeric( 5 ), RandomStringUtils.randomAlphanumeric( 5 ) ) );

        edm.updateEntitySetMetadata( es.getId(),
                new MetadataUpdate(
                        Optional.of( newTitle ),
                        Optional.of( newDescription ),
                        Optional.empty(),
                        Optional.of( newContacts ),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty() ) );

        EntitySet updatedEs = edm.getEntitySet( es.getId() );
        Assert.assertEquals( newTitle, updatedEs.getTitle() );
        Assert.assertEquals( newDescription, updatedEs.getDescription() );
        Assert.assertEquals( newContacts, updatedEs.getContacts() );
    }

    @Test
    public void testUpdateEntitySetPK() {
        EntitySet es = createEntitySet();

        String newEsName = TestDataFactory.name();

        edm.updateEntitySetMetadata( es.getId(),
                new MetadataUpdate(
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of( newEsName ),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty() ) );

        EntitySet updatedEs = edm.getEntitySet( es.getId() );
        Assert.assertEquals( newEsName, updatedEs.getName() );
    }

    @Test
    public void testAddPropertyTypesToEntityTypeViaController() {
        EntityType base = createEntityType();
        EntityType child1 = createEntityTypeWithBaseType( base.getId() );
        EntityType child2 = createEntityTypeWithBaseType( base.getId() );
        EntityType grandchild = createEntityTypeWithBaseType( child1.getId() );

        UUID newProp = createPropertyType().getId();
        edm.addPropertyTypeToEntityType( base.getId(), newProp );

        // Update entity types
        base = edm.getEntityType( base.getId() );
        child1 = edm.getEntityType( child1.getId() );
        child2 = edm.getEntityType( child2.getId() );
        grandchild = edm.getEntityType( grandchild.getId() );

        Assert.assertTrue( base.getProperties().contains( newProp ) );
        Assert.assertTrue( child1.getProperties().contains( newProp ) );
        Assert.assertTrue( child2.getProperties().contains( newProp ) );
        Assert.assertTrue( grandchild.getProperties().contains( newProp ) );
    }

    @Test
    public void removePropertyTypesFromEntityTypeViaController() {
        UUID ptId = createPropertyType().getId();
        UUID ptId2 = createPropertyType().getId();
        Set<UUID> propertyTypes = Sets.newHashSet( ptId, ptId2 );
        PropertyType key = createPropertyType();

        EntityType ancestor = createEntityType();
        EntityType base = createEntityTypeWithBaseTypeAndCreatedProperties( ancestor.getId(), propertyTypes, key );
        EntityType child1 = createEntityTypeWithBaseTypeAndCreatedProperties( base.getId(), propertyTypes, key );
        EntityType child2 = createEntityTypeWithBaseTypeAndCreatedProperties( base.getId(), propertyTypes, key );
        EntityType grandchild = createEntityTypeWithBaseTypeAndCreatedProperties( child1.getId(), propertyTypes, key );

        edm.removePropertyTypeFromEntityType( child1.getId(), ptId );
        Assert.assertTrue( edm.getEntityType( base.getId() ).getProperties().contains( ptId ) );
        Assert.assertTrue( edm.getEntityType( child1.getId() ).getProperties().contains( ptId ) );
        Assert.assertTrue( edm.getEntityType( child2.getId() ).getProperties().contains( ptId ) );
        Assert.assertTrue( edm.getEntityType( grandchild.getId() ).getProperties().contains( ptId ) );

        edm.removePropertyTypeFromEntityType( base.getId(), key.getId() );
        Assert.assertTrue( edm.getEntityType( base.getId() ).getProperties().contains( ptId ) );
        Assert.assertTrue( edm.getEntityType( child1.getId() ).getProperties().contains( ptId ) );
        Assert.assertTrue( edm.getEntityType( child2.getId() ).getProperties().contains( ptId ) );
        Assert.assertTrue( edm.getEntityType( grandchild.getId() ).getProperties().contains( ptId ) );

        edm.removePropertyTypeFromEntityType( base.getId(), ptId );
        Assert.assertFalse( edm.getEntityType( base.getId() ).getProperties().contains( ptId ) );
        Assert.assertFalse( edm.getEntityType( child1.getId() ).getProperties().contains( ptId ) );
        Assert.assertFalse( edm.getEntityType( child2.getId() ).getProperties().contains( ptId ) );
        Assert.assertFalse( edm.getEntityType( grandchild.getId() ).getProperties().contains( ptId ) );

        createEntitySetForEntityType( grandchild.getId() );

        edm.removePropertyTypeFromEntityType( base.getId(), ptId2 );
        Assert.assertTrue( edm.getEntityType( base.getId() ).getProperties().contains( ptId2 ) );
        Assert.assertTrue( edm.getEntityType( child1.getId() ).getProperties().contains( ptId2 ) );
        Assert.assertTrue( edm.getEntityType( child2.getId() ).getProperties().contains( ptId2 ) );
        Assert.assertTrue( edm.getEntityType( grandchild.getId() ).getProperties().contains( ptId2 ) );
    }

    @Test
    public void testCreateEnumType() {
        createEnumType();
    }

    @AfterClass
    public static void testsComplete() {
        logger.info( "This is for setting breakpoints." );
    }
}
