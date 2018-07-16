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

import static com.openlattice.datastore.TestEdmConfigurer.EMPLOYEES;
import static com.openlattice.datastore.TestEdmConfigurer.EMPLOYEE_DEPT;
import static com.openlattice.datastore.TestEdmConfigurer.EMPLOYEE_ID;
import static com.openlattice.datastore.TestEdmConfigurer.EMPLOYEE_ID_PROP_ID;
import static com.openlattice.datastore.TestEdmConfigurer.EMPLOYEE_NAME;
import static com.openlattice.datastore.TestEdmConfigurer.EMPLOYEE_TITLE;
import static com.openlattice.datastore.TestEdmConfigurer.EMPLOYEE_TITLE_PROP_ID;
import static com.openlattice.datastore.TestEdmConfigurer.ENTITY_TYPE;
import static com.openlattice.datastore.TestEdmConfigurer.METADATA_LEVELS;
import static com.openlattice.datastore.TestEdmConfigurer.METADATA_LEVELS_ID;
import static com.openlattice.datastore.TestEdmConfigurer.METADATA_LEVELS_MARS_ID;
import static com.openlattice.datastore.TestEdmConfigurer.NAMESPACE;
import static com.openlattice.datastore.TestEdmConfigurer.SALARY;
import static com.openlattice.datastore.TestEdmConfigurer.SCHEMA_NAME;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.openlattice.conductor.rpc.Employee;
import com.openlattice.datastore.IntegrationTestsBootstrap;
import com.openlattice.datastore.converters.IterableCsvHttpMessageConverter;
import com.openlattice.datastore.odata.EdmProviderImpl;
import com.openlattice.datastore.odata.Transformers.EntitySetTransformer;
import com.openlattice.datastore.odata.Transformers.EntityTypeTransformer;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.ODataStorageService;
import com.openlattice.edm.exceptions.TypeExistsException;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.edm.type.PropertyType;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmEntityContainer;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.core.edm.EdmEntityContainerImpl;
import org.apache.olingo.commons.core.edm.EdmEntitySetImpl;
import org.apache.olingo.commons.core.edm.EdmEntityTypeImpl;
import org.apache.olingo.server.api.ODataApplicationException;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.HttpMessageNotWritableException;

public class DatastoreTests extends IntegrationTestsBootstrap {

    private static final Multimap<String, Object> m = HashMultimap.create();

    @Test
    public void testSerialization() throws HttpMessageNotWritableException, IOException {
        IterableCsvHttpMessageConverter converter = new IterableCsvHttpMessageConverter();
        m.put( new FullQualifiedName( NAMESPACE, SALARY ).getFullQualifiedNameAsString(), 1 );
        m.put( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ).getFullQualifiedNameAsString(), UUID.randomUUID() );
        m.put( new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ).getFullQualifiedNameAsString(), "Master Chief" );
        m.putAll( new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ).getFullQualifiedNameAsString(),
                Arrays.asList( "Fire", "Water" ) );
        converter.write( ImmutableList.of( m ), null, null, new HttpOutputMessage() {

            @Override
            public HttpHeaders getHeaders() {
                return new HttpHeaders();
            }

            @Override
            public OutputStream getBody() throws IOException {
                return System.out;
            }
        } );
        }

    @Test
    public void testCreateEntityByOData() {
        ODataStorageService esc = ds.getContext().getBean( ODataStorageService.class );
        Property empId = new Property();
        Property empName = new Property();
        Property empTitle = new Property();
        Property empSalary = new Property();
        empId.setName( EMPLOYEE_ID );
        empId.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ).getFullQualifiedNameAsString() );
        empId.setValue( ValueType.PRIMITIVE, UUID.randomUUID() );

        empName.setName( EMPLOYEE_NAME );
        empName.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ).getFullQualifiedNameAsString() );
        empName.setValue( ValueType.PRIMITIVE, "Kung Fury" );

        empTitle.setName( EMPLOYEE_TITLE );
        empTitle.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ).getFullQualifiedNameAsString() );
        empTitle.setValue( ValueType.PRIMITIVE, "Kung Fu Master" );

        empSalary.setName( SALARY );
        empSalary.setType( new FullQualifiedName( NAMESPACE, SALARY ).getFullQualifiedNameAsString() );
        empSalary.setValue( ValueType.PRIMITIVE, Long.MAX_VALUE );

        Entity e = new Entity();
        e.setType( ENTITY_TYPE.getFullQualifiedNameAsString() );
        e.addProperty( empId ).addProperty( empName ).addProperty( empTitle ).addProperty( empSalary );
        /**
        esc.replaceEntities( ACLs.EVERYONE_ACL,
                Syncs.BASE.getSyncId(),
                ENTITY_SET_NAME,
                ENTITY_TYPE,
                e );
        */
        // esc.readEntityData( edmEntitySet, keyParams );
    }

    // @Test
    public void polulateEmployeeCsv() throws IOException {
        ODataStorageService esc = ds.getContext().getBean( ODataStorageService.class );
        Property employeeId;
        Property employeeName;
        Property employeeTitle;
        Property employeeDept;
        Property employeeSalary;

        try ( FileReader fr = new FileReader( "src/test/resources/employees.csv" );
                BufferedReader br = new BufferedReader( fr ) ) {

            String line;
            while ( ( line = br.readLine() ) != null ) {
                Employee employee = Employee.EmployeeCsvReader.getEmployee( line );
                System.out.println( employee.toString() );

                employeeId = new Property();
                employeeId.setName( EMPLOYEE_ID );
                employeeId.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ).getFullQualifiedNameAsString() );
                employeeId.setValue( ValueType.PRIMITIVE, UUID.randomUUID() );

                employeeName = new Property();
                employeeName.setName( EMPLOYEE_NAME );
                employeeName
                        .setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ).getFullQualifiedNameAsString() );
                employeeName.setValue( ValueType.PRIMITIVE, employee.getName() );

                employeeTitle = new Property();
                employeeTitle.setName( EMPLOYEE_TITLE );
                employeeTitle
                        .setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ).getFullQualifiedNameAsString() );
                employeeTitle.setValue( ValueType.PRIMITIVE, employee.getTitle() );

                employeeDept = new Property();
                employeeDept.setName( EMPLOYEE_DEPT );
                employeeDept
                        .setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ).getFullQualifiedNameAsString() );
                employeeDept.setValue( ValueType.PRIMITIVE, employee.getDept() );

                employeeSalary = new Property();
                employeeSalary.setName( SALARY );
                employeeSalary.setType( new FullQualifiedName( NAMESPACE, SALARY ).getFullQualifiedNameAsString() );
                employeeSalary.setValue( ValueType.PRIMITIVE, (long) employee.getSalary() );

                Entity entity = new Entity();
                entity.setType( ENTITY_TYPE.getFullQualifiedNameAsString() );
                entity.addProperty( employeeId )
                        .addProperty( employeeName )
                        .addProperty( employeeTitle )
                        .addProperty( employeeDept )
                        .addProperty( employeeSalary );
                /**
                esc.replaceEntities( ACLs.EVERYONE_ACL,
                        Syncs.BASE.getSyncId(),
                        ENTITY_SET_NAME,
                        ENTITY_TYPE,
                        entity );

                // Created by Ho Chung for testing different entity types
                // add entityType "employeeMars"
                esc.replaceEntities( ACLs.EVERYONE_ACL,
                        Syncs.BASE.getSyncId(),
                        ENTITY_SET_NAME,
                        ENTITY_TYPE_MARS,
                        entity );
                // add entityType "employeeSaturn"
                esc.replaceEntities( ACLs.EVERYONE_ACL,
                        Syncs.BASE.getSyncId(),
                        ENTITY_SET_NAME,
                        ENTITY_TYPE_SATURN,
                        entity );
                */
            }
        }
    }

    // @Test
    public void testRead() {
        Set<FullQualifiedName> properties = ImmutableSet.of(
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ),
                new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ),
                new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ),
                new FullQualifiedName( NAMESPACE, SALARY ) );

        ODataStorageService esc = ds.getContext().getBean( ODataStorageService.class );
        EdmManager dms = ds.getContext().getBean( EdmManager.class );
        HazelcastSchemaManager schemaMgr = ds.getContext().getBean( HazelcastSchemaManager.class );
        EdmProviderImpl provider = new EdmProviderImpl( dms, schemaMgr );
        Edm edm = new org.apache.olingo.commons.core.edm.EdmProviderImpl( provider );

        CsdlEntityContainerInfo info = new CsdlEntityContainerInfo().setContainerName( ENTITY_TYPE );
        EdmEntityContainer edmEntityContainer = new EdmEntityContainerImpl( edm, provider, info );

        CsdlEntityType csdlEntityType = new EntityTypeTransformer( dms ).transform(
                METADATA_LEVELS );

        EdmEntityType edmEntityType = new EdmEntityTypeImpl( edm, ENTITY_TYPE, csdlEntityType );

        CsdlEntitySet csdlEntitySet = new EntitySetTransformer( dms ).transform( EMPLOYEES );
        EdmEntitySet edmEntitySet = new EdmEntitySetImpl( edm, edmEntityContainer, csdlEntitySet );

        try {
            EntityCollection ec = esc.readEntitySetData( edmEntitySet );
            ec.forEach( currEntity -> System.out.println( currEntity ) );
        } catch ( ODataApplicationException e ) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAddPropertyTypeToEntityType() {
        // Desired result: Properties EMPLOYEE_COUNTRY, EMPLOYEE_WEIGHT are added to ENTITY_TYPE (Employees)
        final String EMPLOYEE_COUNTRY = "employee_country";
        final String EMPLOYEE_WEIGHT = "employee_weight";
        UUID EMPLOYEE_COUNTRY_ID;
        UUID EMPLOYEE_WEIGHT_ID;

        EdmManager dms = ds.getContext().getBean( EdmManager.class );
        try {
            EMPLOYEE_COUNTRY_ID = UUID.randomUUID();
            EMPLOYEE_WEIGHT_ID = UUID.randomUUID();
            dms.createPropertyTypeIfNotExists( new PropertyType(
                    EMPLOYEE_COUNTRY_ID,
                    new FullQualifiedName( NAMESPACE, EMPLOYEE_COUNTRY ),
                    "Employee Country",
                    Optional
                            .of( "Country of an employee of the city of Chicago." ),
                    ImmutableSet.of(),
                    EdmPrimitiveTypeKind.String ) );

            dms.createPropertyTypeIfNotExists( new PropertyType(
                    EMPLOYEE_WEIGHT_ID,
                    new FullQualifiedName( NAMESPACE, EMPLOYEE_WEIGHT ),
                    "Employee Weight",
                    Optional
                            .of( "Weight of an employee of the city of Chicago." ),
                    ImmutableSet.of(),
                    EdmPrimitiveTypeKind.Int32 ) );
        } catch ( TypeExistsException e ) {
            EMPLOYEE_COUNTRY_ID = dms.getTypeAclKey( new FullQualifiedName( NAMESPACE, EMPLOYEE_COUNTRY ) );
            EMPLOYEE_WEIGHT_ID = dms.getTypeAclKey( new FullQualifiedName( NAMESPACE, EMPLOYEE_WEIGHT ) );

        }
        Set<UUID> properties = ImmutableSet.of( EMPLOYEE_COUNTRY_ID, EMPLOYEE_WEIGHT_ID );

        dms.addPropertyTypesToEntityType( METADATA_LEVELS_ID, properties );
    }

    @Test
    public void testAddExistingPropertyTypeToEntityType() {
        // Action: Property EMPLOYEE_ID is added to ENTITY_TYPE (Employees)
        // Desired result: Since property is already part of ENTITY_TYPE, nothing should happen

        EdmManager dms = ds.getContext().getBean( EdmManager.class );
        dms.addPropertyTypesToEntityType( METADATA_LEVELS_ID, ImmutableSet.of( EMPLOYEE_ID_PROP_ID ) );
    }

    @Test(
        expected = IllegalArgumentException.class )
    public void testAddPhantomPropertyTypeToEntityType() {
        // Action: Add Property EMPLOYEE_HEIGHT to ENTITY_TYPE (Employees)
        // Desired result: Since property does not exists, Bad Request Exception should be thrown

        EdmManager dms = ds.getContext().getBean( EdmManager.class );

        dms.addPropertyTypesToEntityType( METADATA_LEVELS_ID, ImmutableSet.of( UUID.randomUUID() ) );
    }

    @Test
    public void testAddPropertyToSchema() {
        final String EMPLOYEE_TOENAIL_LENGTH = "employee-toenail-length";
        final String EMPLOYEE_FINGERNAIL_LENGTH = "employee-fingernail-length";

        UUID EMPLOYEE_TOENAIL_LENGTH_ID;
        UUID EMPLOYEE_FINGERNAIL_LENGTH_ID;

        EdmManager dms = ds.getContext().getBean( EdmManager.class );
        try {
            EMPLOYEE_TOENAIL_LENGTH_ID = UUID.randomUUID();
            EMPLOYEE_FINGERNAIL_LENGTH_ID = UUID.randomUUID();

            dms.createPropertyTypeIfNotExists( new PropertyType(
                    EMPLOYEE_TOENAIL_LENGTH_ID,
                    new FullQualifiedName( NAMESPACE, EMPLOYEE_TOENAIL_LENGTH ),
                    "Employee Toenail Length",
                    Optional
                            .of( "Toenail Length of an employee of the city of Chicago." ),
                    ImmutableSet.of(),
                    EdmPrimitiveTypeKind.Int32 ) );

            dms.createPropertyTypeIfNotExists( new PropertyType(
                    EMPLOYEE_FINGERNAIL_LENGTH_ID,
                    new FullQualifiedName( NAMESPACE, EMPLOYEE_FINGERNAIL_LENGTH ),
                    "Employee Fingernail Length",
                    Optional
                            .of( "Fingernail Length of an employee of the city of Chicago." ),
                    ImmutableSet.of(),
                    EdmPrimitiveTypeKind.Int32 ) );
        } catch ( TypeExistsException e ) {
            EMPLOYEE_TOENAIL_LENGTH_ID = dms
                    .getTypeAclKey( new FullQualifiedName( NAMESPACE, EMPLOYEE_TOENAIL_LENGTH ) );
            EMPLOYEE_FINGERNAIL_LENGTH_ID = dms
                    .getTypeAclKey( new FullQualifiedName( NAMESPACE, EMPLOYEE_FINGERNAIL_LENGTH ) );
        }
        // Add new property to Schema
        Set<UUID> newProperties = ImmutableSet.of( EMPLOYEE_TOENAIL_LENGTH_ID, EMPLOYEE_FINGERNAIL_LENGTH_ID );
        schemaManager.addPropertyTypesToSchema( newProperties, new FullQualifiedName( NAMESPACE, SCHEMA_NAME ) );

        // Add existing property to Schema
        schemaManager.addPropertyTypesToSchema( ImmutableSet.of( EMPLOYEE_TITLE_PROP_ID ),
                new FullQualifiedName( NAMESPACE, SCHEMA_NAME ) );

        // Add non-existing property to Schema
        Throwable caught = null;
        try {
            schemaManager.addPropertyTypesToSchema( ImmutableSet.of( UUID.randomUUID() ),
                    new FullQualifiedName( NAMESPACE, SCHEMA_NAME ) );
        } catch ( Throwable t ) {
            caught = t;
        }
        Assert.assertNotNull( caught );
        Assert.assertSame( IllegalArgumentException.class, caught.getClass() );
    }

    @Test
    public void removePropertyTypes() {
        // Action: Add Property EMPLOYEE_HAIRLENGTH to ENTITY_TYPE (Employees), and EMPLOYEE_EYEBROW_LENGTH to Schema,
        // then remove them
        // Desired result: Schemas and Entity_Types tables should look the same as before, without any trace of
        // EMPLOYEE_HAIRLENGTH and EMPLOYEE_EYEBROW_LENGTH
        // Property_Types and lookup table should be updated.
        final String EMPLOYEE_HAIR_LENGTH = "employee_hair_length";
        final String EMPLOYEE_EYEBROW_LENGTH = "employee_eyebrow_length";

        UUID EMPLOYEE_HAIR_LENGTH_ID;
        UUID EMPLOYEE_EYEBROW_LENGTH_ID;

        EdmManager dms = ds.getContext().getBean( EdmManager.class );
        try {
            EMPLOYEE_HAIR_LENGTH_ID = UUID.randomUUID();
            EMPLOYEE_EYEBROW_LENGTH_ID = UUID.randomUUID();
            dms.createPropertyTypeIfNotExists( new PropertyType(
                    EMPLOYEE_HAIR_LENGTH_ID,
                    new FullQualifiedName( NAMESPACE, EMPLOYEE_HAIR_LENGTH ),
                    "Employee Hair Length",
                    Optional
                            .of( "Hair Length of an employee of the city of Chicago." ),
                    ImmutableSet.of(),
                    EdmPrimitiveTypeKind.Int32 ) );

            dms.createPropertyTypeIfNotExists( new PropertyType(
                    EMPLOYEE_EYEBROW_LENGTH_ID,
                    new FullQualifiedName( NAMESPACE, EMPLOYEE_EYEBROW_LENGTH ),
                    "Employee Eyebrow Length",
                    Optional
                            .of( "Eyebrow Length of an employee of the city of Chicago." ),
                    ImmutableSet.of(),
                    EdmPrimitiveTypeKind.Int32 ) );
        } catch ( TypeExistsException e ) {
            EMPLOYEE_HAIR_LENGTH_ID = dms.getTypeAclKey( new FullQualifiedName( NAMESPACE, EMPLOYEE_HAIR_LENGTH ) );
            EMPLOYEE_EYEBROW_LENGTH_ID = dms
                    .getTypeAclKey( new FullQualifiedName( NAMESPACE, EMPLOYEE_EYEBROW_LENGTH ) );

        }

        dms.addPropertyTypesToEntityType( METADATA_LEVELS_MARS_ID, ImmutableSet.of( EMPLOYEE_HAIR_LENGTH_ID ) );
        schemaManager.addPropertyTypesToSchema( ImmutableSet.of( EMPLOYEE_EYEBROW_LENGTH_ID ),
                new FullQualifiedName(
                        NAMESPACE,
                        SCHEMA_NAME ) );
        
        dms.removePropertyTypesFromEntityType( METADATA_LEVELS_MARS_ID, ImmutableSet.of( EMPLOYEE_HAIR_LENGTH_ID ) );
        schemaManager.removePropertyTypesFromSchema( ImmutableSet.of( EMPLOYEE_EYEBROW_LENGTH_ID ),
                new FullQualifiedName(
                        NAMESPACE,
                        SCHEMA_NAME ) );
    }

    @Test
    public void testGetEntitySet() {
        Assert.assertNotNull( dms.getEntitySet( EMPLOYEES.getId() ) );
    }
}
