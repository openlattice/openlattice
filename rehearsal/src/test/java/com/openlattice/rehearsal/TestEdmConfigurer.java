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

package com.openlattice.rehearsal;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.Schema;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Assert;

/**
 * This class creates some basic EDM elements for testing. It assumes that entity data model is empty and that properties
 * being created don't already exist.
 */

public class TestEdmConfigurer {
    public static final String SCHEMA_NAME         = "testcsv.csv";
    public static final String SALARY              = "testcsv.salary";
    public static final String NAME                = "testcsv.name";
    public static final String TITLE               = "testcsv.title";
    public static final String DEPT                = "testcsv.dept";
    public static final String ID                  = "testcsv.id";
    public static final String ROLE_NAME           = "testcsv.role";
    public static final String ENTITY_TYPE_NAME    = "testcsv.person";
    public static final String EMPLOYED_IN_NAME    = "testcsv.employed_in";
    public static final String ENTITY_SET_NAME     = "employees";
    public static final String EMPLOYED_IN_ES_NAME = "employedin";

    public static final FullQualifiedName PERSON_FQN      = new FullQualifiedName( ENTITY_TYPE_NAME );
    public static final FullQualifiedName EMPLOYED_IN_FQN = new FullQualifiedName( EMPLOYED_IN_NAME );
    public static final FullQualifiedName ROLE_FQN        = new FullQualifiedName( ROLE_NAME );

    protected static final PropertyType EMPLOYEE_TITLE_PROP_TYPE = new PropertyType(
            new FullQualifiedName( TITLE ),
            "Title",
            Optional.of( "Title of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.String );

    protected static final PropertyType START_DATETIME_PROP_TYPE = new PropertyType(
            new FullQualifiedName( "testcsv.startdate" ),
            "Title",
            Optional.of( "Start Date of entity relationship" ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.DateTimeOffset );

    protected static final PropertyType END_DATETIME_PROP_TYPE    = new PropertyType(
            new FullQualifiedName( "testcsv.enddate" ),
            "Title",
            Optional.of( "Start Date of entity relationship" ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.DateTimeOffset );
    protected static final PropertyType EMPLOYEE_NAME_PROP_TYPE   = new PropertyType(
            new FullQualifiedName( NAME ),
            "Name",
            Optional.of( "Name of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.String );
    protected static final PropertyType ID_PROP_TYPE              = new PropertyType(
            new FullQualifiedName( ID ),
            "ID",
            Optional.of( "Unique ID of an entity" ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.Guid );
    protected static final PropertyType EMPLOYEE_DEPT_PROP_TYPE   = new PropertyType(
            new FullQualifiedName( DEPT ),
            "Department",
            Optional.of( "Department of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.String );
    protected static final PropertyType EMPLOYEE_SALARY_PROP_TYPE = new PropertyType(
            new FullQualifiedName( SALARY ),
            "Salary",
            Optional.of( "Salary of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.Int64 );

    public static EntityType      PERSON;
    public static AssociationType EMPLOYED_IN;
    public static EntityType      ROLE;

    public static UUID      PERSON_ES_ID;
    public static EntitySet PERSON_ES;
    public static EntitySet EMPLOYED_IN_ES;

    public static UUID EMPLOYEE_NAME_PROP_ID   = EMPLOYEE_NAME_PROP_TYPE.getId();
    public static UUID EMPLOYEE_TITLE_PROP_ID  = EMPLOYEE_TITLE_PROP_TYPE.getId();
    public static UUID ID_PROP_ID              = ID_PROP_TYPE.getId();
    public static UUID EMPLOYEE_DEPT_PROP_ID   = EMPLOYEE_DEPT_PROP_TYPE.getId();
    public static UUID EMPLOYEE_SALARY_PROP_ID = EMPLOYEE_SALARY_PROP_TYPE.getId();

    static {
        PERSON = with( PERSON_FQN );
        EMPLOYED_IN = new AssociationType(
                Optional.of(
                        new EntityType( EMPLOYED_IN_FQN,
                                "Employed In",
                                "Relationship representing type of employment",
                                ImmutableSet.of(),
                                Sets.newLinkedHashSet( Arrays.asList( ID_PROP_ID ) ),
                                Sets.newLinkedHashSet( Arrays.asList( ID_PROP_ID,
                                        START_DATETIME_PROP_TYPE.getId(),
                                        END_DATETIME_PROP_TYPE.getId() ) ),
                                Optional.empty(),
                                Optional.of( SecurableObjectType.AssociationType ) )
                ),
                Sets.newLinkedHashSet( Arrays.asList( PERSON.getId() ) ),
                Sets.newLinkedHashSet( Arrays.asList() ),
                false );
    }

    static void setupDatamodel( EdmApi edmApi ) {
        createPropertyTypes( edmApi );
        createEntityTypes( edmApi );
        createAssociationTypes( edmApi );
        createEntitySets( edmApi );

        edmApi.createSchemaIfNotExists( new Schema(
                new FullQualifiedName( SCHEMA_NAME ),
                ImmutableSet.of( ID_PROP_TYPE,
                        EMPLOYEE_TITLE_PROP_TYPE,
                        EMPLOYEE_NAME_PROP_TYPE,
                        EMPLOYEE_DEPT_PROP_TYPE,
                        EMPLOYEE_SALARY_PROP_TYPE ),
                ImmutableSet.of( PERSON, EMPLOYED_IN.getAssociationEntityType() ) ) );

        Assert.assertTrue( edmApi.getEntitySetId( ENTITY_SET_NAME ) != null );
    }

    private static void createAssociationTypes( EdmApi edmApi ) {
        checkNotNull( edmApi.createAssociationType( EMPLOYED_IN ) );
    }

    private static EntityType getRoleEntityType() {
        return new EntityType(
                ROLE_FQN,
                "Job Role",
                "Details for individual job roles for persons.",
                ImmutableSet.of(),
                Sets.newLinkedHashSet( Arrays.asList( ID_PROP_ID ) ),
                Sets.newLinkedHashSet( Arrays.asList( ID_PROP_ID,
                        EMPLOYEE_TITLE_PROP_ID,
                        EMPLOYEE_DEPT_PROP_ID,
                        EMPLOYEE_SALARY_PROP_ID ) ),
                Optional.empty(),
                Optional.of( SecurableObjectType.EntityType )
        );
    }

    public static EntityType with( FullQualifiedName fqn ) {
        return new EntityType(
                fqn,
                fqn.getFullQualifiedNameAsString() + " Employees",
                fqn.getFullQualifiedNameAsString() + " Employees of the city of Chicago",
                ImmutableSet.of(),
                Sets.newLinkedHashSet( Arrays.asList( ID_PROP_ID ) ),
                Sets.newLinkedHashSet( Arrays.asList(
                        ID_PROP_ID,
                        EMPLOYEE_NAME_PROP_ID ) ),
                Optional.empty(),
                Optional.of( SecurableObjectType.EntityType ) );
    }

    private static void createPropertyTypes( EdmApi dms ) {
        checkNotNull( dms.createPropertyType( ID_PROP_TYPE ) );
        checkNotNull( dms.createPropertyType( EMPLOYEE_TITLE_PROP_TYPE ) );
        checkNotNull( dms.createPropertyType( EMPLOYEE_NAME_PROP_TYPE ) );
        checkNotNull( dms.createPropertyType( EMPLOYEE_DEPT_PROP_TYPE ) );
        checkNotNull( dms.createPropertyType( EMPLOYEE_SALARY_PROP_TYPE ) );
        checkNotNull( dms.createPropertyType( START_DATETIME_PROP_TYPE ) );
        checkNotNull( dms.createPropertyType( END_DATETIME_PROP_TYPE ) );
    }

    private static void createEntityTypes( EdmApi dms ) {
        PERSON = createEntityTypeIfNotExists( dms, PERSON );
        ROLE = createEntityTypeIfNotExists( dms, getRoleEntityType() );
    }

    private static EntityType createEntityTypeIfNotExists( EdmApi edmApi, EntityType et ) {
        checkNotNull( edmApi.createEntityType( et ) );
        return et;
    }

    private static void createEntitySets( EdmApi edmApi ) {
        PERSON_ES = new EntitySet(
                PERSON.getId(),
                ENTITY_SET_NAME,
                ENTITY_SET_NAME,
                Optional.of( "Names and salaries of Chicago employees" ),
                ImmutableSet.of( "support@openlattice.com" ) );
        EMPLOYED_IN_ES = new EntitySet( EMPLOYED_IN.getAssociationEntityType().getId(),
                EMPLOYED_IN_ES_NAME,
                "Chicago Employed In",
                Optional.empty(),
                ImmutableSet.of( "support@openlattice.com" )
        );
        PERSON_ES_ID = PERSON_ES.getId();
        final Map<String, UUID> created = checkNotNull( edmApi
                .createEntitySets( ImmutableSet.of( PERSON_ES, EMPLOYED_IN_ES ) ) );
        checkState(
                created.containsKey( ENTITY_SET_NAME ) && PERSON_ES_ID.equals( created.get( ENTITY_SET_NAME ) ) );
    }
}
