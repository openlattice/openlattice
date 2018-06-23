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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.Schema;
import com.openlattice.edm.exceptions.TypeExistsException;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import java.util.Arrays;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Assert;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

public class TestEdmConfigurer {
    public static final String SCHEMA_NAME      = "testcsv.csv";
    public static final String SALARY           = "testcsv.salary";
    public static final String EMPLOYEE_NAME    = "testcsv.name";
    public static final String EMPLOYEE_TITLE   = "testcsv.title";
    public static final String EMPLOYEE_DEPT    = "testcsv.dept";
    public static final String EMPLOYEE_ID      = "testcsv.id";
    public static final String ENTITY_SET_NAME  = "employees";
    public static final String ENTITY_TYPE_NAME = "testcsv.person";
    public static final String EMPLOYED_AS_NAME = "testcsv.employed_as";

    public static final FullQualifiedName ENTITY_TYPE      = new FullQualifiedName( ENTITY_TYPE_NAME );
    public static final FullQualifiedName ASSOCIATION_TYPE = new FullQualifiedName( EMPLOYED_AS_NAME );

    public static final FullQualifiedName ENTITY_TYPE_MARS   = new FullQualifiedName( "testcsv.employeeMars" );
    public static final FullQualifiedName ENTITY_TYPE_SATURN = new FullQualifiedName( "testcsv.employeeSaturn" );

    public static final    PropertyType EMPLOYEE_TITLE_PROP_TYPE  = new PropertyType(
            new FullQualifiedName( EMPLOYEE_TITLE ),
            "Title",
            Optional.of( "Title of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.String );
    protected static final PropertyType EMPLOYEE_NAME_PROP_TYPE   = new PropertyType(
            new FullQualifiedName( EMPLOYEE_NAME ),
            "Name",
            Optional.of( "Name of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.String );
    protected static final PropertyType ID_PROP_TYPE              = new PropertyType(
            new FullQualifiedName( EMPLOYEE_ID ),
            "Employee ID",
            Optional.of( "ID of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.Guid );
    protected static final PropertyType EMPLOYEE_DEPT_PROP_TYPE   = new PropertyType(
            new FullQualifiedName( EMPLOYEE_DEPT ),
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

    public static EntityType      METADATA_LEVELS;
    public static EntityType      METADATA_LEVELS_SATURN;
    public static EntityType      METADATA_LEVELS_MARS;
    public static EntityType      EMPLOYMENT_INFO;
    public static AssociationType EMPLOYED_AS;

    public static UUID      METADATA_LEVELS_ID;
    public static UUID      METADATA_LEVELS_MARS_ID;
    public static UUID      METADATA_LEVELS_SATURN_ID;
    public static EntitySet EMPLOYEES;

    public static UUID EMPLOYED_AS_ID;
    public static UUID EMPLOYEE_NAME_PROP_ID   = EMPLOYEE_NAME_PROP_TYPE.getId();
    public static UUID EMPLOYEE_TITLE_PROP_ID  = EMPLOYEE_TITLE_PROP_TYPE.getId();
    public static UUID ID_PROP_ID              = ID_PROP_TYPE.getId();
    public static UUID EMPLOYEE_DEPT_PROP_ID   = EMPLOYEE_DEPT_PROP_TYPE.getId();
    public static UUID EMPLOYEE_SALARY_PROP_ID = EMPLOYEE_SALARY_PROP_TYPE.getId();

    static {
        METADATA_LEVELS = with( ENTITY_TYPE );
        METADATA_LEVELS_ID = METADATA_LEVELS.getId();
        METADATA_LEVELS_SATURN = with( ENTITY_TYPE_SATURN );
        METADATA_LEVELS_SATURN_ID = METADATA_LEVELS_SATURN.getId();
        METADATA_LEVELS_MARS = with( ENTITY_TYPE_MARS );
        METADATA_LEVELS_MARS_ID = METADATA_LEVELS_MARS.getId();

        EMPLOYED_AS = new AssociationType( Optional
                .of( new EntityType( ASSOCIATION_TYPE,
                        "Employed As",
                        "Relationship representing type of employment",
                        ImmutableSet.of(),
                        Sets.newLinkedHashSet(Arrays.asList(ID_PROP_ID)),
                        Sets.newLinkedHashSet(Arrays.asList(ID_PROP_ID)),
                        Optional.absent(),
                        Optional.of( SecurableObjectType.AssociationType ) ) ),
                Sets.newLinkedHashSet( Arrays.asList(  ) )
                Sets.newLinkedHashSet() );
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
                ImmutableSet.of( METADATA_LEVELS, METADATA_LEVELS_MARS, METADATA_LEVELS_SATURN ) ) );

        Assert.assertTrue( edmApi.getEntitySetId( ENTITY_SET_NAME ) != null );
    }

    private static void createAssociationTypes( EdmApi edmApi ) {
        final UUID employedId = edmApi.createAssociationType( EMPLOYED_AS );
        if ( employedId == null ) {
            final FullQualifiedName associationFqn = EMPLOYED_AS.getAssociationEntityType().getType();
            EMPLOYED_AS_ID = edmApi.getEntityTypeId( associationFqn.getNamespace(), associationFqn.getName() );
        }

    }

    public static EntityType with( FullQualifiedName fqn ) {
        return new EntityType(
                fqn,
                fqn.getFullQualifiedNameAsString() + " Employees",
                fqn.getFullQualifiedNameAsString() + " Employees of the city of Chicago",
                ImmutableSet.of(),
                Sets.newLinkedHashSet( Arrays.asList( ID_PROP_ID ) ),
                Sets.newLinkedHashSet( Arrays.asList( ID_PROP_ID,
                        EMPLOYEE_TITLE_PROP_ID,
                        EMPLOYEE_NAME_PROP_ID,
                        EMPLOYEE_DEPT_PROP_ID,
                        EMPLOYEE_SALARY_PROP_ID ) ),
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) );
    }

    private static void createPropertyTypes( EdmApi dms ) {
        try {
            dms.createPropertyType( ID_PROP_TYPE );
        } catch ( TypeExistsException e ) {
            ID_PROP_ID = dms.getPropertyTypeId( ID_PROP_TYPE.getType().getNamespace(),
                    ID_PROP_TYPE.getType().getName() );
        }
        try {
            dms.createPropertyType( EMPLOYEE_TITLE_PROP_TYPE );
        } catch ( TypeExistsException e ) {
            EMPLOYEE_TITLE_PROP_ID = dms.getPropertyTypeId( EMPLOYEE_TITLE_PROP_TYPE.getType().getNamespace(),
                    EMPLOYEE_TITLE_PROP_TYPE.getType().getName() );
        }
        try {
            dms.createPropertyType( EMPLOYEE_NAME_PROP_TYPE );
        } catch ( TypeExistsException e ) {
            EMPLOYEE_NAME_PROP_ID = dms.getPropertyTypeId( EMPLOYEE_NAME_PROP_TYPE.getType().getNamespace(),
                    EMPLOYEE_NAME_PROP_TYPE.getType().getName() );
        }
        try {
            dms.createPropertyType( EMPLOYEE_DEPT_PROP_TYPE );
        } catch ( TypeExistsException e ) {
            EMPLOYEE_DEPT_PROP_ID = dms.getPropertyTypeId( EMPLOYEE_DEPT_PROP_TYPE.getType().getNamespace(),
                    EMPLOYEE_DEPT_PROP_TYPE.getType().getName() );
        }
        try {
            dms.createPropertyType( EMPLOYEE_SALARY_PROP_TYPE );
        } catch ( TypeExistsException e ) {
            EMPLOYEE_SALARY_PROP_ID = dms.getPropertyTypeId( EMPLOYEE_SALARY_PROP_TYPE.getType().getNamespace(),
                    EMPLOYEE_SALARY_PROP_TYPE.getType().getName() );
        }
    }

    private static void createEntityTypes( EdmApi dms ) {
        METADATA_LEVELS = createEntityTypeIfNotExists( dms, METADATA_LEVELS );
        METADATA_LEVELS_ID = METADATA_LEVELS.getId();

        METADATA_LEVELS_SATURN = createEntityTypeIfNotExists( dms, METADATA_LEVELS_SATURN );
        METADATA_LEVELS_SATURN_ID = METADATA_LEVELS_SATURN.getId();

        METADATA_LEVELS_MARS = createEntityTypeIfNotExists( dms, METADATA_LEVELS_MARS );
        METADATA_LEVELS_MARS_ID = METADATA_LEVELS_MARS.getId();
    }

    private static EntityType createEntityTypeIfNotExists( EdmApi edmApi, EntityType et ) {
        UUID entityTypeId = edmApi.getEntityTypeId( et.getType().getNamespace(), et.getType().getName() );
        if ( entityTypeId == null ) {
            entityTypeId = edmApi.createEntityType( et );
            return et;
        } else {
            return edmApi.getEntityType( entityTypeId );
        }
    }

    private static void createEntitySets( EdmApi edmApi ) {
        UUID entitySetID = edmApi.getEntitySetId( ENTITY_SET_NAME );

        if ( entitySetID == null ) {
            EMPLOYEES = null;
        } else {
            EMPLOYEES = edmApi.getEntitySet( entitySetID );
        }

        if ( EMPLOYEES == null ) {
            EMPLOYEES = new EntitySet(
                    METADATA_LEVELS_ID,
                    ENTITY_SET_NAME,
                    ENTITY_SET_NAME,
                    Optional.of( "Names and salaries of Chicago employees" ),
                    ImmutableSet.of( "support@openlattice.com" ) );
            edmApi.createEntitySets( ImmutableSet.of( EMPLOYEES ) );
        }
    }
}
