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
    public static final String NAMESPACE        = "testcsv";
    public static final String SCHEMA_NAME      = "csv";
    public static final String SALARY           = "salary";
    public static final String EMPLOYEE_NAME    = "employee_name";
    public static final String EMPLOYEE_TITLE   = "employee_title";
    public static final String EMPLOYEE_DEPT    = "employee_dept";
    public static final String EMPLOYEE_ID      = "employee_id";
    public static final String ENTITY_SET_NAME  = "Employees";
    public static final String ENTITY_TYPE_NAME = "employee";

    public static final    FullQualifiedName ENTITY_TYPE               = new FullQualifiedName(
            NAMESPACE,
            ENTITY_TYPE_NAME );
    public static final    FullQualifiedName ENTITY_TYPE_MARS          = new FullQualifiedName(
            NAMESPACE,
            "employeeMars" );
    public static final    FullQualifiedName ENTITY_TYPE_SATURN        = new FullQualifiedName(
            NAMESPACE,
            "employeeSaturn" );
    public static          UUID              METADATA_LEVELS_ID        = UUID.randomUUID();
    public static          UUID              METADATA_LEVELS_MARS_ID   = UUID.randomUUID();
    public static          UUID              METADATA_LEVELS_SATURN_ID = UUID.randomUUID();
    public static          UUID              EMPLOYEE_ID_PROP_ID       = UUID.randomUUID();
    protected static final PropertyType      EMPLOYEE_ID_PROP_TYPE     = new PropertyType(
            EMPLOYEE_ID_PROP_ID,
            new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
            "Employee ID",
            Optional
                    .of( "ID of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.Guid );
    public static EntityType METADATA_LEVELS;
    public static EntityType METADATA_LEVELS_SATURN;
    public static EntityType METADATA_LEVELS_MARS;
    public static EntitySet  EMPLOYEES;
    public static          UUID         EMPLOYEE_TITLE_PROP_ID    = UUID.randomUUID();
    public static final    PropertyType EMPLOYEE_TITLE_PROP_TYPE  = new PropertyType(
            EMPLOYEE_TITLE_PROP_ID,
            new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ),
            "Title",
            Optional.of( "Title of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.String );
    public static          UUID         EMPLOYEE_NAME_PROP_ID     = UUID.randomUUID();
    protected static final PropertyType EMPLOYEE_NAME_PROP_TYPE   = new PropertyType(
            EMPLOYEE_NAME_PROP_ID,
            new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ),
            "Name",
            Optional
                    .of( "Name of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.String );
    public static          UUID         EMPLOYEE_DEPT_PROP_ID     = UUID.randomUUID();
    protected static final PropertyType EMPLOYEE_DEPT_PROP_TYPE   = new PropertyType(
            EMPLOYEE_DEPT_PROP_ID,
            new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ),
            "Department",
            Optional
                    .of( "Department of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.String );
    public static          UUID         EMPLOYEE_SALARY_PROP_ID   = UUID.randomUUID();
    protected static final PropertyType EMPLOYEE_SALARY_PROP_TYPE = new PropertyType(
            EMPLOYEE_SALARY_PROP_ID,
            new FullQualifiedName( NAMESPACE, SALARY ),
            "Salary",
            Optional.of( "Salary of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.Int64 );

    static void setupDatamodel( EdmApi edmApi ) {
        createPropertyTypes( edmApi );
        createEntityTypes( edmApi );
        createEntitySets( edmApi );

        edmApi.createSchemaIfNotExists( new Schema(
                new FullQualifiedName( NAMESPACE, SCHEMA_NAME ),
                ImmutableSet.of( EMPLOYEE_ID_PROP_TYPE,
                        EMPLOYEE_TITLE_PROP_TYPE,
                        EMPLOYEE_NAME_PROP_TYPE,
                        EMPLOYEE_DEPT_PROP_TYPE,
                        EMPLOYEE_SALARY_PROP_TYPE ),
                ImmutableSet.of( METADATA_LEVELS, METADATA_LEVELS_MARS, METADATA_LEVELS_SATURN ) ) );

        Assert.assertTrue( edmApi.getEntitySetId( ENTITY_SET_NAME ) != null );
    }

    public static EntityType from( String modifier ) {
        UUID id;
        switch ( modifier ) {
            case "Saturn":
                id = METADATA_LEVELS_SATURN_ID;
                break;
            case "Mars":
                id = METADATA_LEVELS_MARS_ID;
                break;
            case "":
            default:
                id = METADATA_LEVELS_ID;
        }
        return new EntityType(
                id,
                new FullQualifiedName( NAMESPACE, ENTITY_TYPE_NAME + modifier ),
                modifier + " Employees",
                Optional.of( modifier + " Employees of the city of Chicago" ),
                ImmutableSet.of(),
                Sets.newLinkedHashSet( Arrays.asList( EMPLOYEE_ID_PROP_ID ) ),
                Sets.newLinkedHashSet( Arrays.asList( EMPLOYEE_ID_PROP_ID,
                        EMPLOYEE_TITLE_PROP_ID,
                        EMPLOYEE_NAME_PROP_ID,
                        EMPLOYEE_DEPT_PROP_ID,
                        EMPLOYEE_SALARY_PROP_ID ) ),
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) );
    }

    private static void createPropertyTypes( EdmApi dms ) {
        try {
            dms.createPropertyType( EMPLOYEE_ID_PROP_TYPE );
        } catch ( TypeExistsException e ) {
            EMPLOYEE_ID_PROP_ID = dms.getPropertyTypeId( EMPLOYEE_ID_PROP_TYPE.getType().getNamespace(),
                    EMPLOYEE_ID_PROP_TYPE.getType().getName() );
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
        METADATA_LEVELS = from( "" );
        METADATA_LEVELS_SATURN = from( "Saturn" );
        METADATA_LEVELS_MARS = from( "Mars" );

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
