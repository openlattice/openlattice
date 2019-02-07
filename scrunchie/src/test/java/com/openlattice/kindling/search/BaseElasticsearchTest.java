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

package com.openlattice.kindling.search;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService.StaticLoader;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.conductor.rpc.ConductorConfiguration;
import com.openlattice.conductor.rpc.SearchConfiguration;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.organization.Organization;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@SuppressFBWarnings( "MS_PKGPROTECT" )
public class BaseElasticsearchTest {

    protected static final UUID                       ENTITY_SET_ID               = UUID
            .fromString( "0a648f39-5e41-46b5-a928-ec44cdeeae13" );
    protected static final UUID                       ENTITY_TYPE_ID              = UUID
            .fromString( "c271a300-ea05-420b-b33b-8ecb18de5ce7" );
    protected static final UUID                       SYNC_ID                     = UUID.randomUUID();
    protected static final UUID                       SYNC_ID2                    = UUID.randomUUID();
    protected static final String                     TITLE                       = "The Entity Set Title";
    protected static final String                     DESCRIPTION                 = "This is a description for the entity set called employees.";
    protected static final String                     NAMESPACE                   = "testcsv";
    protected static final String                     SALARY                      = "salary";
    protected static final String                     EMPLOYEE_NAME               = "employee_name";
    protected static final String                     EMPLOYEE_TITLE              = "employee_title";
    protected static final String                     EMPLOYEE_DEPT               = "employee_dept";
    protected static final String                     EMPLOYEE_ID                 = "employee_id";
    protected static final String                     WEIGHT                      = "weight";
    protected static final String                     ENTITY_SET_NAME             = "Employees";
    protected static final FullQualifiedName          ENTITY_TYPE                 = new FullQualifiedName(
            NAMESPACE,
            "employee" );
    protected static final int                        ELASTICSEARCH_PORT          = 9300;
    protected static final String                     ELASTICSEARCH_CLUSTER       = "loom_development";
    protected static final String                     ELASTICSEARCH_URL           = "localhost";
    protected static final Logger                     logger                      = LoggerFactory
            .getLogger( BaseElasticsearchTest.class );
    protected static final UUID                       namePropertyId              = UUID
            .fromString( "12926a46-7b2d-4b9c-98db-d6a8aff047f0" );
    protected static final UUID                       employeeIdPropertyId        = UUID
            .fromString( "65d76d13-0d91-4d78-8dbd-cf6ce6e6162f" );
    protected static final UUID                       salaryPropertyId            = UUID
            .fromString( "60de791c-df3e-462b-8299-ea36dc3beb16" );
    protected static final UUID                       employeeDeptPropertyId      = UUID
            .fromString( "4328a8e7-16e1-42a3-ad5b-adf4b06921ec" );
    protected static final UUID                       employeeTitlePropertyId     = UUID
            .fromString( "4a6f084d-cd44-4d5b-9188-947d7151bf84" );
    protected static final List<PropertyType>         propertyTypesList           = Lists.newArrayList();
    protected static final List<PropertyType>         allPropertyTypesList        = Lists.newArrayList();
    protected static final UUID                       chicagoEmployeesEntitySetId = UUID
            .fromString( "15d8f726-74eb-420f-b63e-9774ebc95c3f" );
    protected static final UUID                       entitySet2Id                = UUID
            .fromString( "4c767353-8fcc-4b37-9ff9-bb3ad0ab96e4" );
    protected static final UUID                       organizationId              = UUID
            .fromString( "93e64078-d1a4-4306-a66c-2448d2fd3504" );
    protected static       PropertyType               name;
    protected static       PropertyType               id;
    protected static       PropertyType               salary;
    protected static       PropertyType               dept;
    protected static       PropertyType               title;
    protected static       EntitySet                  chicagoEmployees;
    protected static       EntitySet                  entitySet2;
    protected static       Principal                  owner;
    protected static       Principal                  openlatticeUser;
    protected static       Organization               organization;
    protected static       ConductorElasticsearchImpl elasticsearchApi;

    @BeforeClass
    public static void init() {
        SearchConfiguration config = StaticLoader.loadConfiguration( ConductorConfiguration.class )
                .getSearchConfiguration();
        initEdmObjects();
        elasticsearchApi = new ConductorElasticsearchImpl( config );
    }

    public static void initEdmObjects() {
        name = new PropertyType(
                namePropertyId,
                new FullQualifiedName( "elasticsearchtest", "name" ),
                "Name",
                Optional.of( "Employee Name" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String );
        title = new PropertyType(
                employeeTitlePropertyId,
                new FullQualifiedName( "elasticsearchtest", "title" ),
                "Title",
                Optional.of( "Employee Title" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String );
        dept = new PropertyType(
                employeeDeptPropertyId,
                new FullQualifiedName( "elasticsearchtest", "dept" ),
                "Dept",
                Optional.of( "Employee Department" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String );
        salary = new PropertyType(
                salaryPropertyId,
                new FullQualifiedName( "elasticsearchtest", "salary" ),
                "Salary",
                Optional.of( "Employee Salary" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.Int64 );
        id = new PropertyType(
                employeeIdPropertyId,
                new FullQualifiedName( "elasticsearchtest", "id" ),
                "Id",
                Optional.of( "Employee Id" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.Int64 );
        propertyTypesList.add( dept );
        propertyTypesList.add( id );
        allPropertyTypesList.add( name );
        allPropertyTypesList.add( title );
        allPropertyTypesList.add( dept );
        allPropertyTypesList.add( salary );
        allPropertyTypesList.add( id );

        chicagoEmployees = new EntitySet(
                Optional.of( chicagoEmployeesEntitySetId ),
                ENTITY_TYPE_ID,
                "chicago_employees",
                "Chicago Employees",
                Optional.of( "employees that are in chicago" ),
                ImmutableSet.of( "foo@bar.com", "foobar@foo.net" ),
                Optional.empty(),
                Optional.empty(),
                Optional.of( true ),
                Optional.empty() );
        entitySet2 = new EntitySet(
                Optional.of( entitySet2Id ),
                ENTITY_TYPE_ID,
                "entity_set2",
                "EntitySet2",
                Optional.of( "this is the second entity set" ),
                ImmutableSet.of( "foo@bar.com", "foobar@foo.net" ),
                Optional.empty(),
                Optional.empty(),
                Optional.of( true ),
                Optional.empty() );

        owner = new Principal( PrincipalType.USER, "support@openlattice.com" );
        openlatticeUser = new Principal( PrincipalType.ROLE, "openlatticeUser" );

        organization = new Organization(
                Optional.of( organizationId ),
                new Principal( PrincipalType.ORGANIZATION, UUID.randomUUID().toString() ),
                "Loom Employees",
                Optional.of( "people that work at loom" ),
                Sets.newHashSet(),
                Sets.newHashSet(),
                Sets.newHashSet() );

    }
}
