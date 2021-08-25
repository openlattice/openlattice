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

package com.openlattice.scrunchie.search;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.Principal;
import com.openlattice.data.EntityDataKey;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.openlattice.search.requests.SearchConstraints;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class ScrunchieElasticsearchTests extends BaseElasticsearchTest {

    @Test
    public void testEntitySetKeywordSearch() {
        Set<Principal> principals = Sets.newHashSet();
        principals.add( openlatticeUser );

        String query = "Employees";
        elasticsearchApi.executeEntitySetMetadataSearch(
                Optional.of( query ),
                Optional.of( ENTITY_TYPE_ID ),
                Optional.empty(),
                false,
                ImmutableSet.of( new AclKey( chicagoEmployeesEntitySetId ) ),
                0,
                50 );
    }

    @Test
    public void testUpdatePropertyTypes() {
        elasticsearchApi.updatePropertyTypesInEntitySet( chicagoEmployeesEntitySetId, allPropertyTypesList );
    }

    @Test
    public void testSearchEntityData() {
        DelegatedUUIDSet authorizedPropertyTypes = DelegatedUUIDSet.wrap( Sets.newHashSet() );
        authorizedPropertyTypes.add( namePropertyId );
        authorizedPropertyTypes.add( employeeTitlePropertyId );
        authorizedPropertyTypes.add( employeeDeptPropertyId );
        authorizedPropertyTypes.add( salaryPropertyId );
        authorizedPropertyTypes.add( employeeIdPropertyId );
        elasticsearchApi
                .executeSearch( SearchConstraints.simpleSearchConstraints( new UUID[]{ chicagoEmployeesEntitySetId },
                        0,
                        50,
                        "police",
                        false ),
                        ImmutableMap.of( chicagoEmployeesEntitySetId, ENTITY_TYPE_ID ),
                        ImmutableMap.of( chicagoEmployeesEntitySetId, authorizedPropertyTypes ),
                        ImmutableMap.of() );
    }

    @Test
    public void testSearchMultipleEntityData() {
        UUID[] entitySetIds = new UUID[]{ chicagoEmployeesEntitySetId, entitySet2Id };
        DelegatedUUIDSet authorizedPropertyTypes = DelegatedUUIDSet.wrap( Sets.newHashSet() );
        authorizedPropertyTypes.add( employeeIdPropertyId );
        elasticsearchApi
                .executeSearch( SearchConstraints.simpleSearchConstraints(
                        entitySetIds,
                        0,
                        50,
                        "12347",
                        false ),
                        ImmutableMap.of( chicagoEmployeesEntitySetId, ENTITY_TYPE_ID, entitySet2Id, ENTITY_TYPE_ID ),
                        ImmutableMap.of( chicagoEmployeesEntitySetId, authorizedPropertyTypes, entitySet2Id, authorizedPropertyTypes ),
                        ImmutableMap.of() );
    }

    @Test
    public void testOrganizationKeywordSearch() {
        Set<Principal> principals = Sets.newHashSet();
        principals.add( owner );
        elasticsearchApi.executeOrganizationSearch( "loom", ImmutableSet.of( new AclKey( organizationId ) ), 0, 50 );
    }

    @BeforeClass
    public static void createIndicesAndData() {
        elasticsearchApi.saveEntityTypeToElasticsearch( entityType, allPropertyTypesList );
        elasticsearchApi.saveEntitySetToElasticsearch( entityType, chicagoEmployees, propertyTypesList );
        elasticsearchApi.saveEntitySetToElasticsearch( entityType, entitySet2, propertyTypesList );
        elasticsearchApi.createOrganization( organization );
        createEntityData();
    }

    public static void createEntityData() {
        SetMultimap<UUID, Object> propertyValues1 = HashMultimap.create();
        propertyValues1.put( namePropertyId, Sets.newHashSet( "APOSTOLOS,  DIMITRIOS M" ) );
        propertyValues1.put( employeeTitlePropertyId, Sets.newHashSet( "ASST CHIEF OPERATING ENGINEER" ) );
        propertyValues1.put( employeeDeptPropertyId, Sets.newHashSet( "AVIATION" ) );
        propertyValues1.put( salaryPropertyId, Sets.newHashSet( "108534" ) );
        propertyValues1.put( employeeIdPropertyId, Sets.newHashSet( "12345" ) );
        SetMultimap<UUID, Object> propertyValues2 = HashMultimap.create();
        propertyValues2.put( namePropertyId, Sets.newHashSet( "ALVAREZ,  ROBERT" ) );
        propertyValues2.put( employeeTitlePropertyId, Sets.newHashSet( "POLICE OFFICER" ) );
        propertyValues2.put( employeeDeptPropertyId, Sets.newHashSet( "POLICE" ) );
        propertyValues2.put( salaryPropertyId, Sets.newHashSet( "81550" ) );
        propertyValues2.put( employeeIdPropertyId, Sets.newHashSet( "12346" ) );
        SetMultimap<UUID, Object> propertyValues3 = HashMultimap.create();
        propertyValues3.put( namePropertyId, Sets.newHashSet( "ALTMAN,  PATRICIA A" ) );
        propertyValues3.put( employeeTitlePropertyId, Sets.newHashSet( "POLICE OFFICER" ) );
        propertyValues3.put( employeeDeptPropertyId, Sets.newHashSet( "POLICE" ) );
        propertyValues3.put( salaryPropertyId, Sets.newHashSet( "93240" ) );
        propertyValues3.put( employeeIdPropertyId, Sets.newHashSet( "12347" ) );
        elasticsearchApi.createEntityData( ENTITY_TYPE_ID, new EntityDataKey( chicagoEmployeesEntitySetId, UUID.randomUUID() ),
                Multimaps.asMap( propertyValues1 ) );
        elasticsearchApi.createEntityData( ENTITY_TYPE_ID, new EntityDataKey( chicagoEmployeesEntitySetId, UUID.randomUUID() ),
                Multimaps.asMap( propertyValues2 ) );
        elasticsearchApi.createEntityData( ENTITY_TYPE_ID, new EntityDataKey( chicagoEmployeesEntitySetId, UUID.randomUUID() ),
                Multimaps.asMap( propertyValues3 ) );

        SetMultimap<UUID, Object> entitySet2PropertyValues = HashMultimap.create();
        entitySet2PropertyValues.put( employeeDeptPropertyId, Sets.newHashSet( "POLICE" ) );
        entitySet2PropertyValues.put( employeeIdPropertyId, Sets.newHashSet( "12347" ) );
        elasticsearchApi
                .createEntityData( ENTITY_TYPE_ID, new EntityDataKey( entitySet2Id, UUID.randomUUID() ),
                        Multimaps.asMap( entitySet2PropertyValues ) );
    }

    @AfterClass
    public static void deleteIndices() {
        elasticsearchApi.deleteEntitySet( chicagoEmployeesEntitySetId, ENTITY_TYPE_ID );
        elasticsearchApi.deleteEntitySet( entitySet2Id, ENTITY_TYPE_ID );
    }

}
