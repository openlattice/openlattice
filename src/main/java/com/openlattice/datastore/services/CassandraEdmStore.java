

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

import java.util.Set;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import com.google.common.util.concurrent.ListenableFuture;
import com.openlattice.datastore.cassandra.Queries;

@Accessor
public interface CassandraEdmStore {
    @Query( Queries.GET_ALL_ENTITY_TYPES_QUERY )
    public Result<EntityType> getEntityTypes();

    @Query( Queries.GET_ALL_ENTITY_TYPES_QUERY )
    public ListenableFuture<Result<EntityType>> getObjectTypesAsync();

    @Query( Queries.GET_ALL_PROPERTY_TYPES_IN_NAMESPACE )
    public Result<PropertyType> getPropertyTypesInNamespace( String namespace );

    @Query( Queries.GET_ALL_PROPERTY_TYPES_QUERY )
    public Result<PropertyType> getPropertyTypes();

    @Query( Queries.CREATE_ENTITY_TYPE_IF_NOT_EXISTS )
    public ResultSet createEntityTypeIfNotExists(
            String namespace,
            String type,
            String typename,
            Set<FullQualifiedName> key,
            Set<FullQualifiedName> properties,
            Set<FullQualifiedName> schemas);

    @Query( Queries.CREATE_PROPERTY_TYPE_IF_NOT_EXISTS )
    public ResultSet createPropertyTypeIfNotExists(
            String namespace,
            String type,
            String typename,
            EdmPrimitiveTypeKind datatype,
            long multiplicity,
            Set<FullQualifiedName> schemas);
    
    @Query( Queries.UPDATE_PROPERTY_TYPE_IF_EXISTS )
    public ResultSet updatePropertyTypeIfExists(
            EdmPrimitiveTypeKind datatype,
            long multiplicity,
            Set<FullQualifiedName> schemas,
            String namespace,
            String type);
    

    @Query( Queries.CREATE_ENTITY_SET_IF_NOT_EXISTS )
    public ResultSet createEntitySetIfNotExists( 
            String typename, 
            String name, 
            String title);

    @Query( Queries.GET_ENTITY_SET_BY_NAME )
    public EntitySet getEntitySet( String name );

    @Query( Queries.GET_ALL_ENTITY_SETS )
    public Result<EntitySet> getEntitySets();
    
    @Query( Queries.GET_ALL_ENTITY_SETS_FOR_ENTITY_TYPE )
    public Result<EntitySet> getEntitySetsForEntityType( String typename );
    
    @Query( Queries.UPDATE_EXISTING_ENTITY_TYPE)
    public ResultSet updateExistingEntityType(
    		@Param(Queries.ParamNames.NAMESPACE) String namespace,
    		@Param(Queries.ParamNames.NAME) String name,
    		@Param(Queries.ParamNames.KEY) Set<FullQualifiedName> key,
    		@Param(Queries.ParamNames.PROPERTIES) Set<FullQualifiedName> properties
    		);
}
