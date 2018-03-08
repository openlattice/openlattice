

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

package com.openlattice.edm.schemas.manager;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.authorization.securable.AbstractSchemaAssociatedSecurableType;
import com.openlattice.datastore.util.Util;
import com.openlattice.edm.Schema;
import com.openlattice.edm.schemas.SchemaQueryService;
import com.openlattice.edm.schemas.processors.AddSchemasToType;
import com.openlattice.edm.schemas.processors.RemoveSchemasFromType;
import com.openlattice.edm.schemas.processors.SchemaMerger;
import com.openlattice.edm.schemas.processors.SchemaRemover;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

public class HazelcastSchemaManager {
    // map( namespace -> set<name> )
    private final IMap<String, DelegatedStringSet> schemas;
    private final IMap<UUID, PropertyType>         propertyTypes;
    private final IMap<UUID, EntityType>           entityTypes;

    private final SchemaQueryService schemaQueryService;

    public HazelcastSchemaManager(
            HazelcastInstance hazelcastInstance,
            SchemaQueryService schemaQueryService ) {
        this.schemas = checkNotNull( hazelcastInstance.getMap( HazelcastMap.SCHEMAS.name() ) );
        this.propertyTypes = checkNotNull( hazelcastInstance.getMap( HazelcastMap.PROPERTY_TYPES.name() ) );
        this.entityTypes = checkNotNull( hazelcastInstance.getMap( HazelcastMap.ENTITY_TYPES.name() ) );
        this.schemaQueryService = schemaQueryService;
    }

    public void createOrUpdateSchemas( Schema... schemas ) {
        upsertSchemas( ImmutableSet.copyOf( Iterables.transform( Arrays.asList( schemas ), Schema::getFqn ) ) );
    }

    public void addPropertyTypesToSchema( Set<UUID> propertyTypeUuids, FullQualifiedName schemaName ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeUuids ), "Some properties do not exists." );
        Preconditions.checkArgument( checkSchemaExists( schemaName ),
                "Schema %s does not exists.",
                schemaName.getFullQualifiedNameAsString() );
        addTypesToSchema( propertyTypes, propertyTypeUuids, ImmutableList.of( schemaName ) );
    }

    public void addEntityTypesToSchema( Set<UUID> entityTypeUuids, FullQualifiedName schemaName ) {
        Preconditions.checkArgument( checkEntityTypesExist( entityTypeUuids ), "Some entity types do not exists." );
        Preconditions.checkArgument( checkSchemaExists( schemaName ),
                "Schema %s does not exists.",
                schemaName.getFullQualifiedNameAsString() );
        addTypesToSchema( entityTypes, entityTypeUuids, ImmutableList.of( schemaName ) );
    }

    public void removePropertyTypesFromSchema( Set<UUID> propertyTypeUuids, FullQualifiedName schemaName ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeUuids ), "Some properties do not exists." );
        Preconditions.checkArgument( checkSchemaExists( schemaName ),
                "Schema %s does not exists.",
                schemaName.getFullQualifiedNameAsString() );
        removeTypesFromSchema( propertyTypes, propertyTypeUuids, ImmutableList.of( schemaName ) );
    }

    public void removeEntityTypesFromSchema( Set<UUID> entityTypeUuids, FullQualifiedName schemaName ) {
        Preconditions.checkArgument( checkEntityTypesExist( entityTypeUuids ), "Some entity types do not exists." );
        Preconditions.checkArgument( checkSchemaExists( schemaName ),
                "Schema %s does not exists.",
                schemaName.getFullQualifiedNameAsString() );
        removeTypesFromSchema( entityTypes, entityTypeUuids, ImmutableList.of( schemaName ) );
    }

    public Consumer<FullQualifiedName> propertyTypesSchemaAdder( Set<UUID> propertyTypeUuids ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeUuids ), "Some properties do not exists." );
        return schema -> addPropertyTypesToSchema( propertyTypeUuids, schema );
    }

    public Consumer<FullQualifiedName> entityTypesSchemaAdder( Set<UUID> entityTypeUuids ) {
        Preconditions.checkArgument( checkEntityTypesExist( entityTypeUuids ), "Some entity types do not exists." );
        return schema -> addPropertyTypesToSchema( entityTypeUuids, schema );
    }

    public Consumer<FullQualifiedName> propertyTypesSchemaAdder( UUID... propertyTypeUuids ) {
        Set<UUID> properties = ImmutableSet.copyOf( propertyTypeUuids );
        Preconditions.checkArgument( checkPropertyTypesExist( properties ), "Some properties do not exists." );
        return schema -> addPropertyTypesToSchema( properties, schema );
    }

    public Consumer<FullQualifiedName> entityTypesSchemaAdder( UUID... entityTypeUuids ) {
        Set<UUID> types = ImmutableSet.copyOf( entityTypeUuids );
        Preconditions.checkArgument( checkEntityTypesExist( types ), "Some entity types do not exists." );
        return schema -> addEntityTypesToSchema( types, schema );
    }

    public Consumer<FullQualifiedName> propertyTypesSchemaRemover( Set<UUID> propertyTypeUuids ) {
        Preconditions.checkArgument( checkPropertyTypesExist( propertyTypeUuids ), "Some properties do not exists." );
        return schema -> removePropertyTypesFromSchema( propertyTypeUuids, schema );
    }

    public Consumer<FullQualifiedName> entityTypesSchemaRemover( Set<UUID> entityTypeUuids ) {
        Preconditions.checkArgument( checkEntityTypesExist( entityTypeUuids ), "Some entity types do not exists." );
        return schema -> removePropertyTypesFromSchema( entityTypeUuids, schema );
    }

    public Consumer<FullQualifiedName> propertyTypesSchemaRemover( UUID... propertyTypeUuids ) {
        Set<UUID> properties = ImmutableSet.copyOf( propertyTypeUuids );
        Preconditions.checkArgument( checkPropertyTypesExist( properties ), "Some properties do not exists." );
        return schema -> removePropertyTypesFromSchema( properties, schema );
    }

    public Consumer<FullQualifiedName> entityTypesSchemaRemover( UUID... entityTypeUuids ) {
        Set<UUID> types = ImmutableSet.copyOf( entityTypeUuids );
        Preconditions.checkArgument( checkEntityTypesExist( types ), "Some entity types do not exists." );
        return schema -> removeEntityTypesFromSchema( types, schema );
    }

    private <T extends AbstractSchemaAssociatedSecurableType> void addTypesToSchema(
            IMap<UUID, T> m,
            Set<UUID> typeUuids,
            Collection<FullQualifiedName> schemaNames ) {
        AddSchemasToType ep = new AddSchemasToType( schemaNames );
        m.executeOnKeys( typeUuids, ep );
    }

    private <T extends AbstractSchemaAssociatedSecurableType> void removeTypesFromSchema(
            IMap<UUID, T> m,
            Set<UUID> typeUuids,
            Collection<FullQualifiedName> schemaNames ) {
        RemoveSchemasFromType ep = new RemoveSchemasFromType( schemaNames );
        m.executeOnKeys( typeUuids, ep );
    }

    public void upsertSchemas( Set<FullQualifiedName> schemaNames ) {
        /*
         * We can probably optimize this a bit, since not every partition needs all the names, but this works for now.
         */
        Set<String> namespaces = schemaNames.stream()
                .map( FullQualifiedName::getNamespace )
                .collect( Collectors.toSet() );
        Set<String> names = schemaNames.stream()
                .map( FullQualifiedName::getName )
                .collect( Collectors.toSet() );
        schemas.executeOnKeys( namespaces, new SchemaMerger( names ) );
    }

    public void deleteSchema( Set<FullQualifiedName> schemaNames ) {
        Set<String> namespaces = schemaNames.stream()
                .map( FullQualifiedName::getNamespace )
                .collect( Collectors.toSet() );
        Set<String> names = schemaNames.stream()
                .map( FullQualifiedName::getName )
                .collect( Collectors.toSet() );
        schemas.executeOnKeys( namespaces, new SchemaRemover( names ) );
    }

    public Schema getSchema( String namespace, String name ) {
        final FullQualifiedName schemaName = new FullQualifiedName( namespace, name );
        final Collection<PropertyType> pts = propertyTypes
                .getAll( schemaQueryService.getAllPropertyTypesInSchema( schemaName ) ).values();

        final Collection<EntityType> ets = entityTypes
                .getAll( schemaQueryService.getAllEntityTypesInSchema( schemaName ) ).values();
        return new Schema(
                schemaName,
                pts,
                ets );
    }

    public Iterable<Schema> getAllSchemas() {

        final Stream<Schema> loadedSchemas = namespaces()
                .flatMap( namespace -> Util.getSafely( schemas, namespace )
                        .stream()
                        .map( name -> getSchema( namespace, name ) ) );
        return loadedSchemas::iterator;
    }

    public Iterable<Schema> getSchemasInNamespace( String namespace ) {
        return Util
                .getSafely( schemas, namespace )
                .stream()
                .map( name -> getSchema( namespace, name ) )::iterator;
    }

    public Set<UUID> getAllEntityTypesInSchema( FullQualifiedName schemaName ) {
        return schemaQueryService.getAllEntityTypesInSchema( schemaName );
    }

    public Set<UUID> getAllPropertyTypesInSchema( FullQualifiedName schemaName ) {
        return schemaQueryService.getAllPropertyTypesInSchema( schemaName );
    }

    private Stream<String> namespaces() {
        return StreamSupport.stream( schemaQueryService.getNamespaces().spliterator(), false );
    }

    /**
     * Basic Validation.
     * TODO Unify this and the validation in EdmService into a microservice
     */

    public boolean checkSchemaExists( FullQualifiedName schemaName ) {
        Set<String> names = schemas.get( schemaName.getNamespace() );
        return names == null ? false : names.contains( schemaName.getName() );
    }

    public boolean checkPropertyTypesExist( Set<UUID> properties ) {
        return properties.stream().allMatch( propertyTypes::containsKey );
    }

    public boolean checkPropertyTypeExists( UUID propertyTypeId ) {
        return propertyTypes.containsKey( propertyTypeId );
    }

    public boolean checkEntityTypesExist( Set<UUID> types ) {
        return types.stream().allMatch( entityTypes::containsKey );
    }

    public boolean checkEntityTypeExists( UUID entityTypeId ) {
        return entityTypes.containsKey( entityTypeId );
    }

}
