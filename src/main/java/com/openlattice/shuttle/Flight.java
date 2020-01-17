/*
 * Copyright (C) 2017. OpenLattice, Inc
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
 */

package com.openlattice.shuttle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.shuttle.conditions.ConditionValueMapper;
import com.openlattice.shuttle.conditions.Conditions;
import com.openlattice.shuttle.util.Constants;

import java.io.Serializable;
import java.util.*;

public class Flight implements Serializable {

    private static final long serialVersionUID = 2207339044078175121L;
    private static final String anon = "Anon";

    private final Map<String, EntityDefinition>                entityDefinitions;
    private final Map<String, AssociationDefinition>           associationDefinitions;
    private final String                                       name;
    private final Set<String>                                  tags;
    public final  SerializableFunction<Map<String, Object>, ?> valueMapper;
    public final  Optional<Conditions>                         condition;

    @JsonCreator
    public Flight(
            @JsonProperty( SerializationConstants.ENTITY_DEFINITIONS_FIELD )
                    Map<String, EntityDefinition> entityDefinitions,
            @JsonProperty( SerializationConstants.CONDITIONS ) Optional<Conditions> condition,
            @JsonProperty( SerializationConstants.ASSOCIATION_DEFINITIONS_FIELD )
                    Optional<Map<String, AssociationDefinition>> associationDefinitions,
            @JsonProperty( SerializationConstants.NAME ) Optional<String> name,
            @JsonProperty( SerializationConstants.TAGS ) Optional<Set<String>> tags
    ) {
        this.condition = condition;
        this.entityDefinitions = entityDefinitions;
        this.associationDefinitions = associationDefinitions.orElseGet( ImmutableMap::of );
        this.name = name.orElse( anon );
        this.tags = tags.orElseGet( ImmutableSet::of );

        if ( condition.isPresent() ) {
            this.valueMapper = new ConditionValueMapper( condition.get() );
        } else {
            this.valueMapper = null;
        }
    }

    private Flight( Flight.Builder builder ) {
        this.entityDefinitions = builder.entityDefinitionMap;
        this.condition = Optional.empty();
        this.valueMapper = null;
        this.associationDefinitions = builder.associationDefinitionMap;
        this.name = builder.name;
        this.tags = ImmutableSet.of();
    }

    public static Flight.Builder newFlight() {
        return new Flight.Builder();
    }

    public static Flight.Builder newFlight( String name ) {
        return new Flight.Builder( name );
    }

    public String getName() {
        return name;
    }

    public Set<String> getTags() {
        return tags;
    }

    @JsonIgnore
    public Collection<EntityDefinition> getEntities() {
        return this.entityDefinitions.values();
    }

    @JsonProperty( SerializationConstants.ENTITY_DEFINITIONS_FIELD )
    public Map<String, EntityDefinition> getEntityDefinitions() {
        return entityDefinitions;
    }

    @JsonIgnore
    public Collection<AssociationDefinition> getAssociations() {
        return this.associationDefinitions.values();
    }

    @JsonProperty( SerializationConstants.CONDITIONS )
    public Optional<Conditions> getCondition() {
        return condition;
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_DEFINITIONS_FIELD )
    public Map<String, AssociationDefinition> getAssociationDefinitions() {
        return associationDefinitions;
    }

    public static class Builder {

        private Map<String, EntityDefinition>      entityDefinitionMap;
        private Map<String, AssociationDefinition> associationDefinitionMap;
        private String                             name = "Anon";

        public Builder() {
            this.entityDefinitionMap = Maps.newHashMap();
            this.associationDefinitionMap = Maps.newHashMap();
        }

        public Builder( String name ) {
            this.entityDefinitionMap = Maps.newHashMap();
            this.associationDefinitionMap = Maps.newHashMap();
            this.name = name;
        }

        public EntityGroup.Builder createEntities() {

            BuilderCallback<EntityGroup> onBuild = entities ->
                    this.entityDefinitionMap = entities.getEntityDefinitions();

            return new EntityGroup.Builder( this, onBuild );
        }

        public AssociationGroup.Builder createAssociations() {

            BuilderCallback<AssociationGroup> onBuild = associations ->
                    this.associationDefinitionMap = associations.getAssociationDefinitions();

            return new AssociationGroup.Builder( entityDefinitionMap.keySet(), this, onBuild );
        }

        public Flight done() {

            if ( this.entityDefinitionMap.size() == 0 ) {
                throw new IllegalStateException( "invoking createEntities() at least once is required" );
            }

            return new Flight( this );
        }
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof Flight ) ) { return false; }

        Flight flight = (Flight) o;

        if ( !entityDefinitions.equals( flight.entityDefinitions ) ) { return false; }
        if ( !associationDefinitions.equals( flight.associationDefinitions ) ) { return false; }
        return name.equals( flight.name );
    }

    @Override public int hashCode() {
        int result = entityDefinitions.hashCode();
        if ( associationDefinitions != null ) {
            result = 31 * result + associationDefinitions.hashCode();
        }
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override public String toString() {
        return "Flight{" +
                "entityDefinitions=" + entityDefinitions +
                ", associationDefinitions=" + associationDefinitions +
                ", name='" + name + '\'' +
                '}';
    }
}
