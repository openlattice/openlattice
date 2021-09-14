/*
 * Copyright (C) 2020. OpenLattice, Inc
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
import com.openlattice.shuttle.conditions.Condition;
import com.openlattice.shuttle.conditions.ConditionValueMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Serializable;
import java.util.*;

@SuppressFBWarnings( value = "SE_BAD_FIELD", justification = "External representation only" )
public class Flight implements Serializable {

    private static final long serialVersionUID = 2207339044078175121L;
    private static final String anon = "Anon";

    private final Map<String, EntityDefinition>                entityDefinitions;
    private final Map<String, AssociationDefinition>           associationDefinitions;
    private final String                                       name;
    private final Set<String>                                  tags;
    public final  SerializableFunction<Map<String, Object>, ?> valueMapper;
    public final  Optional<List<Condition>>                    condition;
    private final Optional<UUID>                               organizationId;

    @JsonCreator
    public Flight(
            @JsonProperty( SerializationConstants.ENTITY_DEFINITIONS_FIELD )
                    Map<String, EntityDefinition> entityDefinitions,
            @JsonProperty( SerializationConstants.CONDITIONS ) Optional<List<Condition>> condition,
            @JsonProperty( SerializationConstants.ASSOCIATION_DEFINITIONS_FIELD )
                    Optional<Map<String, AssociationDefinition>> associationDefinitions,
            @JsonProperty( SerializationConstants.NAME ) Optional<String> name,
            @JsonProperty( SerializationConstants.TAGS ) Optional<Set<String>> tags,
            @JsonProperty( SerializationConstants.ORGANIZATION_ID ) Optional<UUID> organizationId
    ) {
        this.condition = condition;
        this.entityDefinitions = entityDefinitions;
        this.associationDefinitions = associationDefinitions.orElseGet( ImmutableMap::of );
        this.name = name.orElse( anon );
        this.tags = tags.orElseGet( ImmutableSet::of );
        this.organizationId = organizationId;
        if ( condition.isPresent() ) {
            valueMapper = new ConditionValueMapper( condition.get() );
        } else {
            valueMapper = null;
        }
    }

    private Flight( Builder builder ) {
        entityDefinitions = builder.entityDefinitionMap;
        condition = Optional.empty();
        valueMapper = null;
        associationDefinitions = builder.associationDefinitionMap;
        name = builder.name;
        tags = ImmutableSet.of();
        organizationId = builder.organizationId;
    }

    public static Builder newFlight() {
        return new Builder();
    }

    public static Flight failed() {
        return new Builder().failed();
    }

    public static Builder newFlight( String name ) {
        return new Builder( name );
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
    public Optional<List<Condition>> getCondition() {
        return condition;
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_DEFINITIONS_FIELD )
    public Map<String, AssociationDefinition> getAssociationDefinitions() {
        return associationDefinitions;
    }

    @JsonProperty( SerializationConstants.ORGANIZATION_ID )
    public Optional<UUID> getOrganizationId() {
        return organizationId;
    }

    public static class Builder {

        private Map<String, EntityDefinition>      entityDefinitionMap;
        private Map<String, AssociationDefinition> associationDefinitionMap;
        private String                             name           = "Anon";
        private Optional<UUID>                     organizationId = Optional.empty();

        public Builder() {
            entityDefinitionMap = Maps.newHashMap();
            associationDefinitionMap = Maps.newHashMap();
        }

        public Builder( String name ) {
            entityDefinitionMap = Maps.newHashMap();
            associationDefinitionMap = Maps.newHashMap();
            this.name = name;
        }

        public Flight.Builder organizationId( UUID id ) {
            organizationId = Optional.of( id );
            return this;
        }

        public EntityGroup.Builder createEntities() {

            BuilderCallback<EntityGroup> onBuild = entities ->
                    this.entityDefinitionMap = entities.getEntityDefinitions();

            return new EntityGroup.Builder( this, onBuild );
        }

        public AssociationGroup.Builder createAssociations() {

            BuilderCallback<AssociationGroup> onBuild = associations ->
                    associationDefinitionMap = associations.getAssociationDefinitions();

            return new AssociationGroup.Builder( entityDefinitionMap.keySet(), this, onBuild );
        }

        public Flight failed() {
            return new Flight( this );
        }

        public Flight done() {

            if ( entityDefinitionMap.size() == 0 ) {
                throw new IllegalStateException( "invoking createEntities() at least once is required" );
            }

            return new Flight( this );
        }
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        Flight flight = (Flight) o;
        return Objects.equals( entityDefinitions, flight.entityDefinitions ) &&
                Objects.equals( associationDefinitions, flight.associationDefinitions ) &&
                Objects.equals( name, flight.name ) &&
                Objects.equals( valueMapper, flight.valueMapper ) &&
                Objects.equals( condition, flight.condition ) &&
                Objects.equals( organizationId, flight.organizationId );
    }

    @Override public int hashCode() {
        return Objects.hash( entityDefinitions, associationDefinitions, name, valueMapper, condition, organizationId );
    }

    @Override public String toString() {
        return "Flight{" +
                "entityDefinitions=" + entityDefinitions +
                ", associationDefinitions=" + associationDefinitions +
                ", name='" + name + '\'' +
                ", valueMapper=" + valueMapper +
                ", condition=" + condition +
                ", organizationId=" + organizationId +
                '}';
    }
}
