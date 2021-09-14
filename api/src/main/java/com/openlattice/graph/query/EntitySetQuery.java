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
 *
 */

package com.openlattice.graph.query;

import static com.openlattice.client.serialization.SerializationConstants.ENTITY_SET_ID;
import static com.openlattice.client.serialization.SerializationConstants.PROPERTY_TYPE_IDS;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.openlattice.client.serialization.SerializationConstants;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntitySetQuery implements EntityQuery {
    private final int            id;
    private final UUID           entityTypeId;
    private final Optional<UUID> entitySetId;
    private final Set<UUID>      propertyTypes;
    private final BooleanClauses clauses;

    @JsonCreator
    public EntitySetQuery(
            @JsonProperty( SerializationConstants.ID_FIELD ) int id,
            @JsonProperty( SerializationConstants.ENTITY_TYPE_ID ) UUID entityTypeId,
            @JsonProperty( ENTITY_SET_ID ) Optional<UUID> entitySetId,
            @JsonProperty( SerializationConstants.PROPERTY_TYPE_IDS ) Set<UUID> propertyTypes,
            @JsonProperty( SerializationConstants.CLAUSES ) BooleanClauses clauses ) {
        this.id = id;
        this.entitySetId = entitySetId;
        this.entityTypeId = entityTypeId;
        this.propertyTypes = propertyTypes;
        this.clauses = clauses;
    }

    @Override
    @JsonProperty( SerializationConstants.ID_FIELD )
    public int getId() {
        return id;
    }

    @Override
    @JsonProperty( SerializationConstants.ENTITY_TYPE_ID )
    public UUID getEntityTypeId() {
        return entityTypeId;
    }

    @JsonProperty( ENTITY_SET_ID )
    public Optional<UUID> getEntitySetId() {
        return entitySetId;
    }

    @JsonProperty( PROPERTY_TYPE_IDS )
    public Set<UUID> getPropertyTypes() {
        return propertyTypes;
    }

    @JsonProperty( SerializationConstants.CLAUSES )
    public BooleanClauses getClauses() {
        return clauses;
    }

    @Override public Set<EntityQuery> getChildQueries() {
        return ImmutableSet.of();
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof EntitySetQuery ) ) { return false; }
        EntitySetQuery that = (EntitySetQuery) o;
        return Objects.equals( entitySetId, that.entitySetId ) &&
                Objects.equals( entityTypeId, that.entityTypeId );
    }

    @Override public int hashCode() {

        return Objects.hash( entitySetId, entityTypeId );
    }

    @Override public String toString() {
        return "EntitySetQuery{" +
                "entitySetId=" + entitySetId +
                ", entityTypeId=" + entityTypeId +
                '}';
    }
}
