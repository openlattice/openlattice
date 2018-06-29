/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.data.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class EntitySetSelection {
    private final Optional<Set<UUID>> properties;
    private final Optional<Set<UUID>> entityKeyIds;

    @JsonCreator
    public EntitySetSelection(
            @JsonProperty( SerializationConstants.PROPERTIES_FIELD ) Optional<Set<UUID>> properties,
            @JsonProperty( SerializationConstants.IDS ) Optional<Set<UUID>> entityKeyIds ) {
        this.properties = properties;
        this.entityKeyIds = entityKeyIds;
    }

    public EntitySetSelection( Optional<Set<UUID>> properties ) {
        this( properties, Optional.empty() );
    }

    @JsonProperty( SerializationConstants.PROPERTIES_FIELD )
    public Optional<Set<UUID>> getProperties() {
        return properties;
    }

    @JsonProperty( SerializationConstants.IDS )
    public Optional<Set<UUID>> getEntityKeyIds() {
        return entityKeyIds;
    }

    @Override public String toString() {
        return "EntitySetSelection{" +
                "properties=" + properties +
                ", entityKeyIds=" + entityKeyIds +
                '}';
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof EntitySetSelection ) ) { return false; }
        EntitySetSelection that = (EntitySetSelection) o;
        return Objects.equals( properties, that.properties ) &&
                Objects.equals( entityKeyIds, that.entityKeyIds );
    }

    @Override public int hashCode() {

        return Objects.hash( properties, entityKeyIds );
    }
}
