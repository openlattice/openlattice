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

package com.openlattice.data;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Uniquely identifies a version of an entity in an entity set.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EntityKey implements Comparable<EntityKey> {

    private final UUID entitySetId;
    private final String entityId;

    @JsonCreator
    public EntityKey(
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID entitySetId,
            @JsonProperty( SerializationConstants.ENTITY_ID ) String entityId ) {
        this.entitySetId = checkNotNull( entitySetId );
        this.entityId = checkNotNull( entityId );
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getEntitySetId() {
        return entitySetId;
    }

    @JsonProperty( SerializationConstants.ENTITY_ID )
    public String getEntityId() {
        return entityId;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof EntityKey ) ) { return false; }
        EntityKey entityKey = (EntityKey) o;
        return Objects.equals( entitySetId, entityKey.entitySetId ) &&
                Objects.equals( entityId, entityKey.entityId );
    }

    @Override public int hashCode() {

        return Objects.hash( entitySetId, entityId );
    }

    @Override
    public int compareTo( EntityKey o ) {
        int result = entitySetId.compareTo( o.entitySetId );

        if ( result == 0 ) {
            result = entityId.compareTo( o.entityId );
        }

        return result;
    }

    @Override public String toString() {
        return "EntityKey{" +
                "entitySetId=" + entitySetId +
                ", entityId='" + entityId + '\'' +
                '}';
    }
}
