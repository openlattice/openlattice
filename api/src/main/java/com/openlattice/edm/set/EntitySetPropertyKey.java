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

package com.openlattice.edm.set;

import java.util.UUID;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class EntitySetPropertyKey {

    private UUID entitySetId;
    private UUID propertyTypeId;
    
    @JsonCreator
    public EntitySetPropertyKey(
            @JsonProperty( SerializationConstants.ENTITY_SET_ID ) UUID entitySetId,
            @JsonProperty( SerializationConstants.PROPERTY_TYPE_ID ) UUID propertyTypeId ) {
        this.entitySetId = entitySetId;
        this.propertyTypeId = propertyTypeId;
    }
    
    @JsonProperty( SerializationConstants.ENTITY_SET_ID )
    public UUID getEntitySetId() {
        return entitySetId;
    }
    
    @JsonProperty( SerializationConstants.PROPERTY_TYPE_ID )
    public UUID getPropertyTypeId() {
        return propertyTypeId;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( entitySetId == null ) ? 0 : entitySetId.hashCode() );
        result = prime * result + ( ( propertyTypeId == null ) ? 0 : propertyTypeId.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        EntitySetPropertyKey other = (EntitySetPropertyKey) obj;
        if ( entitySetId == null ) {
            if ( other.entitySetId != null ) return false;
        } else if ( !entitySetId.equals( other.entitySetId ) ) return false;
        if ( propertyTypeId == null ) {
            if ( other.propertyTypeId != null ) return false;
        } else if ( !propertyTypeId.equals( other.propertyTypeId ) ) return false;
        return true;
    }

}
