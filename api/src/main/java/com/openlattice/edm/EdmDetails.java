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

package com.openlattice.edm;

import com.openlattice.edm.type.EntityType;
import java.util.Map;
import java.util.UUID;

import com.openlattice.edm.type.PropertyType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class EdmDetails {
    private final Map<UUID, PropertyType> propertyTypes;
    private final Map<UUID, EntityType>   entityTypes;
    private final Map<UUID, EntitySet>    entitySets;

    @JsonCreator
    public EdmDetails(
            @JsonProperty( EdmApi.PROPERTY_TYPES ) Map<UUID, PropertyType> propertyTypes,
            @JsonProperty( EdmApi.ENTITY_TYPES ) Map<UUID, EntityType> entityTypes,
            @JsonProperty( EdmApi.ENTITY_SETS ) Map<UUID, EntitySet> entitySets ) {
        this.propertyTypes = propertyTypes;
        this.entityTypes = entityTypes;
        this.entitySets = entitySets;
    }

    @JsonProperty( EdmApi.PROPERTY_TYPES )
    public Map<UUID, PropertyType> getPropertyTypes() {
        return propertyTypes;
    }

    @JsonProperty( EdmApi.ENTITY_TYPES )
    public Map<UUID, EntityType> getEntityTypes() {
        return entityTypes;
    }

    @JsonProperty( EdmApi.ENTITY_SETS )
    public Map<UUID, EntitySet> getEntitySets() {
        return entitySets;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( entitySets == null ) ? 0 : entitySets.hashCode() );
        result = prime * result + ( ( entityTypes == null ) ? 0 : entityTypes.hashCode() );
        result = prime * result + ( ( propertyTypes == null ) ? 0 : propertyTypes.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        EdmDetails other = (EdmDetails) obj;
        if ( entitySets == null ) {
            if ( other.entitySets != null ) return false;
        } else if ( !entitySets.equals( other.entitySets ) ) return false;
        if ( entityTypes == null ) {
            if ( other.entityTypes != null ) return false;
        } else if ( !entityTypes.equals( other.entityTypes ) ) return false;
        if ( propertyTypes == null ) {
            if ( other.propertyTypes != null ) return false;
        } else if ( !propertyTypes.equals( other.propertyTypes ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "EdmDetails [propertyTypes=" + propertyTypes + ", entityType=" + entityTypes + ", entitySets="
                + entitySets + "]";
    }

}
