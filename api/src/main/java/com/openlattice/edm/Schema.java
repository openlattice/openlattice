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
import com.openlattice.edm.type.PropertyType;
import java.util.Collection;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * This class roughly corresponds to {@link CsdlSchema}
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 * 
 */
public class Schema {
    private final FullQualifiedName        fqn;

    // These columns aren't stored in cassandra
    private final Collection<PropertyType> propertyTypes;
    private final Collection<EntityType>   entityTypes;

    @JsonCreator
    public Schema(
            @JsonProperty( SerializationConstants.FQN ) FullQualifiedName fqn,
            @JsonProperty( SerializationConstants.PROPERTY_TYPES ) Collection<PropertyType> propertyTypes,
            @JsonProperty( SerializationConstants.ENTITY_TYPES ) Collection<EntityType> entityTypes ) {
        this.fqn = Preconditions.checkNotNull( fqn );
        this.entityTypes = Preconditions.checkNotNull( entityTypes );
        this.propertyTypes = Preconditions.checkNotNull( propertyTypes );
    }

    @JsonProperty( SerializationConstants.FQN )
    public FullQualifiedName getFqn() {
        return fqn;
    }

    @JsonProperty( SerializationConstants.ENTITY_TYPES )
    public Collection<EntityType> getEntityTypes() {
        return entityTypes;
    }

    @JsonProperty( SerializationConstants.PROPERTY_TYPES )
    public Collection<PropertyType> getPropertyTypes() {
        return propertyTypes;
    }

    @Override
    public String toString() {
        return "Schema [fqn=" + fqn + ", propertyTypes=" + propertyTypes + ", entityType=" + entityTypes + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( entityTypes == null ) ? 0 : entityTypes.hashCode() );
        result = prime * result + ( ( fqn == null ) ? 0 : fqn.hashCode() );
        result = prime * result + ( ( propertyTypes == null ) ? 0 : propertyTypes.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( obj == null ) {
            return false;
        }
        if ( !( obj instanceof Schema ) ) {
            return false;
        }
        Schema other = (Schema) obj;
        if ( entityTypes == null ) {
            if ( other.entityTypes != null ) {
                return false;
            }
        } else if ( !entityTypes.equals( other.entityTypes ) ) {
            return false;
        }
        if ( fqn == null ) {
            if ( other.fqn != null ) {
                return false;
            }
        } else if ( !fqn.equals( other.fqn ) ) {
            return false;
        }
        if ( propertyTypes == null ) {
            if ( other.propertyTypes != null ) {
                return false;
            }
        } else if ( !propertyTypes.equals( other.propertyTypes ) ) {
            return false;
        }
        return true;
    }

}
