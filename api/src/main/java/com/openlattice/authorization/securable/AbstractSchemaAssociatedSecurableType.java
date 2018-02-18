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

package com.openlattice.authorization.securable;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Internal abstract base class for categorical types in the entity data model.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public abstract class AbstractSchemaAssociatedSecurableType extends AbstractSecurableType {
    protected final Set<FullQualifiedName> schemas;

    // TODO: Consider tracking delta since last write to avoid re-writing entire object on each change.

    protected AbstractSchemaAssociatedSecurableType(
            Optional<UUID> id,
            FullQualifiedName type,
            String title,
            Optional<String> description,
            Set<FullQualifiedName> schemas ) {
        super( id, type, title, description );
        this.schemas = Sets.newHashSet( checkNotNull( schemas, "Schemas can be empty, but not null." ) );
    }

    @JsonProperty( SerializationConstants.SCHEMAS )
    public Set<FullQualifiedName> getSchemas() {
        return Collections.unmodifiableSet( schemas );
    }

    public void addToSchemas( Collection<FullQualifiedName> additionalSchemas ) {
        schemas.addAll( additionalSchemas );
    }

    public void removeFromSchemas( Collection<FullQualifiedName> additionalSchemas ) {
        schemas.removeAll( additionalSchemas );
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ( ( schemas == null ) ? 0 : schemas.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( !super.equals( obj ) ) {
            return false;
        }
        if ( !( obj instanceof AbstractSchemaAssociatedSecurableType ) ) {
            return false;
        }
        AbstractSchemaAssociatedSecurableType other = (AbstractSchemaAssociatedSecurableType) obj;
        if ( schemas == null ) {
            if ( other.schemas != null ) {
                return false;
            }
        } else if ( !schemas.equals( other.schemas ) ) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "AbstractSchemaAssociatedSecurableType [schemas=" + schemas + ", type=" + type + ", id=" + id
                + ", title=" + title + ", description=" + description + "]";
    }

}
