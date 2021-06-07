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

package com.openlattice.edm.type;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class AssociationType {

    private Optional<EntityType> associationEntityType;
    private LinkedHashSet<UUID>  src;
    private LinkedHashSet<UUID>  dst;
    private boolean              bidirectional;

    @JsonCreator
    public AssociationType(
            @JsonProperty( SerializationConstants.ENTITY_TYPE ) Optional<EntityType> associationEntityType,
            @JsonProperty( SerializationConstants.SRC ) LinkedHashSet<UUID> src,
            @JsonProperty( SerializationConstants.DST ) LinkedHashSet<UUID> dst,
            @JsonProperty( SerializationConstants.BIDIRECTIONAL ) boolean bidirectional ) {

        this.associationEntityType = associationEntityType;
        this.src = src;
        this.dst = dst;
        this.bidirectional = bidirectional;
    }

    @JsonProperty( SerializationConstants.ENTITY_TYPE )
    public EntityType getAssociationEntityType() {
        return associationEntityType.orElse( null );
    }

    @JsonProperty( SerializationConstants.SRC )
    public LinkedHashSet<UUID> getSrc() {
        return src;
    }

    @JsonProperty( SerializationConstants.DST )
    public LinkedHashSet<UUID> getDst() {
        return dst;
    }

    @JsonProperty( SerializationConstants.BIDIRECTIONAL )
    public boolean isBidirectional() {
        return bidirectional;
    }

    public void addSrcEntityTypes( Set<UUID> entityTypeIds ) {
        src.addAll( checkNotNull( entityTypeIds, "Src entity types cannot be null." ) );
    }

    public void addDstEntityTypes( Set<UUID> entityTypeIds ) {
        dst.addAll( checkNotNull( entityTypeIds, "Dst entity types cannot be null." ) );
    }

    public void removeSrcEntityTypes( Set<UUID> entityTypeIds ) {
        src.removeAll( checkNotNull( entityTypeIds, "Src entity types cannot be null." ) );
    }

    public void removeDstEntityTypes( Set<UUID> entityTypeIds ) {
        dst.removeAll( checkNotNull( entityTypeIds, "Dst entity types cannot be null." ) );
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( bidirectional ? 1231 : 1237 );
        result = prime * result + ( ( dst == null ) ? 0 : dst.hashCode() );
        result = prime * result + ( ( associationEntityType == null ) ? 0 : associationEntityType.hashCode() );
        result = prime * result + ( ( src == null ) ? 0 : src.hashCode() );
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
        if ( getClass() != obj.getClass() ) {
            return false;
        }
        AssociationType other = (AssociationType) obj;
        if ( bidirectional != other.bidirectional ) {
            return false;
        }
        if ( dst == null ) {
            if ( other.dst != null ) {
                return false;
            }
        } else if ( !dst.equals( other.dst ) ) {
            return false;
        }
        if ( associationEntityType == null ) {
            if ( other.associationEntityType != null ) {
                return false;
            }
        } else if ( !associationEntityType.equals( other.associationEntityType ) ) {
            return false;
        }
        if ( src == null ) {
            if ( other.src != null ) {
                return false;
            }
        } else if ( !src.equals( other.src ) ) {
            return false;
        }
        return true;
    }

}
