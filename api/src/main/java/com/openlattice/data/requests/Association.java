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

import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.data.EntityKey;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.SetMultimap;
import java.util.UUID;

public class Association {
    private final EntityKey                 key;
    private final EntityKey                 src;
    private final EntityKey                 dst;
    // This is the actual values of the LinkSet, which can be thought of as "association details" of this association
    private final SetMultimap<UUID, Object> details;

    @JsonCreator
    public Association(
            @JsonProperty( SerializationConstants.KEY_FIELD ) EntityKey key,
            @JsonProperty( SerializationConstants.SRC ) EntityKey src,
            @JsonProperty( SerializationConstants.DST ) EntityKey dst,
            @JsonProperty( SerializationConstants.DETAILS_FIELD ) SetMultimap<UUID, Object> details ) {
        this.key = key;
        this.src = src;
        this.dst = dst;
        this.details = details;
    }

    @JsonProperty( SerializationConstants.KEY_FIELD )
    public EntityKey getKey() {
        return key;
    }

    @JsonProperty( SerializationConstants.SRC )
    public EntityKey getSrc() {
        return src;
    }

    @JsonProperty( SerializationConstants.DST )
    public EntityKey getDst() {
        return dst;
    }

    @JsonProperty( SerializationConstants.DETAILS_FIELD )
    public SetMultimap<UUID, Object> getDetails() {
        return details;
    }

    @Override
    public String toString() {
        return "Association [key=" + key + ", src=" + src + ", dst=" + dst + ", details=" + details + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( details == null ) ? 0 : details.hashCode() );
        result = prime * result + ( ( dst == null ) ? 0 : dst.hashCode() );
        result = prime * result + ( ( key == null ) ? 0 : key.hashCode() );
        result = prime * result + ( ( src == null ) ? 0 : src.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) { return true; }
        if ( obj == null ) { return false; }
        if ( getClass() != obj.getClass() ) { return false; }
        Association other = (Association) obj;
        if ( details == null ) {
            if ( other.details != null ) { return false; }
        } else if ( !details.equals( other.details ) ) { return false; }
        if ( dst == null ) {
            if ( other.dst != null ) { return false; }
        } else if ( !dst.equals( other.dst ) ) { return false; }
        if ( key == null ) {
            if ( other.key != null ) { return false; }
        } else if ( !key.equals( other.key ) ) { return false; }
        if ( src == null ) {
            if ( other.src != null ) { return false; }
        } else if ( !src.equals( other.src ) ) { return false; }
        return true;
    }
}
