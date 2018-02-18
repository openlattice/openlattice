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

package com.openlattice.authorization;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;

public class Principal implements Comparable<Principal> {

    private final PrincipalType type;

    private final String        id;

    @JsonCreator
    public Principal(
            @JsonProperty( SerializationConstants.TYPE_FIELD ) PrincipalType type,
            @JsonProperty( SerializationConstants.ID_FIELD ) String id ) {
        this.type = type;
        this.id = id;
    }

    @JsonProperty( SerializationConstants.TYPE_FIELD )
    public PrincipalType getType() {
        return type;
    }

    @JsonProperty( SerializationConstants.ID_FIELD )
    public String getId() {
        return id;
    }

    private String validate( String id ) {
        checkArgument( StringUtils.isAllLowerCase( id ),"Principal id must be all lower case" );
        return id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( id == null ) ? 0 : id.hashCode() );
        result = prime * result + ( ( type == null ) ? 0 : type.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        Principal other = (Principal) obj;
        if ( id == null ) {
            if ( other.id != null ) return false;
        } else if ( !id.equals( other.id ) ) return false;
        if ( type != other.type ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "Principal [type=" + type + ", id=" + id + "]";
    }

    @Override
    public int compareTo( Principal o ) {
        int result = type.compareTo( o.getType() );
        
        if( result == 0){
            result = id.compareTo( o.getId() );
        }
        
        return result;
    }

}
