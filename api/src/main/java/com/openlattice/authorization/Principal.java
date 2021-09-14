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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.util.Hashcodes;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Objects;

/**
 * This class represents a principal in the OpenLattice system. It is only serializable because it is used
 * as an internal member of {@link SystemRole} enum members.
 */
public class Principal implements Comparable<Principal>, Serializable {

    private final     PrincipalType type;
    private final     String        id;
    private transient int           h = 0;

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

    private static String validate( String id ) {
        Preconditions.checkArgument( StringUtils.isAllLowerCase( id ), "Principal id must be all lower case" );
        return id;
    }

    @Override
    public int hashCode() {
        if ( h == 0 ) {
            h = Hashcodes.generate( id, type );
        }
        return h;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        Principal principal = (Principal) o;
        return hashCode() == principal.hashCode() &&
                type == principal.type &&
                Objects.equals( id, principal.id );
    }

    @Override
    public String toString() {
        return "Principal [type=" + type + ", id=" + id + "]";
    }

    @Override
    public int compareTo( Principal o ) {
        int result = type.compareTo( o.getType() );

        if ( result == 0 ) {
            result = id.compareTo( o.getId() );
        }

        return result;
    }

}
