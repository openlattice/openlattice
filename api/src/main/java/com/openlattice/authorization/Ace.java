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

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.EnumSet;
import java.util.Set;

public class Ace {
    private final Principal           principal;
    private final EnumSet<Permission> permissions;
    private transient int h = 0;

    @JsonCreator
    public Ace(
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal principal,
            @JsonProperty( SerializationConstants.PERMISSIONS ) Set<Permission> permissions ) {
        this.principal = principal;
        this.permissions = EnumSet.noneOf( Permission.class );
        this.permissions.addAll( permissions );
    }

    @JsonProperty( SerializationConstants.PRINCIPAL )
    public Principal getPrincipal() {
        return principal;
    }

    @JsonProperty( SerializationConstants.PERMISSIONS )
    public EnumSet<Permission> getPermissions() {
        return permissions;
    }

    @Override
    public int hashCode() {
        if ( h == 0 ) {
            final int prime = 31;
            int result = 1;
            result = prime * result + ( ( permissions == null ) ? 0 : permissions.hashCode() );
            result = prime * result + ( ( principal == null ) ? 0 : principal.hashCode() );
            h = result;
        }
        return h;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( obj == null ) {
            return false;
        }
        if ( !( obj instanceof Ace ) ) {
            return false;
        }
        Ace other = (Ace) obj;
        if ( permissions == null ) {
            if ( other.permissions != null ) {
                return false;
            }
        } else if ( !permissions.equals( other.permissions ) ) {
            return false;
        }
        if ( principal == null ) {
            if ( other.principal != null ) {
                return false;
            }
        } else if ( !principal.equals( other.principal ) ) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Ace [principal=" + principal + ", permissions=" + permissions + "]";
    }

}
