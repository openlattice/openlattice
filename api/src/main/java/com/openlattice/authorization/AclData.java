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

public class AclData {
    private final Acl    acl;
    private final Action action;

    @JsonCreator
    public AclData(
            @JsonProperty( SerializationConstants.ACL ) Acl acl,
            @JsonProperty( SerializationConstants.ACTION ) Action action ) {
        this.acl = acl;
        this.action = action;
    }

    @JsonProperty( SerializationConstants.ACL ) 
    public Acl getAcl() {
        return acl;
    }

    @JsonProperty( SerializationConstants.ACTION ) 
    public Action getAction() {
        return action;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( acl == null ) ? 0 : acl.hashCode() );
        result = prime * result + ( ( action == null ) ? 0 : action.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        AclData other = (AclData) obj;
        if ( acl == null ) {
            if ( other.acl != null ) return false;
        } else if ( !acl.equals( other.acl ) ) return false;
        if ( action != other.action ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "AclData [acl=" + acl + ", action=" + action + "]";
    }

}
