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
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.util.Hashcodes;

import java.util.List;
import java.util.UUID;

public class Acl {
    protected final   AclKey    aclKey;
    protected final   Iterable<Ace> aces;
    private transient int           h = 0;

    @JsonCreator
    public Acl(
            @JsonProperty( SerializationConstants.ACL_OBJECT_PATH ) List<UUID> aclKey,
            @JsonProperty( SerializationConstants.ACES ) Iterable<Ace> aces ) {
        this.aclKey = new AclKey(aclKey);
        this.aces = aces;
    }

    @JsonProperty( SerializationConstants.ACL_OBJECT_PATH )
    public AclKey getAclKey() {
        return aclKey;
    }

    @JsonProperty( SerializationConstants.ACES )
    public Iterable<Ace> getAces() {
        return aces;
    }

    @Override
    public int hashCode() {
        if ( h == 0 ) {
            h = Hashcodes.generate( aces, aclKey );
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
        if ( getClass() != obj.getClass() ) {
            return false;
        }
        Acl other = (Acl) obj;
        if ( aces == null ) {
            if ( other.aces != null ) {
                return false;
            }
        } else if ( !aces.equals( other.aces ) ) {
            return false;
        }
        if ( aclKey == null ) {
            if ( other.aclKey != null ) {
                return false;
            }
        } else if ( !aclKey.equals( other.aclKey ) ) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Acl [aclKey=" + aclKey + ", aces=" + aces + "]";
    }

}
