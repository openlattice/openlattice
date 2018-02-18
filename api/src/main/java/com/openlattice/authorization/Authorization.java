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

import java.util.Map;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Authorization {
    private AclKey               aclKey;
    private Map<Permission, Boolean> permissions;

    @JsonCreator
    public Authorization( 
            @JsonProperty( SerializationConstants.ACL_OBJECT_PATH ) AclKey aclKey,
            @JsonProperty( SerializationConstants.PERMISSIONS ) Map<Permission, Boolean> permissionsMap ) {
        this.aclKey = aclKey;
        this.permissions = permissionsMap;
    }

    @JsonProperty( SerializationConstants.ACL_OBJECT_PATH )
    public AclKey getAclKey() {
        return aclKey;
    }

    @JsonProperty( SerializationConstants.PERMISSIONS )
    public Map<Permission, Boolean> getPermissions() {
        return permissions;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( permissions == null ) ? 0 : permissions.hashCode() );
        result = prime * result + ( ( aclKey == null ) ? 0 : aclKey.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        Authorization other = (Authorization) obj;
        if ( permissions == null ) {
            if ( other.permissions != null ) return false;
        } else if ( !permissions.equals( other.permissions ) ) return false;
        if ( aclKey == null ) {
            if ( other.aclKey != null ) return false;
        } else if ( !aclKey.equals( other.aclKey ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "Authorization [aclKey=" + aclKey + ", permissionsMap=" + permissions + "]";
    }

}
