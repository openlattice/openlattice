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

package com.openlattice.requests;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.openlattice.authorization.Permission;
import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.authorization.AclKey;
import java.util.EnumSet;
import java.util.Optional;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Request {
    protected final AclKey              aclKey;
    protected final EnumSet<Permission> permissions;
    protected final String              reason;

    @JsonCreator
    public Request(
            @JsonProperty( SerializationConstants.ACL_OBJECT_PATH ) AclKey aclKey,
            @JsonProperty( SerializationConstants.PERMISSIONS ) EnumSet<Permission> permissions,
            @JsonProperty( SerializationConstants.REASON ) Optional<String> reason ) {
        this( aclKey, permissions, reason.orElse( "" ) );
    }

    public Request( AclKey aclKey, EnumSet<Permission> permissions, String reason ) {
        this.aclKey = checkNotNull( aclKey, "AclKey cannot be null." );
        checkState( aclKey.size() > 0, "AclKey must have at least one component." );
        this.permissions = permissions;
        this.reason = reason;
    }

    @JsonProperty( SerializationConstants.ACL_OBJECT_PATH )
    public AclKey getAclKey() {
        return aclKey;
    }

    @JsonProperty( SerializationConstants.PERMISSIONS )
    public EnumSet<Permission> getPermissions() {
        return permissions;
    }

    @JsonProperty( SerializationConstants.REASON )
    public String getReason() {
        return reason;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( aclKey == null ) ? 0 : aclKey.hashCode() );
        result = prime * result + ( ( permissions == null ) ? 0 : permissions.hashCode() );
        result = prime * result + ( ( reason == null ) ? 0 : reason.hashCode() );
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
        if ( !( obj instanceof Request ) ) {
            return false;
        }
        Request other = (Request) obj;
        if ( aclKey == null ) {
            if ( other.aclKey != null ) {
                return false;
            }
        } else if ( !aclKey.equals( other.aclKey ) ) {
            return false;
        }
        if ( permissions == null ) {
            if ( other.permissions != null ) {
                return false;
            }
        } else if ( !permissions.equals( other.permissions ) ) {
            return false;
        }
        if ( reason == null ) {
            if ( other.reason != null ) {
                return false;
            }
        } else if ( !reason.equals( other.reason ) ) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Request [aclKey=" + aclKey + ", permissions=" + permissions + ", reasons=" + reason + "]";
    }

}
