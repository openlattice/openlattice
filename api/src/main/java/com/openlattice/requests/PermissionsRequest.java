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

import java.util.List;
import java.util.UUID;

import com.openlattice.authorization.Principal;
import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PermissionsRequest {
    private List<UUID>                aclRoot;
    private Principal                 user;
    private PermissionsRequestDetails details;

    @JsonCreator
    public PermissionsRequest(
            @JsonProperty( SerializationConstants.ACL_OBJECT_ROOT ) List<UUID> aclRoot,
            @JsonProperty( SerializationConstants.REQUESTING_USER ) Principal user,
            @JsonProperty( SerializationConstants.DETAILS_FIELD ) PermissionsRequestDetails details ) {
        this.aclRoot = aclRoot;
        this.user = user;
        this.details = details;
    }

    @JsonProperty( SerializationConstants.ACL_OBJECT_ROOT )
    public List<UUID> getAclRoot() {
        return aclRoot;
    }

    @JsonProperty( SerializationConstants.REQUESTING_USER )
    public Principal getUser() {
        return user;
    }

    @JsonProperty( SerializationConstants.DETAILS_FIELD )
    public PermissionsRequestDetails getDetails() {
        return details;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( aclRoot == null ) ? 0 : aclRoot.hashCode() );
        result = prime * result + ( ( details == null ) ? 0 : details.hashCode() );
        result = prime * result + ( ( user == null ) ? 0 : user.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        PermissionsRequest other = (PermissionsRequest) obj;
        if ( aclRoot == null ) {
            if ( other.aclRoot != null ) return false;
        } else if ( !aclRoot.equals( other.aclRoot ) ) return false;
        if ( details == null ) {
            if ( other.details != null ) return false;
        } else if ( !details.equals( other.details ) ) return false;
        if ( user == null ) {
            if ( other.user != null ) return false;
        } else if ( !user.equals( other.user ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "PermissionsRequest [aclRoot=" + aclRoot + ", user=" + user + ", details=" + details + "]";
    }

}
