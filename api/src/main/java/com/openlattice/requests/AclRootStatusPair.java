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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class AclRootStatusPair {
    private List<UUID>             aclRoot;
    private EnumSet<RequestStatus> status;

    @JsonCreator
    public AclRootStatusPair(
            @JsonProperty( SerializationConstants.ACL_OBJECT_ROOT ) List<UUID> aclRoot,
            @JsonProperty( SerializationConstants.REQUEST_STATUS ) EnumSet<RequestStatus> status ) {
        this.aclRoot = aclRoot;
        this.status = status;
    }

    @JsonProperty( SerializationConstants.ACL_OBJECT_ROOT )
    public List<UUID> getAclRoot() {
        return aclRoot;
    }

    @JsonProperty( SerializationConstants.REQUEST_STATUS )
    public EnumSet<RequestStatus> getStatus() {
        return status;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        AclRootStatusPair that = (AclRootStatusPair) o;
        return Objects.equals( aclRoot, that.aclRoot ) &&
                Objects.equals( status, that.status );
    }

    @Override public int hashCode() {
        return Objects.hash( aclRoot, status );
    }

    @Override
    public String toString() {
        return "AclRootStatusPair [aclRoot=" + aclRoot + ", status=" + status + "]";
    }
}
