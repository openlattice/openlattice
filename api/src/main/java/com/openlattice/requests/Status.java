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

import com.openlattice.authorization.AclKey;
import java.util.EnumSet;

import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Status {
    private final Request       request;
    private final Principal     principal;
    private final RequestStatus status;

    @JsonCreator
    public Status(
            @JsonProperty( SerializationConstants.REQUEST ) Request request,
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal principal,
            @JsonProperty( SerializationConstants.STATUS ) RequestStatus status ) {
        this.request = request;
        this.principal = principal;
        this.status = status;
    }

    public Status(
            AclKey aclKey,
            EnumSet<Permission> permissions,
            String reason,
            Principal principal,
            RequestStatus status ) {
        this( new Request( aclKey, permissions, Optional.fromNullable( reason ) ), principal, status );
    }

    @JsonProperty( SerializationConstants.REQUEST )
    public Request getRequest() {
        return request;
    }

    @JsonProperty( SerializationConstants.STATUS )
    public RequestStatus getStatus() {
        return status;
    }

    @JsonProperty( SerializationConstants.PRINCIPAL )
    public Principal getPrincipal() {
        return principal;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( principal == null ) ? 0 : principal.hashCode() );
        result = prime * result + ( ( request == null ) ? 0 : request.hashCode() );
        result = prime * result + ( ( status == null ) ? 0 : status.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        Status other = (Status) obj;
        if ( principal == null ) {
            if ( other.principal != null ) return false;
        } else if ( !principal.equals( other.principal ) ) return false;
        if ( request == null ) {
            if ( other.request != null ) return false;
        } else if ( !request.equals( other.request ) ) return false;
        if ( status != other.status ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "Status [request=" + request + ", principal=" + principal + ", status=" + status + "]";
    }

}
