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

package com.openlattice.organization.roles;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.SecurablePrincipal;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Role extends SecurablePrincipal {

    private static final Logger logger = LoggerFactory.getLogger( Role.class );

    private final UUID organizationId;

    @JsonCreator
    public Role(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.ORGANIZATION_ID ) UUID organizationId,
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal principal,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description ) {
        super( new AclKey( organizationId, id.orElseGet( UUID::randomUUID ) ), principal, title, description );
        checkArgument( principal.getType().equals( PrincipalType.ROLE ) );
        this.organizationId = checkNotNull( organizationId, "Organization id cannot be null." );
    }

    public Role(
            AclKey aclKey,
            Principal principal,
            String title,
            Optional<String> description ) {
        super( aclKey, principal, title, description );
        this.organizationId = aclKey.get( 0 );
    }

    @JsonProperty( SerializationConstants.ORGANIZATION_ID )
    public UUID getOrganizationId() {
        return organizationId;
    }

    @JsonIgnore
    public String getName() {
        return getPrincipal().getId();
    }

    @Override
    @JsonIgnore
    public SecurableObjectType getCategory() {
        return SecurableObjectType.Role;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof Role ) ) { return false; }
        if ( !super.equals( o ) ) { return false; }

        Role role = (Role) o;

        return organizationId.equals( role.organizationId );
    }

    @Override public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + organizationId.hashCode();
        return result;
    }

    @Override public String toString() {
        return "RoleKey { " +
                "organizationId=" + organizationId.toString() +
                ", roleId=" + getId().toString() +
                ", title=" + title +
                " }";
    }
}
