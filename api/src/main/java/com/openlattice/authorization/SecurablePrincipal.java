/*
 * Copyright (C) 2017. OpenLattice, Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.authorization;

import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class SecurablePrincipal extends AbstractSecurableObject {
    private final AclKey    aclKey;
    private final Principal principal;

    @JsonCreator
    public SecurablePrincipal(
            @JsonProperty( SerializationConstants.ID_FIELD ) Optional<UUID> id,
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal principal,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description ) {

        super( id, title, description );
        this.principal = checkNotNull( principal );
        this.aclKey = new AclKey( getId() );
    }

    public SecurablePrincipal(
            AclKey aclKey,
            Principal principal,
            String title,
            Optional<String> description ) {
        super( aclKey.get( aclKey.size() - 1 ), title, description );
        this.principal = principal;
        this.aclKey = aclKey;
    }

    public Principal getPrincipal() {
        return principal;
    }

    @Override
    @JsonIgnore
    public SecurableObjectType getCategory() {
        return SecurableObjectType.Principal;
    }

    @JsonIgnore
    public PrincipalType getPrincipalType() {
        return principal.getType();
    }

    @JsonIgnore
    public String getName() {
        return principal.getId();
    }

    public AclKey getAclKey() {
        return aclKey;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof SecurablePrincipal ) ) { return false; }
        if ( !super.equals( o ) ) { return false; }

        SecurablePrincipal that = (SecurablePrincipal) o;

        if ( !aclKey.equals( that.aclKey ) ) { return false; }
        return principal.equals( that.principal );
    }

    @Override public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + aclKey.hashCode();
        result = 31 * result + principal.hashCode();
        return result;
    }

    @Override public String toString() {
        return "SecurablePrincipal{" +
                "id=" + id +
                ", title='" + title + '\'' +
                ", description='" + description + '\'' +
                ", aclKey=" + aclKey +
                ", principal=" + principal +
                '}';
    }
}
