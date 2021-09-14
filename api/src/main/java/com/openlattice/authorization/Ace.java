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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.openlattice.client.serialization.SerializationConstants;

import java.time.OffsetDateTime;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class Ace {
    private final     Principal                principal;
    private final     ImmutableSet<Permission> permissions;
    private final     OffsetDateTime           expirationDate;
    private transient int                      hashValue = 0;

    @JsonCreator
    public Ace(
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal principal,
            @JsonProperty( SerializationConstants.PERMISSIONS ) Set<Permission> permissions,
            @JsonProperty( SerializationConstants.EXPIRATION ) Optional<OffsetDateTime> expirationDate ) {
        this.principal = principal;
        this.permissions = Sets.immutableEnumSet( permissions );
        this.expirationDate = expirationDate.orElse( OffsetDateTime.MAX );
    }

    public Ace(
            Principal principal,
            EnumSet<Permission> permissions ) {
        this( principal, permissions, Optional.empty() );
    }

    public Ace(
            Principal principal,
            EnumSet<Permission> permissions,
            OffsetDateTime expirationDate ) {
        this( principal, permissions, Optional.of( expirationDate ) );
    }

    @JsonProperty( SerializationConstants.PRINCIPAL )
    public Principal getPrincipal() {
        return principal;
    }

    @JsonProperty( SerializationConstants.PERMISSIONS )
    public ImmutableSet<Permission> getPermissions() {
        return permissions;
    }

    @JsonProperty( SerializationConstants.EXPIRATION )
    public OffsetDateTime getExpirationDate() {
        return expirationDate;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        Ace ace = (Ace) o;
        return hashCode() == ace.hashCode() &&
                Objects.equals( principal, ace.principal ) &&
                Objects.equals( permissions, ace.permissions ) &&
                Objects.equals( expirationDate, ace.expirationDate );
    }

    @Override public int hashCode() {
        if ( hashValue == 0 ) {
            hashValue = Objects.hash( principal, permissions, expirationDate );
        }
        return hashValue;
    }

    @Override
    public String toString() {
        return "Ace [principal=" + principal + ", permissions=" + permissions + ", expiration date=" + expirationDate
                + "]";
    }

}
