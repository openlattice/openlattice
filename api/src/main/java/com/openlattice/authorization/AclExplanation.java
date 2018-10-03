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

import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AclExplanation {
    protected final   Principal             principal;
    protected final   List<List<Principal>> paths;
    private transient int                   h = 0;

    @JsonCreator
    public AclExplanation(
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal principal,
            @JsonProperty( SerializationConstants.PRINCIPAL_PATHS ) List<List<Principal>> paths ) {
        this.principal = principal;
        this.paths = paths;
    }

    @JsonProperty( SerializationConstants.PRINCIPAL )
    public Principal getPrincipal() {
        return principal;
    }

    @JsonProperty( SerializationConstants.PRINCIPAL_PATHS )
    public List<List<Principal>> getPaths() {
        return paths;
    }

    @Override public String toString() {
        return "AclExplanation{" +
                "principal=" + principal +
                ", paths=" + paths +
                ", h=" + h +
                '}';
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        AclExplanation that = (AclExplanation) o;
        return h == that.h &&
                Objects.equals( principal, that.principal ) &&
                Objects.equals( paths, that.paths );
    }

    @Override public int hashCode() {
        return Objects.hash( principal, paths, h );
    }

}
