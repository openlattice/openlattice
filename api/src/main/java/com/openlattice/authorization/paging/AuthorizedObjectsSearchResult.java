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

package com.openlattice.authorization.paging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.authorization.AclKey;

import java.util.Objects;
import java.util.Set;

public class AuthorizedObjectsSearchResult {
    private String      pagingToken;
    private Set<AclKey> authorizedObjects;

    @JsonCreator
    public AuthorizedObjectsSearchResult(
            @JsonProperty( SerializationConstants.PAGING_TOKEN ) String pagingToken,
            @JsonProperty( SerializationConstants.AUTHORIZED_OBJECTS ) Set<AclKey> authorizedObjects ) {
        this.pagingToken = pagingToken;
        this.authorizedObjects = authorizedObjects;
    }

    @JsonProperty( SerializationConstants.PAGING_TOKEN )
    public String getPagingToken() {
        return pagingToken;
    }

    @JsonProperty( SerializationConstants.AUTHORIZED_OBJECTS )
    public Set<AclKey> getAuthorizedObjects() {
        return authorizedObjects;
    }

    @Override
    public String toString() {
        return "AuthorizedObjectsSearchResult [pagingToken=" + pagingToken + ", authorizedObjects=" + authorizedObjects
                + "]";
    }

    @Override
    public boolean equals( Object other ) {
        if ( this == other ) return true;
        if ( other == null || getClass() != other.getClass() ) return false;
        AuthorizedObjectsSearchResult authorizedObjectsSearchResult = ( AuthorizedObjectsSearchResult ) other;
        return Objects.equals( authorizedObjectsSearchResult.pagingToken, this.pagingToken ) &&
               Objects.equals( authorizedObjectsSearchResult.authorizedObjects, this.authorizedObjects );
    }
}
