/*
 * Copyright (C) 2018. OpenLattice, Inc.
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

package com.openlattice.authorization.paging;

import com.openlattice.authorization.Principal;
import com.datastax.driver.core.PagingState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * Represents all the information needed to do paging when querying backend for authorized objects of a user with a
 * specified securable object type and specified permission. Principal cannot be null, whereas PagingState could be
 * null. (This corresponds to the first ever query made for this principal)
 * 
 * @author Ho Chung Siu
 *
 */
public class AuthorizedObjectsPagingInfo {
    private static final String PRINCIPAL    = "principal";
    private static final String PAGING_STATE = "pagingState";

    private Principal           principal;
    private PagingState         pagingState;

    public AuthorizedObjectsPagingInfo(
            Principal principal,
            PagingState pagingState ) {
        this.principal = Preconditions.checkNotNull( principal );
        this.pagingState = pagingState;
    }

    @JsonCreator
    public AuthorizedObjectsPagingInfo(
            @JsonProperty( PRINCIPAL ) Principal principal,
            @JsonProperty( PAGING_STATE ) String pagingStateString ) {
        this( principal, ( pagingStateString == null ) ? null : PagingState.fromString( pagingStateString ) );
    }

    @JsonProperty( PRINCIPAL )
    public Principal getPrincipal() {
        return principal;
    }

    @JsonIgnore
    public PagingState getPagingState() {
        return pagingState;
    }

    @JsonProperty( PAGING_STATE )
    public String getPagingStateString() {
        return pagingState == null ? null : pagingState.toString();
    }

    @Override
    public String toString() {
        return "AuthorizedObjectsPagingInfo [principal=" + principal + ", pagingState=" + pagingState + "]";
    }

}
