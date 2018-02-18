

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

package com.openlattice.neuron.signals;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.Principal;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.mapstores.TestDataFactory;
import com.openlattice.neuron.SignalType;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Signal {
    private static final Logger       logger = LoggerFactory.getLogger( Signal.class );
    private static final SignalType[] values = SignalType.values();

    private SignalType type;
    private AclKey     aclKey;
    private Principal  principal;
    private String     details;

    @JsonCreator
    public Signal(
            @JsonProperty( SerializationConstants.TYPE_FIELD ) SignalType type,
            @JsonProperty( SerializationConstants.ACL_KEY ) AclKey aclKey,
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal principal,
            @JsonProperty( SerializationConstants.DETAILS_FIELD ) String details ) {

        this.type = checkNotNull( type );
        this.aclKey = aclKey;
        this.principal = principal;

        // TODO: should details be String, or Object?
        if ( details == null ) {
            this.details = "";
        } else {
            this.details = details;
        }
    }

    public Signal( SignalType type, AclKey aclKey, Principal principal ) {

        this( type, aclKey, principal, "" );
    }

    @JsonProperty( SerializationConstants.TYPE_FIELD )
    public SignalType getType() {
        return type;
    }

    @JsonProperty( SerializationConstants.ACL_KEY )
    public AclKey getAclKey() {
        return aclKey;
    }

    @JsonProperty( SerializationConstants.PRINCIPAL )
    public Principal getPrincipal() {
        return principal;
    }

    @JsonProperty( SerializationConstants.DETAILS_FIELD )
    public String getDetails() {
        return details;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof Signal ) ) { return false; }

        Signal signal = (Signal) o;

        if ( type != signal.type ) { return false; }
        if ( !aclKey.equals( signal.aclKey ) ) { return false; }
        if ( !principal.equals( signal.principal ) ) { return false; }
        return details.equals( signal.details );
    }

    @Override public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + aclKey.hashCode();
        result = 31 * result + principal.hashCode();
        result = 31 * result + details.hashCode();
        return result;
    }

    @Override public String toString() {
        return "Signal{" +
                "type=" + type +
                ", aclKey=" + aclKey +
                ", principal=" + principal +
                ", details='" + details + '\'' +
                '}';
    }

    @VisibleForTesting
    public static Signal randomValue() {
        SignalType st = values[ RandomUtils
                .nextInt( values.length ) ];
        return new Signal( st,
                TestDataFactory.aclKey(),
                TestDataFactory.userPrincipal(),
                RandomStringUtils.random( 10 ) );
    }
}
