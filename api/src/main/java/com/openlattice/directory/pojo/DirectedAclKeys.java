package com.openlattice.directory.pojo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.authorization.AclKey;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.Objects;

public class DirectedAclKeys {

    private final AclKey target;
    private final AclKey source;

    @JsonCreator
    public DirectedAclKeys(
            @JsonProperty( SerializationConstants.TARGET ) AclKey target,
            @JsonProperty( SerializationConstants.SRC ) AclKey source ) {
        this.target = target;
        this.source = source;
    }

    @JsonProperty( SerializationConstants.TARGET )
    public AclKey getTarget() {
        return target;
    }

    @JsonProperty( SerializationConstants.SRC )
    public AclKey getSource() {
        return source;
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        DirectedAclKeys that = (DirectedAclKeys) o;
        return Objects.equals( target, that.target ) &&
                Objects.equals( source, that.source );
    }

    @Override
    public int hashCode() {
        return Objects.hash( target, source );
    }
}
