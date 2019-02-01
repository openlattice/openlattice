package com.openlattice.search.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ConstraintGroup {

    private final int              minimumMatches;
    private final List<Constraint> constraints;

    @JsonCreator
    public ConstraintGroup(
            @JsonProperty( SerializationConstants.MIN ) Optional<Integer> minimumMatches,
            @JsonProperty( SerializationConstants.CONSTRAINTS ) List<Constraint> constraints ) {
        this.minimumMatches = minimumMatches.orElse( 1 );
        this.constraints = constraints;

        Preconditions.checkArgument( this.constraints.size() > 0, "Constraint groups must not be empty" );
        Preconditions.checkArgument( this.minimumMatches > 0, "Minumum number of matches must be at least 1" );
        Preconditions.checkArgument( this.minimumMatches <= constraints.size(),
                "Minimum number of matches cannot be greater than the number of constraints" );
    }

    public ConstraintGroup( int minimumMatches, List<Constraint> constraints ) {
        this( Optional.of( minimumMatches ), constraints );
    }

    public ConstraintGroup( List<Constraint> constraints ) {
        this( Optional.empty(), constraints );
    }

    @JsonProperty( SerializationConstants.MIN )
    public int getMinimumMatches() {
        return minimumMatches;
    }

    @JsonProperty( SerializationConstants.CONSTRAINTS )
    public List<Constraint> getConstraints() {
        return constraints;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        ConstraintGroup that = (ConstraintGroup) o;
        return minimumMatches == that.minimumMatches &&
                Objects.equals( constraints, that.constraints );
    }

    @Override public int hashCode() {

        return Objects.hash( minimumMatches, constraints );
    }
}
