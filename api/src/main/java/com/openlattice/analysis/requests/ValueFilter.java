package com.openlattice.analysis.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;
import java.util.stream.Collectors;

public class ValueFilter<T extends Comparable<T>> implements Filter {
    @JsonProperty("values")
    private final Set<T> values;

    @JsonCreator
    public ValueFilter( @JsonProperty("values") Set<T> values ) {
        this.values = values;
    }

    @Override
    public String asSql( String field ) {
        return field +
                " IN (" + values.stream().map( Object::toString ).collect( Collectors.joining( "," ) ) + ")";
    }
}
