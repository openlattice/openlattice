package com.openlattice.analysis.requests;

import java.util.Set;
import java.util.stream.Collectors;

public class ValueFilter<T extends Comparable<T>> implements Filter {
    private final Set<T> values;

    public ValueFilter( Set<T> values ) {
        this.values = values;
    }

    @Override
    public String asSql( String field ) {
        return field +
                " IN (" + values.stream().map( Object::toString ).collect( Collectors.joining( "," ) ) + ")";
    }
}
