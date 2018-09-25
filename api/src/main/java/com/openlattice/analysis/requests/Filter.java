package com.openlattice.analysis.requests;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo( use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY )
public interface Filter<T extends Comparable<T>> {

    /**
     * @param field Used for constructing the sql expression.
     * @return The sql expression for this filter applied to the specified field.
     */

    String asSql( String field );
}
