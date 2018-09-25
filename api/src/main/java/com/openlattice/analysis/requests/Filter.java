package com.openlattice.analysis.requests;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo( use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY )
public interface Filter<T> {
    String asSql( String field );
}
