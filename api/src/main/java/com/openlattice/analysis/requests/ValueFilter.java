package com.openlattice.analysis.requests;

import com.openlattice.analysis.SqlBindInfo;
import java.util.Collections;
import java.util.LinkedHashSet;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Set;

public class ValueFilter<T extends Comparable<T>> implements Filter {
    private final Set<T> values;

    public ValueFilter( Set<T> values ) {
        this.values = values;
    }

    @Override
    public String asSql( String field ) {
        return field + " = ANY(?) ";
    }

    @Override
    public @NonNull LinkedHashSet<SqlBindInfo> bindInfo( int base ) {
        return new LinkedHashSet<>( Collections.singletonList( new SqlBindInfo( base, values ) ) );
    }
}
