package com.openlattice.analysis.requests;

import com.openlattice.analysis.SqlBindInfo;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Set;
import java.util.stream.Collectors;

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
    public @NonNull Set<SqlBindInfo> bindInfo( int base ) {
        return Set.of( new SqlBindInfo( base, values ) );
    }
}
