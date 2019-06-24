package com.openlattice.analysis.requests;

import com.openlattice.analysis.SqlBindInfo;
import java.util.Arrays;
import java.util.LinkedHashSet;
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
    public @NonNull LinkedHashSet<SqlBindInfo> bindInfo( int base ) {
        return new LinkedHashSet<>( Arrays.asList( new SqlBindInfo( base, values ) ) );
    }
}
