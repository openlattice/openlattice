package com.openlattice.analysis.requests;

import java.util.Set;
import java.util.stream.Collectors;

public class ValueFilter<T extends Comparable<T>> implements Filter<T> {
    private Set<T> values;

    public ValueFilter(Set<T> values) {
        this.values = values;
    }

    @Override
    public String asSql(String field) {
        return field +
                " IN (" + String.join(", ", values.stream().map(
                        val -> val.toString()).collect(Collectors.toList())) + ")";
    }
}
