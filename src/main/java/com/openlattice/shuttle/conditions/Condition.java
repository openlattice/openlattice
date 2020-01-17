package com.openlattice.shuttle.conditions;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.function.Function;

import static com.openlattice.client.serialization.SerializationConstants.CONDITIONS;

@JsonTypeInfo( use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = CONDITIONS )
public abstract class Condition<I> implements Function<I, Boolean> {
    public static final String CONDITIONS = "@condition";
}