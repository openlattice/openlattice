package com.openlattice.shuttle.conditions

import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.openlattice.client.serialization.SerializationConstants.CONDITIONS
import java.util.function.Function

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = CONDITIONS)
abstract class Condition<I> : Function<I, Boolean>