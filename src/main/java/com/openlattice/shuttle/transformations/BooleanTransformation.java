package com.openlattice.shuttle.transformations;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.shuttle.util.Constants;

import java.util.Map;
import java.util.Optional;

public class BooleanTransformation<I> extends Transformation<I> {
    private final SerializableFunction<Map<String, Object>, Object>              trueValueMapper;
    private final SerializableFunction<Map<String, Object>, Object>              falseValueMapper;
    private final Optional<Transformations>                                      transformsIfTrue;
    private final Optional<Transformations>                                      transformsIfFalse;

    /**
     * Represents a selection of transformations based on empty cells.  If either transformsiftrue or transformsiffalse are empty,
     * the value of the tested column will be passed on.
     *
     * @param transformsIfTrue:  transformations to do on column value if exists
     * @param transformsIfFalse: transformations to do if does not exist (note ! define columntransform to choose column !)
     */
    @JsonCreator
    public BooleanTransformation(
            @JsonProperty( Constants.TRANSFORMS_IF_TRUE ) Optional<Transformations> transformsIfTrue,
            @JsonProperty( Constants.TRANSFORMS_IF_FALSE ) Optional<Transformations> transformsIfFalse ) {
        this.transformsIfTrue = transformsIfTrue;
        this.transformsIfFalse = transformsIfFalse;

        // true valuemapper
        if ( transformsIfTrue.isPresent() ) {
            this.trueValueMapper = new TransformValueMapper( transformsIfTrue.get() );
        } else {
            this.trueValueMapper = row -> null;
        }

        // false valuemapper
        if ( transformsIfFalse.isPresent() ) {
            this.falseValueMapper = new TransformValueMapper( transformsIfFalse.get() );
        } else {
            this.falseValueMapper = row -> null;
        }
    }

    protected Map<String, Object> getInputMap( Object o ) {
        ObjectMapper m = ObjectMappers.getJsonMapper();
        Map<String, Object> row = m.convertValue( o, Map.class );
        return row;
    }

    @JsonProperty( Constants.TRANSFORMS_IF_TRUE )
    public Optional<Transformations> getTransformsIfTrue() {
        return transformsIfTrue;
    }

    @JsonProperty( Constants.TRANSFORMS_IF_FALSE )
    public Optional<Transformations> getTransformsIfFalse() {
        return transformsIfFalse;
    }

    public SerializableFunction<Map<String, Object>, Object> getTrueValueMapper() {
        return trueValueMapper;
    }

    public SerializableFunction<Map<String, Object>, Object> getFalseValueMapper() {
        return falseValueMapper;
    }

    public boolean applyCondition( Map<String, Object> row ) {
        return true;
    }

    @Override
    public Object apply( I o ) {
        Map<String, Object> row = getInputMap(o);
        return applyCondition( row ) ? this.trueValueMapper.apply( row ) : this.falseValueMapper.apply( row );
    }
}

