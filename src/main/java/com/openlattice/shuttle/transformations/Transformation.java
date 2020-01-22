/*
 * Copyright (C) 2020. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.shuttle.transformations;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.client.serialization.SerializationConstants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.openlattice.shuttle.transformations.Transformation.TRANSFORM;

@JsonTypeInfo( use = Id.CLASS, include = As.PROPERTY, property = TRANSFORM )
public abstract class Transformation<I extends Object> implements Function<I, Object> {
    protected final Logger logger = LoggerFactory.getLogger( getClass() );
    public static final String TRANSFORM = "@transform";

    private final Optional<String> column;

    public Transformation( Optional<String> column ) {
        this.column = column;
    }

    public Transformation() {
        this.column = Optional.empty();
    }

    @JsonProperty( SerializationConstants.COLUMN )
    public String getColumn() {
        return column.orElse( null );
    }

    protected String getInputString( Object o, Optional<String> column ) {
        if ( o == null ) {
            return null;
        }
        if ( !column.isPresent() ) {
            return o.toString();
        }
        ObjectMapper m = ObjectMappers.getJsonMapper();
        Map<String, String> row = m.convertValue( o, Map.class );
        String col = getColumn();
        if ( !row.containsKey(col) ) {
            throw new IllegalStateException( String.format( "The column %s is not found.", column ) );
        }
        return row.get( col );
    }

    protected String applyValueWrapper( String s ) {

        if ( StringUtils.isBlank(s)) return null;

        Object out = applyValue( s );

        if ( out == null ) return null;

        return out.toString();

    }

    protected Object applyValue( String s ) {
        return s;
    }

    @Override
    public Object apply( I o ) {
        return applyValueWrapper( getInputString( o, column ) );
    }
}
