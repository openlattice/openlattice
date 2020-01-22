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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.client.serialization.SerializationConstants;

import java.util.List;
import java.util.Map;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class TransformValueMapper implements SerializableFunction<Map<String, Object>, Object> {
    private final List<Transformation> transforms;

    @JsonCreator
    public TransformValueMapper(
            @JsonProperty( SerializationConstants.TRANSFORMS ) List<Transformation> transforms
    ) {
        this.transforms = transforms;
    }

    @Override
    public Object apply( Map<String, Object> input ) {

        if (transforms.size() == 0){
            throw new IllegalStateException( "At least 1 transformation should be specified (or left blank for columntransform)." );
        }

        Object value = input;
        for ( Transformation t : transforms ) {
            value = t.apply( value );
        }
        return value == "" ? null : value;
    }
}
