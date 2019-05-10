/*
 * Copyright (C) 2019. OpenLattice, Inc.
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

package com.openlattice.client.serialization;

import com.dataloom.mappers.ObjectMappers;
import com.esotericsoftware.kryo.util.ObjectMap;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.junit.Test;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class JsonDeserializationTest {
    @Test
    public void testSerdes() throws IOException {
        final var map = ImmutableMap.of( "a", 1, "b", "f", "c", 1.1, "d", Long.MAX_VALUE, "e", Double.MAX_VALUE-1 );
        final var json = ObjectMappers.getJsonMapper().writeValueAsString( map );
        final var readValue = ObjectMappers.getJsonMapper().readValue( json, new TypeReference<Map<String, Object>>() {
        } );
        System.out.println( readValue.toString() );
    }
}
