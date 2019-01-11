

/*
 * Copyright (C) 2018. OpenLattice, Inc.
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
 */

package com.openlattice.conductor.rpc;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.openlattice.edm.type.PropertyType;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.openlattice.conductor.codecs.TimestampDateTimeTypeCodec;

public final class ResultSetAdapterFactory {

    private ResultSetAdapterFactory() {}

    private static final Map<DataType.Name, TypeCodec> preferredCodec = new HashMap<>();

    static {
        preferredCodec.put( DataType.Name.TIMESTAMP, TimestampDateTimeTypeCodec.getInstance() );
    }

    /**
     * // Only one static method here; should be incorporated in class that writes query result into Cassandra Table
     * 
     * @param mapTypenameToFullQualifiedName a Map that sends Typename to FullQualifiedName
     * @return a Function that converts Row to SetMultimap, which has FullQualifiedName as a key and Row value as the
     *         corresponding value.
     */
    public static Function<Row, SetMultimap<FullQualifiedName, Object>> toSetMultimap(
            final Map<String, FullQualifiedName> mapTypenameToFullQualifiedName ) {
        return ( Row row ) -> {
            List<ColumnDefinitions.Definition> definitions = row.getColumnDefinitions().asList();
            int numOfColumns = definitions.size();

            return IntStream.range( 0, numOfColumns ).boxed().collect(
                    Collector.of(
                            ImmutableSetMultimap.Builder<FullQualifiedName, Object>::new,
                            ( builder, index ) -> builder.put(
                                    mapTypenameToFullQualifiedName.get( definitions.get( index ).getName() ),
                                    row.getObject( index ) ),
                            ( lhs, rhs ) -> lhs.putAll( rhs.build() ),
                            builder -> builder.build() ) );
        };
    }

    public static Entity mapRowToEntity( Row row, Set<PropertyType> properties ) {
        Entity entity = new Entity();
        properties.forEach( property -> {
            Object value = row.getObject( property.getType().getFullQualifiedNameAsString() );
            entity.addProperty( new Property(
                    property.getType().getFullQualifiedNameAsString(),
                    property.getType().getName(),
                    ValueType.PRIMITIVE,
                    value ) );
        } );
        return entity;
    }

    private static Object getObject( Row row, DataType dt, String colName ) {
        Name dtName = dt.getName();
        if ( preferredCodec.containsKey( dtName ) ) {
            return row.get( colName, preferredCodec.get( dtName ) );
        } else {
            return row.getObject( colName );
        }
    }

    /**
     * 
     * @param row Cassandra Row object, expected to have a single column of UUID
     * @return UUID
     */
    public static UUID mapRowToUUID( Row row ) {
        return row.getUUID( 0 );
    }

}