/*
 * Copyright (C) 2020. OpenLattice, Inc
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

package com.openlattice.shuttle;

import com.openlattice.client.serialization.SerializableFunction;

import java.util.Map;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class RowAdapter implements SerializableFunction<Map<String, Object>, Object> {
    private final SerializableFunction<Row, Object> extractor;

    RowAdapter( SerializableFunction<Row, Object> extractor ) {
        this.extractor = extractor;
    }

    @Override public Object apply( final Map<String, Object> row ) {
        return extractor.apply( new Row() {
            @Override public <T> T getAs( String column ) {
                return (T) row.get( column );
            }
        } );
    }

}
