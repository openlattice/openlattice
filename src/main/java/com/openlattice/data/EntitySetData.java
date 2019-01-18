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

package com.openlattice.data;

import java.util.Iterator;
import java.util.LinkedHashSet;

import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.SetMultimap;

/* 
 * Note: T must have a good .toString() method, as this will be used for serialization
 */
public class EntitySetData<T> implements Iterable<SetMultimap<T, Object>> {

    private static final Logger              logger = LoggerFactory
            .getLogger( EntitySetData.class );

    private LinkedHashSet<String>            columnTitles;
    private Iterable<SetMultimap<T, Object>> entities;

    public EntitySetData(
            LinkedHashSet<String> columnTitles,
            Iterable<SetMultimap<T, Object>> entities ) {
        this.columnTitles = columnTitles;
        this.columnTitles.add( "openlattice.@id" );
        this.entities = entities;
    }

    public LinkedHashSet<String> getColumnTitles() {
        return columnTitles;
    }

    @JsonValue
    public Iterable<SetMultimap<T, Object>> getEntities() {
        return entities::iterator;
    }

    @Override
    public Iterator<SetMultimap<T, Object>> iterator() {
        throw new NotImplementedException( "Purposefully not implemented." );
    }
}
