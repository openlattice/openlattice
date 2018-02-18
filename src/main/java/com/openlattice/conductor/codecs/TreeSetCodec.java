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

package com.openlattice.conductor.codecs;

import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.TypeCodec.AbstractCollectionCodec;
import com.datastax.driver.core.TypeTokens;

public class TreeSetCodec<T> extends AbstractCollectionCodec<T, Set<T>> {

    private static final TreeSetCodec<UUID> uuidInstance = new TreeSetCodec<UUID>( TypeCodec.uuid() );
    
    public TreeSetCodec( TypeCodec<T> eltCodec ) {
        super( DataType.set( eltCodec.getCqlType() ), TypeTokens.setOf( eltCodec.getJavaType() ), eltCodec );
    }

    @Override
    protected Set<T> newInstance( int size ) {
        return new TreeSet<T>();
    }

    public static TreeSetCodec<UUID> getUUIDInstance(){
        return uuidInstance;
    }
}
