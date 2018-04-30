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
 *
 */

package com.openlattice.graph.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public interface Op {
    AtomicInteger idSource = new AtomicInteger(  );

    Integer getId();
    Set<Integer> getOps();

    Op negate();

    boolean isNegated();

    default And and( Op ... ops ) {
        Set<Integer> newOps = new HashSet<>( ops.length + 1 );
        newOps.add( this.getId() );
        for( Op op : ops ) {
            newOps.add( op.getId() );
        }
        return new And( newOps );
    }

    default Or or( Op ... ops ) {
        Set<Integer> newOps = new HashSet<>( ops.length + 1 );
        newOps.add( this.getId() );
        for( Op op : ops ) {
            newOps.add( op.getId() );
        }
        return new Or( newOps );
    }

}
