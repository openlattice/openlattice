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

import com.google.common.collect.ImmutableSet;
import com.openlattice.graph.query.AbstractQuery;
import com.openlattice.graph.query.Query;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class AssociationQuery extends AbstractQuery {
    private final Query source;
    private final Query destination;
    private final Query association;

    public AssociationQuery( Query source, Query destination, Query association ) {
        super( ImmutableSet.of() );
        this.source = source;
        this.destination = destination;
        this.association = association;
    }

    public Query getSource() {
        return source;
    }

    public Query getDestination() {
        return destination;
    }

    public Query getAssociation() {
        return association;
    }
}
