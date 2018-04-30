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

package com.openlattice.graph;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.openlattice.graph.query.AbstractOp;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class AssociationQuery extends AbstractOp {
    private final Set<Integer> srcEntityQueries;
    private final Set<Integer> dstEntityQueries;
    private final Set<Integer> associationEntityQueries;

    @JsonCreator
    public AssociationQuery(
            Integer id,
            Set<Integer> srcEntityQueries,
            Set<Integer> dstEntityQueries,
            Set<Integer> associationEntityQueries,
            boolean negated ) {
        super( id, aggregateOps( srcEntityQueries, dstEntityQueries, associationEntityQueries ), negated );
        this.srcEntityQueries = srcEntityQueries;
        this.dstEntityQueries = dstEntityQueries;

        this.associationEntityQueries = associationEntityQueries;
    }

    public AssociationQuery(
            Set<Integer> srcEntityQueries,
            Set<Integer> dstEntityQueries,
            Set<Integer> associationEntityQueries,
            boolean negated ) {
        super( aggregateOps( srcEntityQueries, dstEntityQueries, associationEntityQueries ), negated );
        this.srcEntityQueries = srcEntityQueries;
        this.dstEntityQueries = dstEntityQueries;
        this.associationEntityQueries = associationEntityQueries;
    }

    private static Set<Integer> aggregateOps( Set<Integer> srcEQ, Set<Integer> dstEQ, Set<Integer> assocEQ ) {
        final Set<Integer> ops = new HashSet<>( srcEQ.size() + dstEQ.size() + assocEQ.size() );
        ops.addAll( srcEQ );
        ops.addAll( dstEQ );
        ops.addAll( assocEQ );
        return ops;
    }
}
