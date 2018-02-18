/*
 * Copyright (C) 2017. OpenLattice, Inc
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

package com.openlattice.linking.predicates;

import com.openlattice.linking.LinkingEdge;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;

import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public final class LinkingPredicates {
    private LinkingPredicates() {
    }

    public static Predicate minimax( UUID graphId , double minimax ) {
        return Predicates.and( graphId( graphId ), Predicates.lessEqual( "this", minimax ) );
    }

    public static Predicate getAllEdges( LinkingEdge edge ) {
        return Predicates.and(
                Predicates.equal( "__key#graphId", edge.getGraphId() ),
                Predicates.or(
                        Predicates.in("__key#vertexId", edge.getSrcId(), edge.getDstId() ),
                        Predicates.in( "value[any].vertexKey", edge.getSrc(), edge.getDst() ) )
        );
    }

    public static Predicate graphId( UUID graphId ) {
        return Predicates.equal( "__key#graphId", graphId );
    }
    
    public static Predicate entitiesFromEntityKeyIdsAndGraphId( UUID[] entityKeyIds, UUID graphId ) {
        return Predicates.and(
                Predicates.equal( "__key#graphId", graphId ),
                Predicates.in( "__key#entityKeyId", entityKeyIds )
        );
    }
}
