

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

package com.openlattice.linking;

import com.openlattice.linking.util.UnorderedPair;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.openlattice.data.EntityKey;
import com.openlattice.linking.util.UnorderedPair;
import com.datastax.driver.core.Row;
import com.openlattice.datastore.cassandra.CommonColumns;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class LinkingUtil {
    public static Set<UUID> requiredEntitySets( Set<Map<UUID, UUID>> linkingProperties ) {
        return linkingProperties
                .stream()
                .map( Map::keySet )
                .flatMap( Set::stream )
                .collect( Collectors.toSet() );
    }

    public static UnorderedPair<EntityKey> getEntityKeyPair( UnorderedPair<Entity> entityPair ) {
        Set<EntityKey> keys = entityPair.getBackingCollection().stream().map( entity -> entity.getKey() )
                .collect( Collectors.toSet() );
        return new UnorderedPair<EntityKey>( keys );
    }

    public static Double edgeValue( Row row ) {
        return row.getDouble( CommonColumns.EDGE_VALUE.cql() );
    }

    public static UUID srcId( Row row ) {
        return row.getUUID( CommonColumns.SOURCE_LINKING_VERTEX_ID.cql() );
    }

    public static UUID dstId( Row row ) {
        return row.getUUID( CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql() );
    }

    public static UUID graphId( Row row ) {
        return row.getUUID( CommonColumns.GRAPH_ID.cql() );
    }

    public static LinkingEdge linkingEdge( Row row ) {
        UUID graphId = graphId( row );

        return new LinkingEdge(
                new LinkingVertexKey( graphId, srcId( row ) ),
                new LinkingVertexKey( graphId, dstId( row ) ) );
    }

    public static WeightedLinkingEdge weightedEdge( Row row ) {
        return new WeightedLinkingEdge( edgeValue( row ), linkingEdge( row ) );
    }
}
