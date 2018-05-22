

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

package com.openlattice.graph;

import com.datastax.driver.core.Row;
import com.openlattice.data.EntityKey;
import com.openlattice.datastore.cassandra.CommonColumns;
import com.openlattice.linking.LinkingEdge;
import com.openlattice.linking.LinkingVertexKey;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public final class GraphUtil {
    private static final Logger logger = LoggerFactory.getLogger( GraphUtil.class );

    private GraphUtil() {
    }

    public static EntityKey min( EntityKey a, EntityKey b ) {
        return a.compareTo( b ) < 0 ? a : b;
    }

    public static EntityKey max( EntityKey a, EntityKey b ) {
        return a.compareTo( b ) > 0 ? a : b;
    }

    public static LinkingEdge linkingEdge( Row row ) {
        final UUID graphId = row.getUUID( CommonColumns.GRAPH_ID.cql() );
        final UUID src = row.getUUID( CommonColumns.SOURCE_LINKING_VERTEX_ID.cql() );
        final UUID dst = row.getUUID( CommonColumns.DESTINATION_LINKING_VERTEX_ID.cql() );

        return new LinkingEdge( new LinkingVertexKey( graphId, src ), new LinkingVertexKey( graphId, dst ) );
    }

    public static Double edgeValue( Row row ) {
        return row.getDouble( CommonColumns.EDGE_VALUE.cql() );
    }
}
