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

package com.openlattice.hazelcast.serializers;

import com.openlattice.hazelcast.serializers.WeightedLinkingEdgeStreamSerializer;
import com.openlattice.linking.LinkingEdge;
import com.openlattice.linking.LinkingVertexKey;
import com.openlattice.linking.WeightedLinkingEdge;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import java.util.UUID;
import org.apache.commons.lang3.RandomUtils;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class WeightedLinkingEdgeStreamSerializerTest
        extends AbstractStreamSerializerTest<WeightedLinkingEdgeStreamSerializer, WeightedLinkingEdge> {
    @Override protected WeightedLinkingEdgeStreamSerializer createSerializer() {
        return new WeightedLinkingEdgeStreamSerializer();
    }

    @Override protected WeightedLinkingEdge createInput() {
        LinkingVertexKey src = new LinkingVertexKey( UUID.randomUUID(), UUID.randomUUID() );
        LinkingVertexKey dst = new LinkingVertexKey( src.getGraphId(), UUID.randomUUID() );
        return new WeightedLinkingEdge( RandomUtils.nextDouble( 0,1 ), new LinkingEdge( src, dst ) );
    }
}
