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

package com.openlattice.hazelcast.serializers;

import com.openlattice.hazelcast.serializers.LinkingEdgeStreamSerializer;
import com.openlattice.linking.LinkingVertexKey;
import java.io.Serializable;

import com.openlattice.linking.LinkingEdge;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import java.util.UUID;

public class LinkingEdgeStreamSerializerTest
        extends AbstractStreamSerializerTest<LinkingEdgeStreamSerializer, LinkingEdge>
        implements Serializable {

    private static final long serialVersionUID = 1020885685339290580L;

    @Override
    protected LinkingEdge createInput() {
        UUID graphId = UUID.randomUUID();
        LinkingVertexKey src = new LinkingVertexKey( graphId, UUID.randomUUID());
        LinkingVertexKey dst = new LinkingVertexKey( graphId, UUID.randomUUID());
        return new LinkingEdge(src, dst);
    }

    @Override
    protected LinkingEdgeStreamSerializer createSerializer() {
        return new LinkingEdgeStreamSerializer();
    }

}