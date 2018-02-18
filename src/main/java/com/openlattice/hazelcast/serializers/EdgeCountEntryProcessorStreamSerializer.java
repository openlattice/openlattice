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

import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractUUIDStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.graph.core.objects.EdgeCountEntryProcessor;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
public class EdgeCountEntryProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<EdgeCountEntryProcessor> {
    @Override public Class<EdgeCountEntryProcessor> getClazz() {
        return EdgeCountEntryProcessor.class;
    }

    @Override public void write(
            ObjectDataOutput out, EdgeCountEntryProcessor object ) throws IOException {
        AbstractUUIDStreamSerializer.serialize( out, object.getAssociationTypeId() );
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getNeighborTypeIds() );

    }

    @Override public EdgeCountEntryProcessor read( ObjectDataInput in ) throws IOException {
        UUID associationTypeId = AbstractUUIDStreamSerializer.deserialize( in );
        Set<UUID> neighorhoodTypeIds = SetStreamSerializers.fastUUIDSetDeserialize( in );

        return new EdgeCountEntryProcessor( associationTypeId, neighorhoodTypeIds );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.EDGE_COUNT_ENTRY_PROCESSOR.ordinal();
    }

    @Override public void destroy() {

    }
}
