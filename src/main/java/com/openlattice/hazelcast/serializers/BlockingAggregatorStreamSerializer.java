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

import com.google.common.collect.Maps;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Preconditions;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.blocking.BlockingAggregator;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.linking.HazelcastBlockingService;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

@Component
public class BlockingAggregatorStreamSerializer implements SelfRegisteringStreamSerializer<BlockingAggregator> {

    private HazelcastBlockingService blockingService;

    @Override public Class<? extends BlockingAggregator> getClazz() {
        return BlockingAggregator.class;
    }

    @Override public void write( ObjectDataOutput out, BlockingAggregator object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getGraphId() );
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getEntitySetIds() );

        out.writeInt( object.getPropertyTypesIndexedByFqn().size() );
        for ( Map.Entry<FullQualifiedName, UUID> entry : object.getPropertyTypesIndexedByFqn().entrySet() ) {
            FullQualifiedNameStreamSerializer.serialize( out, entry.getKey() );
            UUIDStreamSerializer.serialize( out, entry.getValue() );
        }
    }

    @Override public BlockingAggregator read( ObjectDataInput in ) throws IOException {
        UUID graphId = UUIDStreamSerializer.deserialize( in );
        Iterable<UUID> entitySetIds = SetStreamSerializers.fastUUIDSetDeserialize( in );

        Map<FullQualifiedName, UUID> propertyTypeIdIndexedByFqn = Maps.newHashMap();
        int fqnMapSize = in.readInt();
        for ( int i = 0; i < fqnMapSize; i++ ) {
            FullQualifiedName fqn = FullQualifiedNameStreamSerializer.deserialize( in );
            UUID id = UUIDStreamSerializer.deserialize( in );
            propertyTypeIdIndexedByFqn.put( fqn, id );
        }

        return new BlockingAggregator( graphId, entitySetIds, propertyTypeIdIndexedByFqn, blockingService );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.BLOCKING_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {
    }

    public synchronized void setBlockingService( HazelcastBlockingService blockingService ) {
        Preconditions.checkState( this.blockingService == null, "HazelcastBlockingService can only be set once" );
        this.blockingService = Preconditions.checkNotNull( blockingService );
    }
}
