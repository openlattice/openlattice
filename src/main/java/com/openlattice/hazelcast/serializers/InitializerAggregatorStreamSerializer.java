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

import com.hazelcast.util.Preconditions;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.linking.HazelcastBlockingService;
import com.openlattice.linking.HazelcastLinkingGraphs.Initializer;
import java.io.IOException;
import org.springframework.stereotype.Component;

@Component
public class InitializerAggregatorStreamSerializer implements SelfRegisteringStreamSerializer<Initializer> {

    private HazelcastBlockingService blockingService;

    @Override public Class<? extends Initializer> getClazz() {
        return Initializer.class;
    }

    @Override public void write(
            ObjectDataOutput out, Initializer object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getGraphId() );
    }

    @Override public Initializer read( ObjectDataInput in ) throws IOException {
        return new Initializer( UUIDStreamSerializer.deserialize( in ), blockingService );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.INITIALIZER_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {
    }

    public synchronized void setBlockingService( HazelcastBlockingService blockingService ) {
        Preconditions.checkState( this.blockingService == null, "HazelcastBlockingService can only be set once" );
        this.blockingService = Preconditions.checkNotNull( blockingService );
    }
}
