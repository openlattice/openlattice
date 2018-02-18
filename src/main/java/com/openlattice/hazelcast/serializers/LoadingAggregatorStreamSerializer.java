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
import com.google.common.collect.Maps;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.blocking.GraphEntityPair;
import com.openlattice.blocking.LinkingEntity;
import com.openlattice.blocking.LoadingAggregator;
import com.openlattice.edm.type.PropertyType;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class LoadingAggregatorStreamSerializer implements SelfRegisteringStreamSerializer<LoadingAggregator> {

    @Override public void write( ObjectDataOutput out, LoadingAggregator object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getGraphId() );
        Map<UUID, Map<UUID, PropertyType>> pts = object.getAuthorizedPropertyTypes();
        out.writeInt( pts.size() );
        for ( Map.Entry<UUID, Map<UUID, PropertyType>> entry : pts.entrySet() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            out.writeInt( entry.getValue().size() );
            for ( Map.Entry<UUID, PropertyType> valueEntry : entry.getValue().entrySet() ) {
                UUIDStreamSerializer.serialize( out, valueEntry.getKey() );
                PropertyTypeStreamSerializer.serialize( out, valueEntry.getValue() );
            }
        }

        Map<GraphEntityPair, LinkingEntity> entities = object.getEntities();
        out.writeInt( entities.size() );
        for ( Map.Entry<GraphEntityPair, LinkingEntity> entry : entities.entrySet() ) {
            GraphEntityPairStreamSerializer.serialize( out, entry.getKey() );
            LinkingEntityStreamSerializer.serialize( out, entry.getValue() );
        }
    }

    @Override public LoadingAggregator read( ObjectDataInput in ) throws IOException {
        UUID graphId = UUIDStreamSerializer.deserialize( in );

        Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = Maps.newHashMap();

        int numEntries = in.readInt();
        for ( int i = 0; i < numEntries; i++ ) {
            UUID entitySetId = UUIDStreamSerializer.deserialize( in );
            Map<UUID, PropertyType> pts = Maps.newHashMap();
            int numValueEntries = in.readInt();
            for ( int j = 0; j < numValueEntries; j++ ) {
                UUID propertyTypeId = UUIDStreamSerializer.deserialize( in );
                PropertyType pt = PropertyTypeStreamSerializer.deserialize( in );
                pts.put( propertyTypeId, pt );
            }
            authorizedPropertyTypes.put( entitySetId, pts );
        }

        Map<GraphEntityPair, LinkingEntity> entities = Maps.newHashMap();
        int entitiesMapSize = in.readInt();
        for (int i = 0; i < entitiesMapSize; i++ ) {
            GraphEntityPair key = GraphEntityPairStreamSerializer.deserialize( in );
            LinkingEntity value = LinkingEntityStreamSerializer.deserialize( in );
            entities.put( key, value );
        }

        return new LoadingAggregator( graphId, authorizedPropertyTypes, entities );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.LOADING_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {
    }

    @Override public Class<? extends LoadingAggregator> getClazz() {
        return LoadingAggregator.class;
    }
}
