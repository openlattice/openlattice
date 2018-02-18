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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.blocking.GraphEntityPair;
import com.openlattice.blocking.LinkingEntity;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.matching.FeatureExtractionAggregator;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

@Component
public class FeatureExtractionAggregationStreamSerializer implements SelfRegisteringStreamSerializer<FeatureExtractionAggregator> {
    private ConductorElasticsearchApi elasticsearchApi;

    @Override public Class<? extends FeatureExtractionAggregator> getClazz() {
        return FeatureExtractionAggregator.class;
    }

    @Override public void write( ObjectDataOutput out, FeatureExtractionAggregator object ) throws IOException {
        GraphEntityPairStreamSerializer.serialize( out, object.getGraphEntityPair() );
        LinkingEntityStreamSerializer.serialize( out, object.getLinkingEntity() );

        out.writeInt( object.getPropertyTypeIdIndexedByFqn().size() );
        for ( Map.Entry<FullQualifiedName, UUID> entry : object.getPropertyTypeIdIndexedByFqn().entrySet() ) {
            FullQualifiedNameStreamSerializer.serialize( out, entry.getKey() );
            UUIDStreamSerializer.serialize( out, entry.getValue() );
        }

        out.writeDouble( object.getLightest() );
    }

    @Override public FeatureExtractionAggregator read( ObjectDataInput in ) throws IOException {
        GraphEntityPair graphEntityPair = GraphEntityPairStreamSerializer.deserialize( in );
        LinkingEntity linkingEntity = LinkingEntityStreamSerializer.deserialize( in );

        Map<FullQualifiedName, UUID> propertyTypeIdIndexedByFqn = Maps.newHashMap();
        int fqnMapSize = in.readInt();
        for ( int i = 0; i < fqnMapSize; i++ ) {
            FullQualifiedName fqn = FullQualifiedNameStreamSerializer.deserialize( in );
            UUID id = UUIDStreamSerializer.deserialize( in );
            propertyTypeIdIndexedByFqn.put( fqn, id );
        }

        double lightest = in.readDouble();
        return new FeatureExtractionAggregator( graphEntityPair, linkingEntity, propertyTypeIdIndexedByFqn, lightest, elasticsearchApi );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.FEATURE_EXTRACTION_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {

    }


        public synchronized void setConductorElasticsearchApi( ConductorElasticsearchApi api ) {
            Preconditions.checkState( this.elasticsearchApi == null, "Api can only be set once" );
            this.elasticsearchApi = Preconditions.checkNotNull( api );
        }
}
