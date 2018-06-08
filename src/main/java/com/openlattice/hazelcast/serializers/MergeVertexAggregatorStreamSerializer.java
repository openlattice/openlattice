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
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.linking.aggregators.MergeVertexAggregator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.springframework.stereotype.Component;

@Component
public class MergeVertexAggregatorStreamSerializer implements SelfRegisteringStreamSerializer<MergeVertexAggregator> {

    @Override public Class<? extends MergeVertexAggregator> getClazz() {
        return MergeVertexAggregator.class;
    }

    @Override
    @SuppressFBWarnings
    public void write(
            ObjectDataOutput out, MergeVertexAggregator object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getGraphId() );
        UUIDStreamSerializer.serialize( out, object.getSyncId() );

        int ptByEsSize = object.getPropertyTypeIdsByEntitySet().size();
        out.writeInt( ptByEsSize );
        for ( Map.Entry<UUID, Set<UUID>> entry : object.getPropertyTypeIdsByEntitySet().entrySet() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            SetStreamSerializers.fastUUIDSetSerialize( out, entry.getValue() );
        }

        int ptByIdSize = object.getPropertyTypesById().size();
        out.writeInt( ptByIdSize );
        for ( Map.Entry<UUID, PropertyType> entry : object.getPropertyTypesById().entrySet() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            PropertyTypeStreamSerializer.serialize( out, entry.getValue() );
        }

        SetStreamSerializers.fastUUIDSetSerialize( out, object.getPropertyTypesToPopulate() );

        int dtSize = object.getAuthorizedPropertiesWithDataTypeForLinkedEntitySet().size();
        out.writeInt( dtSize );
        for ( Map.Entry<UUID, EdmPrimitiveTypeKind> entry : object
                .getAuthorizedPropertiesWithDataTypeForLinkedEntitySet().entrySet() ) {
            UUIDStreamSerializer.serialize( out, entry.getKey() );
            EdmPrimitiveTypeKindStreamSerializer.serialize( out, entry.getValue() );
        }
    }

    @Override
    @SuppressFBWarnings
    public MergeVertexAggregator read( ObjectDataInput in ) throws IOException {
        UUID graphId = UUIDStreamSerializer.deserialize( in );
        UUID syncId = UUIDStreamSerializer.deserialize( in );

        Map<UUID, Set<UUID>> propertyTypeIdsByEntitySet = Maps.newHashMap();
        int ptByEsSize = in.readInt();
        for ( int i = 0; i < ptByEsSize; i++ ) {
            UUID key = UUIDStreamSerializer.deserialize( in );
            Set<UUID> value = SetStreamSerializers.fastUUIDSetDeserialize( in );
            propertyTypeIdsByEntitySet.put( key, value );
        }

        Map<UUID, PropertyType> propertyTypesById = Maps.newHashMap();
        int ptByIdSize = in.readInt();
        for ( int i = 0; i < ptByIdSize; i++ ) {
            UUID key = UUIDStreamSerializer.deserialize( in );
            PropertyType value = PropertyTypeStreamSerializer.deserialize( in );
            propertyTypesById.put( key, value );
        }

        Set<UUID> propertyTypesToPopulate = SetStreamSerializers.fastUUIDSetDeserialize( in );

        Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataTypeForLinkedEntitySet = Maps.newHashMap();
        int dtSize = in.readInt();
        for ( int i = 0; i < dtSize; i++ ) {
            UUID key = UUIDStreamSerializer.deserialize( in );
            EdmPrimitiveTypeKind value = EdmPrimitiveTypeKindStreamSerializer.deserialize( in );
            authorizedPropertiesWithDataTypeForLinkedEntitySet.put( key, value );
        }

        return new MergeVertexAggregator( graphId,
                syncId,
                propertyTypeIdsByEntitySet,
                propertyTypesById,
                propertyTypesToPopulate,
                authorizedPropertiesWithDataTypeForLinkedEntitySet );//mergingService );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.MERGE_VERTEX_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {

    }

    //    public synchronized void setMergingService( HazelcastMergingService mergingService ) {
    //        Preconditions.checkState( this.mergingService == null, "HazelcastMergingService can only be set once" );
    //        this.mergingService = Preconditions.checkNotNull( mergingService );
    //    }
}
