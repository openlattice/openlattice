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

package com.openlattice.linking.aggregators;

import com.openlattice.edm.type.PropertyType;
import com.openlattice.linking.LinkingVertex;
import com.openlattice.linking.LinkingVertexKey;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ICountDownLatch;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class MergeVertexAggregator extends Aggregator<Map.Entry<LinkingVertexKey, LinkingVertex>, Void>
        implements HazelcastInstanceAware {
    private static final long serialVersionUID = -3109431240337504574L;

    @SuppressFBWarnings( value = "SE_BAD_FIELD", justification = "Custom Stream Serializer is implemented" )
    private           Map<UUID, PropertyType>         propertyTypesById;
    private           UUID                            graphId;
    private           UUID                            syncId;
    private           Map<UUID, Set<UUID>>            propertyTypeIdsByEntitySet;
    private           Set<UUID>                       propertyTypesToPopulate;
    private           Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataTypeForLinkedEntitySet;
    private transient ICountDownLatch                 countDownLatch;

    private final int MAX_FAILED_CONSEC_ATTEMPTS = 5;

    public MergeVertexAggregator(
            UUID graphId,
            UUID syncId,
            Map<UUID, Set<UUID>> propertyTypeIdsByEntitySet,
            Map<UUID, PropertyType> propertyTypesById,
            Set<UUID> propertyTypesToPopulate,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataTypeForLinkedEntitySet ) {
        this.graphId = graphId;
        this.syncId = syncId;
        this.propertyTypeIdsByEntitySet = propertyTypeIdsByEntitySet;
        this.propertyTypesById = propertyTypesById;
        this.propertyTypesToPopulate = propertyTypesToPopulate;
        this.authorizedPropertiesWithDataTypeForLinkedEntitySet = authorizedPropertiesWithDataTypeForLinkedEntitySet;
//        this.mergingService = mergingService;
    }

    @Override public void accumulate( Map.Entry<LinkingVertexKey, LinkingVertex> input ) {
//        mergingService.mergeEntity( input.getValue().getEntityKeys(),
//                graphId,
//                syncId,
//                propertyTypeIdsByEntitySet,
//                propertyTypesById,
//                propertyTypesToPopulate,
//                authorizedPropertiesWithDataTypeForLinkedEntitySet );
    }

    @Override public void combine( Aggregator aggregator ) {

    }

    @Override public Void aggregate() {
        int numConsecFailures = 0;
        long count = countDownLatch.getCount();
        while ( count > 0 && numConsecFailures < MAX_FAILED_CONSEC_ATTEMPTS ) {
            try {
                Thread.sleep( 5000 );
                long newCount = countDownLatch.getCount();
                if ( newCount == count ) {
                    System.err.println( "Nothing is happening." );
                    numConsecFailures++;
                } else
                    numConsecFailures = 0;
                count = newCount;
            } catch ( InterruptedException e ) {
                System.err.println( "Error occurred while waiting for linking vertices to merge." );
            }
        }
        return null;
    }

    @Override public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        this.countDownLatch = hazelcastInstance.getCountDownLatch( graphId.toString() );
    }

    public UUID getGraphId() {
        return graphId;
    }

    public UUID getSyncId() {
        return syncId;
    }

    public Map<UUID, Set<UUID>> getPropertyTypeIdsByEntitySet() {
        return propertyTypeIdsByEntitySet;
    }

    public Map<UUID, PropertyType> getPropertyTypesById() {
        return propertyTypesById;
    }

    public Set<UUID> getPropertyTypesToPopulate() {
        return propertyTypesToPopulate;
    }

    public Map<UUID, EdmPrimitiveTypeKind> getAuthorizedPropertiesWithDataTypeForLinkedEntitySet() {
        return authorizedPropertiesWithDataTypeForLinkedEntitySet;
    }
}
