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

package com.openlattice.matching;

import com.openlattice.blocking.GraphEntityPair;
import com.openlattice.blocking.LinkingEntity;
import com.openlattice.linking.HazelcastLinkingGraphs;
import com.openlattice.linking.LinkingEdge;
import com.openlattice.linking.LinkingVertexKey;
import com.openlattice.linking.util.PersonMetric;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.linking.HazelcastLinkingGraphs;
import com.openlattice.linking.LinkingEdge;
import com.openlattice.linking.LinkingVertexKey;
import com.openlattice.linking.util.PersonMetric;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Map;
import java.util.UUID;

public class FeatureExtractionAggregator extends Aggregator<Map.Entry<GraphEntityPair, LinkingEntity>, Double>
        implements HazelcastInstanceAware {
    private static final long serialVersionUID = -8460238073748062034L;

    private GraphEntityPair graphEntityPair;
    private LinkingEntity   linkingEntity;
    private double lightest = Double.MAX_VALUE;
    private Map<FullQualifiedName, UUID> propertyTypeIdIndexedByFqn;

    private transient HazelcastLinkingGraphs    graphs           = null;
    private transient ConductorElasticsearchApi elasticsearchApi = null;

    public FeatureExtractionAggregator(
            GraphEntityPair graphEntityPair,
            LinkingEntity linkingEntity,
            Map<FullQualifiedName, UUID> propertyTypeIdIndexedByFqn ) {
        this(
                graphEntityPair,
                linkingEntity,
                propertyTypeIdIndexedByFqn,
                Double.MAX_VALUE,
                null );
    }

    public FeatureExtractionAggregator(
            GraphEntityPair graphEntityPair,
            LinkingEntity linkingEntity,
            Map<FullQualifiedName, UUID> propertyTypesIndexedByFqn,
            double lightest,
            ConductorElasticsearchApi elasticsearchApi ) {
        this.graphEntityPair = graphEntityPair;
        this.linkingEntity = linkingEntity;
        this.propertyTypeIdIndexedByFqn = propertyTypesIndexedByFqn;
        this.lightest = lightest;
        this.elasticsearchApi = elasticsearchApi;
    }

    @Override
    public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
        this.graphs = new HazelcastLinkingGraphs( hazelcastInstance );
    }

    @Override
    public void accumulate( Map.Entry<GraphEntityPair, LinkingEntity> input ) {
        UUID graphId = graphEntityPair.getGraphId();
        UUID ek1 = graphEntityPair.getEntityKeyId();
        UUID ek2 = input.getKey().getEntityKeyId();

        if ( !ek1.equals( ek2 ) ) {
            LinkingVertexKey u = new LinkingVertexKey( graphId, ek1 );
            LinkingVertexKey v = new LinkingVertexKey( graphId, ek2 );
            final LinkingEdge edge = new LinkingEdge( u, v );

            Map<UUID, DelegatedStringSet> e1 = linkingEntity.getEntity();
            Map<UUID, DelegatedStringSet> e2 = input.getValue().getEntity();

            double[] dist = PersonMetric.pDistance( e1, e2, propertyTypeIdIndexedByFqn );
            double[][] features = new double[ 1 ][ 0 ];
            features[ 0 ] = dist;
            double weight = elasticsearchApi.getModelScore( features );
            lightest = Math.min( lightest, weight );
            graphs.setEdgeWeight( edge, weight );
        }

    }

    @Override
    public void combine( Aggregator aggregator ) {
        if ( aggregator instanceof FeatureExtractionAggregator ) {
            FeatureExtractionAggregator other = (FeatureExtractionAggregator) aggregator;
            if ( other.lightest < lightest )
                lightest = other.lightest;
        }

    }

    @Override
    public Double aggregate() {
        return lightest;
    }

    public GraphEntityPair getGraphEntityPair() {
        return graphEntityPair;
    }

    public LinkingEntity getLinkingEntity() {
        return linkingEntity;
    }

    public Map<FullQualifiedName, UUID> getPropertyTypeIdIndexedByFqn() {
        return propertyTypeIdIndexedByFqn;
    }

    public double getLightest() {
        return lightest;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        FeatureExtractionAggregator that = (FeatureExtractionAggregator) o;

        if ( Double.compare( that.lightest, lightest ) != 0 )
            return false;
        if ( graphEntityPair != null ? !graphEntityPair.equals( that.graphEntityPair ) : that.graphEntityPair != null )
            return false;
        if ( linkingEntity != null ? !linkingEntity.equals( that.linkingEntity ) : that.linkingEntity != null )
            return false;
        return propertyTypeIdIndexedByFqn != null ?
                propertyTypeIdIndexedByFqn.equals( that.propertyTypeIdIndexedByFqn ) :
                that.propertyTypeIdIndexedByFqn == null;
    }

    @Override public int hashCode() {
        int result;
        long temp;
        result = graphEntityPair != null ? graphEntityPair.hashCode() : 0;
        result = 31 * result + ( linkingEntity != null ? linkingEntity.hashCode() : 0 );
        temp = Double.doubleToLongBits( lightest );
        result = 31 * result + (int) ( temp ^ ( temp >>> 32 ) );
        result = 31 * result + ( propertyTypeIdIndexedByFqn != null ? propertyTypeIdIndexedByFqn.hashCode() : 0 );
        return result;
    }
}
