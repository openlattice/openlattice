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

package com.openlattice.linking.aggregators;

import com.openlattice.linking.LinkingEdge;
import com.openlattice.linking.WeightedLinkingEdge;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.hazelcast.aggregation.Aggregator;
import com.openlattice.linking.LinkingEdge;
import com.openlattice.linking.WeightedLinkingEdge;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class EdgeLoadingAggregator extends Aggregator<Entry<LinkingEdge, Double>, Long> {
    private static final LoadingCache<UUID, PriorityBlockingQueue<WeightedLinkingEdge>> pqs    =
            CacheBuilder
                    .newBuilder()
                    .expireAfterAccess( 5, TimeUnit.MINUTES )
                    .build( new CacheLoader<UUID, PriorityBlockingQueue<WeightedLinkingEdge>>() {
                        @Override public PriorityBlockingQueue<WeightedLinkingEdge> load( UUID key ) throws Exception {
                            return new PriorityBlockingQueue<>();
                        }
                    } );
    private static final Logger                                                         logger = LoggerFactory
            .getLogger( LightestEdgeAggregator.class );

    private long count = 0;

    @Override
    public void accumulate( Entry<LinkingEdge, Double> input ) {
        PriorityBlockingQueue<WeightedLinkingEdge> pq = pqs.getUnchecked( input.getKey().getGraphId() );
        pq.add( new WeightedLinkingEdge( input.getValue().doubleValue(), input.getKey() ) );

    }

    @Override
    public void combine( Aggregator aggregator ) {

    }

    @Override public Long aggregate() {
        return null;
    }
}
