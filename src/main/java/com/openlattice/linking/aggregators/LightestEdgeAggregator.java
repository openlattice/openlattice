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

import com.openlattice.linking.*;
import com.hazelcast.aggregation.Aggregator;
import com.openlattice.linking.LinkingEdge;
import com.openlattice.linking.LinkingVertexKey;
import com.openlattice.linking.WeightedLinkingEdge;
import com.openlattice.linking.WeightedLinkingVertexKey;
import com.openlattice.linking.WeightedLinkingVertexKeySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map.Entry;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class LightestEdgeAggregator
        extends Aggregator<Entry<LinkingVertexKey, WeightedLinkingVertexKeySet>, WeightedLinkingEdge> {
    private static final Logger logger = LoggerFactory.getLogger( LightestEdgeAggregator.class );

    private WeightedLinkingEdge lightest = null;

    public LightestEdgeAggregator( WeightedLinkingEdge lightest ) {
        this.lightest = lightest;
    }

    public LightestEdgeAggregator() {
    }

    @Override
    public void accumulate( Entry<LinkingVertexKey, WeightedLinkingVertexKeySet> input ) {
        if (input.getValue() != null && input.getValue().size() != 0) {
            WeightedLinkingVertexKey wlvk = input.getValue().first();
            LinkingEdge le = new LinkingEdge( input.getKey(), wlvk.getVertexKey() );
            WeightedLinkingEdge wle = new WeightedLinkingEdge( wlvk.getWeight(), le );
            double weight = wle.getWeight();
            if ( lightest == null || weight < lightest.getWeight() ) {
                lightest = wle;
            }
        }
    }

    @Override
    public void combine( Aggregator aggregator ) {
        if ( aggregator instanceof LightestEdgeAggregator ) {
            LightestEdgeAggregator other = (LightestEdgeAggregator) aggregator;
            if ( lightest == null || ( other.lightest != null && other.lightest.getWeight() < lightest.getWeight() ) ) {
                lightest = other.lightest;
            }
        } else {
            logger.error( "Incompatible aggregator for lightest edge" );
        }
    }

    @Override
    public WeightedLinkingEdge aggregate() {
        return lightest;
    }

}
