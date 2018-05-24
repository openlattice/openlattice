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

package com.openlattice;

import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.hazelcast.serializers.*;
import com.openlattice.linking.HazelcastBlockingService;
import com.openlattice.linking.HazelcastMergingService;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

@Configuration
public class PlasmaCoupling {

    @Inject
    private ConductorElasticsearchApi elasticsearchApi;

    @Inject
    private ConductorElasticsearchCallStreamSerializer cecss;

    @Inject
    private FeatureExtractionAggregationStreamSerializer feass;

    @Inject
    private HazelcastBlockingService blockingService;

    @Inject
    private BlockingAggregatorStreamSerializer bass;

    @Inject
    private InitializerAggregatorStreamSerializer iass;

    @Inject
    private HazelcastMergingService mergingService;

    @Inject
    private MergeVertexAggregatorStreamSerializer mvass;

    @Inject
    private MergeEdgeAggregatorStreamSerializer meass;

    @PostConstruct
    public void connect() {
        cecss.setConductorElasticsearchApi( elasticsearchApi );
        feass.setConductorElasticsearchApi( elasticsearchApi );
        bass.setBlockingService( blockingService );
        iass.setBlockingService( blockingService );
        mvass.setMergingService( mergingService );
        meass.setMergingService( mergingService );
    }
}
