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

package com.openlattice.pods;

import com.codahale.metrics.MetricRegistry;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.EdmAuthorizationHelper;
import com.openlattice.conductor.rpc.ConductorConfiguration;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.conductor.rpc.MapboxConfiguration;
import com.openlattice.data.storage.EntityDatastore;
import com.openlattice.data.storage.IndexingMetadataManager;
import com.openlattice.data.storage.partitions.PartitionManager;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.EntitySetManager;
import com.openlattice.graph.core.GraphService;
import com.openlattice.mail.MailServiceClient;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.scrunchie.search.ConductorElasticsearchImpl;
import com.openlattice.search.PersistentSearchMessengerTask;
import com.openlattice.search.PersistentSearchMessengerTaskDependencies;
import com.openlattice.search.SearchService;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.inject.Inject;

@Configuration
public class ConductorPostInitializationPod {

    @Inject
    private EventBus eventBus;

    @Inject
    private MetricRegistry metricRegistry;

    @Inject
    private ConductorConfiguration conductorConfiguration;

    @Inject
    private MapboxConfiguration mapboxConfiguration;

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private HikariDataSource hikariDataSource;

    @Inject
    private SecurePrincipalsManager principalService;

    @Inject
    private AuthorizationManager authorizationManager;

    @Inject
    private EntitySetManager entitySetManager;

    @Inject
    private EdmManager edmManager;

    @Inject
    private EdmAuthorizationHelper edmAuthorizationHelper;

    @Inject
    private MailServiceClient mailServiceClient;

    @Inject
    private PartitionManager partitionManager;

    @Inject
    private IndexingMetadataManager indexingMetadataManager;

    @Inject
    private GraphService graphService;

    @Inject
    private EntityDatastore entityDatastore;

    @Bean
    public ConductorElasticsearchApi elasticsearchApi() {
        return new ConductorElasticsearchImpl( conductorConfiguration.getSearchConfiguration() );
    }

    @Bean
    public PersistentSearchMessengerTaskDependencies persistentSearchMessengerTaskDependencies() {
        return new PersistentSearchMessengerTaskDependencies(
                hazelcastInstance,
                hikariDataSource,
                principalService,
                authorizationManager,
                edmAuthorizationHelper,
                searchService(),
                mailServiceClient,
                mapboxConfiguration.getMapboxToken()
        );
    }

    @Bean
    public PersistentSearchMessengerTask persistentSearchMessengerTask() {
        return new PersistentSearchMessengerTask();
    }

    @Bean
    public SearchService searchService() {
        return new SearchService(
                eventBus,
                metricRegistry,
                authorizationManager,
                elasticsearchApi(),
                edmManager,
                entitySetManager,
                graphService,
                entityDatastore,
                indexingMetadataManager
        );
    }
}
