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

package com.openlattice.linking.pods;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.geekbeast.hazelcast.HazelcastClientProvider;
import com.geekbeast.rhizome.jobs.HazelcastJobService;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DataGraphService;
import com.openlattice.data.EntityKeyIdService;
import com.openlattice.data.ids.PostgresEntityKeyIdService;
import com.openlattice.data.storage.ByteBlobDataManager;
import com.openlattice.data.storage.DataSourceResolver;
import com.openlattice.data.storage.EntityDatastore;
import com.openlattice.data.storage.IndexingMetadataManager;
import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService;
import com.openlattice.data.storage.postgres.PostgresEntityDatastore;
import com.openlattice.datastore.pods.ByteBlobServicePod;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.EntitySetManager;
import com.openlattice.graph.Graph;
import com.openlattice.graph.core.GraphService;
import com.openlattice.ids.HazelcastIdGenerationService;
import com.openlattice.jdbc.DataSourceManager;
import com.openlattice.linking.BackgroundLinkingService;
import com.openlattice.linking.DataLoader;
import com.openlattice.linking.EdmCachingDataLoader;
import com.openlattice.linking.LinkingConfiguration;
import com.openlattice.linking.LinkingQueryService;
import com.openlattice.linking.PostgresLinkingFeedbackService;
import com.openlattice.linking.blocking.Blocker;
import com.openlattice.linking.blocking.ElasticsearchBlocker;
import com.openlattice.linking.clustering.Clusterer;
import com.openlattice.linking.clustering.PostgresClusterer;
import com.openlattice.linking.controllers.RealtimeLinkingController;
import com.openlattice.linking.graph.PostgresLinkingQueryService;
import com.openlattice.linking.matching.Matcher;
import com.openlattice.postgres.PostgresTable;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.inject.Inject;
import java.io.IOException;

@Configuration
@Import( { ByteBlobServicePod.class } )
public class LinkerPostConfigurationServicesPod {

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private LinkingConfiguration linkingConfiguration;

    @Inject
    private HikariDataSource hikariDataSource;

    @Inject
    private EdmManager edm;

    @Inject
    private ListeningExecutorService executor;

    @Inject
    private Matcher matcher;

    @Inject
    private ByteBlobDataManager byteBlobDataManager;

    @Inject
    private AuthorizationManager authz;

    @Inject
    private ConductorElasticsearchApi elasticsearchApi;

    @Inject
    private EntitySetManager entitySetManager;

    @Inject
    private HazelcastClientProvider hazelcastClientProvider;

    @Inject
    private ObjectMapper defaultObjectMapper;

    @Inject
    private EventBus eventBus;

    @Inject
    private MetricRegistry metricRegistry;

    @Inject
    private HealthCheckRegistry healthCheckRegistry;

    @Inject
    private PostgresLinkingFeedbackService postgresLinkingFeedbackQueryService;

    @Inject
    private DataSourceManager dataSourceManager;

    @Bean
    public HazelcastIdGenerationService idGeneration() {
        return new HazelcastIdGenerationService( hazelcastClientProvider );
    }

    @Bean
    public EntityKeyIdService idService() {
        return new PostgresEntityKeyIdService(
                dataSourceResolver(),
                idGeneration() );
    }

    @Bean
    public DataSourceResolver dataSourceResolver() {
        dataSourceManager.registerTablesWithAllDatasources( PostgresTable.E );
        dataSourceManager.registerTablesWithAllDatasources( PostgresTable.DATA );
        dataSourceManager.registerTablesWithAllDatasources( PostgresTable.IDS );
        dataSourceManager.registerTablesWithAllDatasources( PostgresTable.SYNC_IDS );
        return new DataSourceResolver( hazelcastInstance, dataSourceManager );
    }

    @Bean
    public PostgresEntityDataQueryService dataQueryService() {

        //TODO: fix it to use read replica
        return new PostgresEntityDataQueryService(
                dataSourceResolver(),
                byteBlobDataManager
        );
    }

    @Bean
    public DataLoader dataLoader() {
        return new EdmCachingDataLoader( dataQueryService(), hazelcastInstance );
    }

    @Bean
    public Blocker blocker() throws IOException {
        return new ElasticsearchBlocker(
                elasticsearchApi,
                dataLoader(),
                postgresLinkingFeedbackQueryService,
                hazelcastInstance );
    }

    @Bean
    public LinkingQueryService lqs() {
        return new PostgresLinkingQueryService( hikariDataSource );
    }

    @Bean
    public EntityDatastore entityDatastore() {
        return new PostgresEntityDatastore(
                dataQueryService(),
                edm,
                entitySetManager,
                metricRegistry,
                eventBus,
                postgresLinkingFeedbackQueryService,
                lqs()
        );
    }

    @Bean
    public GraphService graphService() {
        return new Graph( dataSourceResolver(),
                entitySetManager,
                dataQueryService(),
                idService(),
                metricRegistry );
    }

    @Bean
    public HazelcastJobService jobService() {
        return new HazelcastJobService( hazelcastInstance );
    }

    @Bean
    public DataGraphManager dgm() {
        return new DataGraphService(graphService(), idService(), entityDatastore(), jobService() );
    }

    @Bean
    public IndexingMetadataManager indexingMetadataManager() {
        return new IndexingMetadataManager( dataSourceResolver() );
    }

    @Bean
    public BackgroundLinkingService linkingService() throws IOException {
        return new BackgroundLinkingService( executor,
                hazelcastInstance,
                blocker(),
                matcher,
                idService(),
                clusterer(),
                lqs(),
                postgresLinkingFeedbackQueryService,
                edm.getEntityTypeUuids( linkingConfiguration.getEntityTypes() ),
                linkingConfiguration );
    }

    @Bean
    public Clusterer clusterer() {
        return new PostgresClusterer(
                dataLoader(),
                matcher
        );
    }

    @Bean
    public RealtimeLinkingController realtimeLinkingController() {
        return new RealtimeLinkingController( linkingConfiguration, edm );
    }
}
