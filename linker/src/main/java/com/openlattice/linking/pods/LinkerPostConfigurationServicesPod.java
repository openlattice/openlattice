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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.geekbeast.hazelcast.HazelcastClientProvider;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.data.EntityKeyIdService;
import com.openlattice.data.ids.PostgresEntityKeyIdService;
import com.openlattice.data.storage.ByteBlobDataManager;
import com.openlattice.data.storage.EntityDatastore;
import com.openlattice.data.storage.IndexingMetadataManager;
import com.openlattice.data.storage.PostgresEntityDataQueryService;
import com.openlattice.data.storage.PostgresEntityDatastore;
import com.openlattice.data.storage.partitions.PartitionManager;
import com.openlattice.datastore.pods.ByteBlobServicePod;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.edm.PostgresEdmManager;
import com.openlattice.ids.HazelcastIdGenerationService;
import com.openlattice.linking.BackgroundLinkingService;
import com.openlattice.linking.Blocker;
import com.openlattice.linking.DataLoader;
import com.openlattice.linking.EdmCachingDataLoader;
import com.openlattice.linking.LinkingConfiguration;
import com.openlattice.linking.LinkingLogService;
import com.openlattice.linking.LinkingQueryService;
import com.openlattice.linking.Matcher;
import com.openlattice.linking.PostgresLinkingFeedbackService;
import com.openlattice.linking.PostgresLinkingLogService;
import com.openlattice.linking.blocking.ElasticsearchBlocker;
import com.openlattice.linking.controllers.RealtimeLinkingController;
import com.openlattice.linking.graph.PostgresLinkingQueryService;
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
    private PostgresEdmManager pgEdmManager;

    @Inject
    private PartitionManager partitionManager;

    @Inject
    private LinkingLogService linkingLogService;

    @Inject
    private HazelcastClientProvider hazelcastClientProvider;

    @Inject
    private ObjectMapper defaultObjectMapper;

    @Bean
    public HazelcastIdGenerationService idGeneration() {
        return new HazelcastIdGenerationService( hazelcastClientProvider );
    }

    @Bean
    public EntityKeyIdService idService() {
        return new PostgresEntityKeyIdService( hazelcastClientProvider,
                executor,
                hikariDataSource,
                idGeneration(),
                partitionManager );
    }

    @Bean
    public PostgresEntityDataQueryService dataQueryService() {
        return new PostgresEntityDataQueryService(
                hikariDataSource,
                byteBlobDataManager,
                partitionManager,
                indexingMetadataManager()
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
                postgresLinkingFeedbackQueryService(),
                hazelcastInstance );
    }

    @Bean
    public LinkingQueryService lqs() {
        return new PostgresLinkingQueryService( hikariDataSource, partitionManager );
    }

    @Bean
    public EntityDatastore entityDatastore() {
        return new PostgresEntityDatastore( dataQueryService(), edm, pgEdmManager );
    }

    @Bean
    public IndexingMetadataManager indexingMetadataManager() {
        return new IndexingMetadataManager( hikariDataSource, partitionManager );
    }

    @Bean
    public BackgroundLinkingService linkingService() throws IOException {
        return new BackgroundLinkingService( executor,
                hazelcastInstance,
                pgEdmManager,
                blocker(),
                matcher,
                idService(),
                dataLoader(),
                lqs(),
                postgresLinkingFeedbackQueryService(),
                edm.getEntityTypeUuids( linkingConfiguration.getEntityTypes() ),
                linkingLogService,
                linkingConfiguration );
    }

    @Bean
    public LinkingLogService linkingLogService() {
        return new PostgresLinkingLogService( hikariDataSource, defaultObjectMapper );
    }

    @Bean
    public RealtimeLinkingController realtimeLinkingController() {
        return new RealtimeLinkingController( linkingConfiguration, edm );
    }

    @Bean
    public PostgresLinkingFeedbackService postgresLinkingFeedbackQueryService() {
        return new PostgresLinkingFeedbackService( hikariDataSource, hazelcastInstance );
    }
}
