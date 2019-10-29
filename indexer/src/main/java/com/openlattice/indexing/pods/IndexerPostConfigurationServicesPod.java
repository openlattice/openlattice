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

package com.openlattice.indexing.pods;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.openlattice.BackgroundExternalDatabaseSyncingService;
import com.openlattice.auditing.AuditingManager;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.storage.EntityDatastore;
import com.openlattice.data.storage.IndexingMetadataManager;
import com.openlattice.data.storage.PostgresEntityDataQueryService;
import com.openlattice.data.storage.partitions.PartitionManager;
import com.openlattice.indexing.BackgroundExpiredDataDeletionService;
import com.openlattice.indexing.BackgroundIndexingService;
import com.openlattice.indexing.BackgroundLinkingIndexingService;
import com.openlattice.indexing.IndexingService;
import com.openlattice.indexing.configuration.IndexerConfiguration;
import com.openlattice.linking.LinkingQueryService;
import com.openlattice.linking.PostgresLinkingFeedbackService;
import com.openlattice.linking.graph.PostgresLinkingQueryService;
import com.openlattice.organizations.ExternalDatabaseManagementService;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.inject.Inject;

@Configuration
public class IndexerPostConfigurationServicesPod {

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private IndexerConfiguration indexerConfiguration;

    @Inject
    private HikariDataSource hikariDataSource;

    @Inject
    private ListeningExecutorService executor;

    @Inject
    private ConductorElasticsearchApi elasticsearchApi;

    @Inject
    private AuditingManager auditingManager;

    @Inject
    private DataGraphManager dataGraphService;

    @Inject
    private PostgresEntityDataQueryService dataQueryService;

    @Inject
    private EntityDatastore entityDatastore;

    @Inject
    private ExternalDatabaseManagementService edms;

    @Bean
    public PartitionManager partitionManager() {
        return new PartitionManager( hazelcastInstance, hikariDataSource );
    }

    @Bean
    public LinkingQueryService lqs() {
        return new PostgresLinkingQueryService( hikariDataSource, partitionManager() );
    }

    @Bean
    public PostgresLinkingFeedbackService postgresLinkingFeedbackService() {
        return new PostgresLinkingFeedbackService( hikariDataSource, hazelcastInstance );
    }

    @Bean
    public IndexingMetadataManager indexingMetadataManager() {
        return new IndexingMetadataManager( hikariDataSource, partitionManager() );
    }

    @Bean
    public BackgroundIndexingService backgroundIndexingService() {
        return new BackgroundIndexingService(
                hazelcastInstance,
                indexerConfiguration,
                hikariDataSource,
                dataQueryService,
                elasticsearchApi,
                indexingMetadataManager() );
    }

    @Bean
    public BackgroundLinkingIndexingService backgroundLinkingIndexingService() {
        return new BackgroundLinkingIndexingService(
                hazelcastInstance,
                executor,
                hikariDataSource,
                elasticsearchApi,
                indexingMetadataManager(),
                entityDatastore,
                indexerConfiguration );
    }

    @Bean
    public BackgroundExpiredDataDeletionService backgroundExpiredDataDeletionService() {
        return new BackgroundExpiredDataDeletionService(
                hazelcastInstance,
                indexerConfiguration,
                auditingManager,
                dataGraphService );
    }

    @Bean
    public BackgroundExternalDatabaseSyncingService backgroundExternalDatabaseUpdatingService() {
        return new BackgroundExternalDatabaseSyncingService(
                hazelcastInstance,
                edms,
                indexerConfiguration);
    }

    @Bean
    public IndexingService indexingService() {
        return new IndexingService( hikariDataSource,
                backgroundIndexingService(),
                partitionManager(),
                executor,
                hazelcastInstance );
    }
}
