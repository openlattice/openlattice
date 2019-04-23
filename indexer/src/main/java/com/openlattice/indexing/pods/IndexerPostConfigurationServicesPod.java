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
import com.openlattice.assembler.Assembler;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.conductor.rpc.ConductorConfiguration;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.data.EntityDatastore;
import com.openlattice.data.EntityKeyIdService;
import com.openlattice.data.ids.PostgresEntityKeyIdService;
import com.openlattice.data.storage.ByteBlobDataManager;
import com.openlattice.data.storage.HazelcastEntityDatastore;
import com.openlattice.data.storage.IndexingMetadataManager;
import com.openlattice.data.storage.PostgresEntityDataQueryService;
import com.openlattice.datastore.pods.ByteBlobServicePod;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.ids.HazelcastIdGenerationService;
import com.openlattice.indexing.BackgroundIndexingService;
import com.openlattice.indexing.BackgroundLinkingIndexingService;
import com.openlattice.indexing.IndexingService;
import com.openlattice.indexing.configuration.IndexerConfiguration;
import com.openlattice.linking.LinkingQueryService;
import com.openlattice.linking.PostgresLinkingFeedbackService;
import com.openlattice.linking.graph.PostgresLinkingQueryService;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import javax.inject.Inject;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import( { ByteBlobServicePod.class } )
public class IndexerPostConfigurationServicesPod {

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private IndexerConfiguration indexerConfiguration;

    @Inject
    private HikariDataSource hikariDataSource;

    @Inject
    private EdmManager edm;

    @Inject
    private ListeningExecutorService executor;

    @Inject
    private ByteBlobDataManager byteBlobDataManager;

    @Inject
    private AuthorizationManager authz;

    @Inject
    private ConductorElasticsearchApi elasticsearchApi;

    @Inject
    private Assembler assembler;

    @Bean
    public HazelcastIdGenerationService idGeneration() {
        return new HazelcastIdGenerationService( hazelcastInstance );
    }

    @Bean
    public EntityKeyIdService idService() {
        return new PostgresEntityKeyIdService( hazelcastInstance, executor, hikariDataSource, idGeneration() );
    }

    @Bean
    public PostgresEntityDataQueryService dataQueryService() {
        return new PostgresEntityDataQueryService( hikariDataSource, byteBlobDataManager );
    }

    @Bean
    public LinkingQueryService lqs() {
        return new PostgresLinkingQueryService( hikariDataSource );
    }

    @Bean
    public PostgresLinkingFeedbackService postgresLinkingFeedbackService() {
        return new PostgresLinkingFeedbackService( hikariDataSource, hazelcastInstance );
    }

    @Bean
    public EntityDatastore entityDatastore() {
        return new HazelcastEntityDatastore( idService(),
                indexingMetadataManager(),
                dataQueryService(),
                edm,
                assembler );
    }

    @Bean
    public IndexingMetadataManager indexingMetadataManager() {
        return new IndexingMetadataManager( hikariDataSource );
    }

    @Bean
    public BackgroundIndexingService backgroundIndexingService() {
        return new BackgroundIndexingService(
                hazelcastInstance,
                indexerConfiguration,
                hikariDataSource,
                dataQueryService(),
                elasticsearchApi,
                indexingMetadataManager() );
    }

    @Bean
    public BackgroundLinkingIndexingService backgroundLinkingIndexingService() {
        return new BackgroundLinkingIndexingService( hikariDataSource,
                entityDatastore(),
                elasticsearchApi,
                indexingMetadataManager(),
                indexerConfiguration,
                hazelcastInstance );
    }

    @Bean
    public IndexingService indexingService() {
        return new IndexingService(hikariDataSource, backgroundIndexingService(), executor, hazelcastInstance );
    }
}
