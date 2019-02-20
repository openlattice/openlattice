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
import com.openlattice.ResourceConfigurationLoader;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.conductor.rpc.ConductorConfiguration;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.data.EntityKeyIdService;
import com.openlattice.data.ids.PostgresEntityKeyIdService;
import com.openlattice.data.storage.ByteBlobDataManager;
import com.openlattice.data.storage.PostgresEntityDataQueryService;
import com.openlattice.datastore.pods.ByteBlobServicePod;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.ids.HazelcastIdGenerationService;
import com.openlattice.linking.*;
import com.openlattice.linking.Blocker;
import com.openlattice.linking.DataLoader;
import com.openlattice.linking.EdmCachingDataLoader;
import com.openlattice.linking.LinkingConfiguration;
import com.openlattice.linking.LinkingQueryService;
import com.openlattice.linking.Matcher;
import com.openlattice.linking.RealtimeLinkingService;
import com.openlattice.linking.blocking.ElasticsearchBlocker;
import com.openlattice.linking.controllers.RealtimeLinkingController;
import com.openlattice.linking.graph.PostgresLinkingQueryService;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import javax.inject.Inject;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import( { ByteBlobServicePod.class } )
public class LinkerPostConfigurationServicesPod {

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private ConductorConfiguration conductorConfiguration;

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

    @Bean
    public HazelcastIdGenerationService idGeneration() {
        return new HazelcastIdGenerationService( hazelcastInstance );
    }

    @Bean
    public EntityKeyIdService idService() {
        return new PostgresEntityKeyIdService( hazelcastInstance, hikariDataSource, idGeneration() );
    }

    @Bean
    public PostgresEntityDataQueryService dataQueryService() {
        return new PostgresEntityDataQueryService( hikariDataSource, byteBlobDataManager );
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
        return new PostgresLinkingQueryService( hikariDataSource );
    }

    @Bean
    public LinkingConfiguration linkingConfiguration() {
        return ResourceConfigurationLoader.loadConfiguration( LinkingConfiguration.class );
    }

    @Bean
    public RealtimeLinkingService linkingService() throws IOException {
        var lc = linkingConfiguration();
        return new RealtimeLinkingService( hazelcastInstance,
                blocker(),
                matcher,
                idService(),
                dataLoader(),
                lqs(),
                executor,
                postgresLinkingFeedbackQueryService(),
                edm.getEntityTypeUuids( lc.getEntityTypes() ),
                lc.getBlacklist(),
                lc.getWhitelist(),
                lc.getBlockSize() );
    }

    @Bean
    public RealtimeLinkingController realtimeLinkingController() {
        var lc = linkingConfiguration();
        return new RealtimeLinkingController( lc, edm );
    }

    @Bean
    public PostgresLinkingFeedbackService postgresLinkingFeedbackQueryService() {
        return new PostgresLinkingFeedbackService( hikariDataSource, hazelcastInstance );
    }
}
