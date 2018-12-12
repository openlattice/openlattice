

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

package com.openlattice.neuron.pods;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizationQueryService;
import com.openlattice.authorization.HazelcastAclKeyReservationService;
import com.openlattice.authorization.HazelcastAuthorizationService;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DataGraphService;
import com.openlattice.data.EntityKeyIdService;
import com.openlattice.data.ids.PostgresEntityKeyIdService;
import com.openlattice.data.storage.ByteBlobDataManager;
import com.openlattice.data.storage.HazelcastEntityDatastore;
import com.openlattice.data.storage.PostgresDataManager;
import com.openlattice.data.storage.PostgresEntityDataQueryService;
import com.openlattice.datastore.pods.ByteBlobServicePod;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.edm.PostgresEdmManager;
import com.openlattice.edm.properties.PostgresTypeManager;
import com.openlattice.edm.schemas.SchemaQueryService;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.edm.schemas.postgres.PostgresSchemaQueryService;
import com.openlattice.graph.Graph;
import com.openlattice.graph.core.GraphService;
import com.openlattice.ids.HazelcastIdGenerationService;
import com.openlattice.neuron.Neuron;
import com.openlattice.postgres.PostgresTableManager;
import com.zaxxer.hikari.HikariDataSource;
import javax.inject.Inject;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import( {
        AuditEntitySetPod.class,
        ByteBlobServicePod.class
} )
public class NeuronPod {

    @Inject
    private EventBus eventBus;

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private ListeningExecutorService executor;

    @Inject
    private PostgresTableManager tableManager;

    @Inject
    private HikariDataSource hikariDataSource;

    @Inject
    private ByteBlobDataManager byteBlobDataManager;

    /*
     *
     * Neuron bean
     *
     */

    @Bean
    public Neuron neuron() {
        return new Neuron( dataGraphService(), idService(), hikariDataSource );
    }

    /*
     *
     * other dependency beans
     *
     */

    @Bean
    public AuthorizationQueryService authorizationQueryService() {
        return new AuthorizationQueryService( hikariDataSource, hazelcastInstance );
    }

    @Bean
    public AuthorizationManager authorizationManager() {
        return new HazelcastAuthorizationService( hazelcastInstance, authorizationQueryService(), eventBus );
    }

    @Bean
    public PostgresDataManager postgresDataManager() {
        return new PostgresDataManager( hikariDataSource );
    }

    @Bean
    public PostgresEntityDataQueryService dataQueryService() {
        return new PostgresEntityDataQueryService( hikariDataSource, byteBlobDataManager );
    }

    @Bean
    public HazelcastEntityDatastore entityDatastore() {
        return new HazelcastEntityDatastore(
                hazelcastInstance,
                executor,
                defaultObjectMapper(),
                idService(),
                postgresDataManager(),
                dataQueryService()
        );
    }

    @Bean
    public PostgresEdmManager edmManager() {
        return new PostgresEdmManager( hikariDataSource, tableManager );
    }

    @Bean
    public PostgresTypeManager entityTypeManager() {
        return new PostgresTypeManager( hikariDataSource );
    }

    @Bean
    public DataGraphManager dataGraphService() {
        return new DataGraphService(
                hazelcastInstance,
                eventBus,
                graphApi(),
                idService(),
                entityDatastore(),
                dataModelService()
        );
    }

    @Bean
    public EdmManager dataModelService() {
        return new EdmService(
                hikariDataSource,
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                edmManager(),
                entityTypeManager(),
                schemaManager() );
    }

    @Bean
    public EntityKeyIdService idService() {
        return new PostgresEntityKeyIdService( hazelcastInstance, hikariDataSource, idGenerationService() );
    }

    @Bean
    public HazelcastIdGenerationService idGenerationService() {
        return new HazelcastIdGenerationService( hazelcastInstance );
    }

    @Bean
    public HazelcastAclKeyReservationService aclKeyReservationService() {
        return new HazelcastAclKeyReservationService( hazelcastInstance );
    }

    @Bean
    public HazelcastSchemaManager schemaManager() {
        return new HazelcastSchemaManager(
                hazelcastInstance,
                schemaQueryService() );
    }

    @Bean
    public GraphService graphApi() {
        return new Graph( hikariDataSource, dataModelService() );
    }

    @Bean
    public ObjectMapper defaultObjectMapper() {
        return ObjectMappers.getJsonMapper();
    }

    @Bean
    public SchemaQueryService schemaQueryService() {
        return new PostgresSchemaQueryService( hikariDataSource );
    }
}
