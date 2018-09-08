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

package com.openlattice.datastore.pods;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.auth0.Auth0TokenProvider;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.authorization.*;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DataGraphService;
import com.openlattice.data.EntityDatastore;
import com.openlattice.data.EntityKeyIdService;
import com.openlattice.data.ids.PostgresEntityKeyIdService;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.data.storage.HazelcastEntityDatastore;
import com.openlattice.data.storage.PostgresDataManager;
import com.openlattice.data.storage.PostgresEntityDataQueryService;
import com.openlattice.datastore.apps.services.AppService;
import com.openlattice.datastore.services.*;
import com.openlattice.directory.UserDirectoryService;
import com.openlattice.edm.PostgresEdmManager;
import com.openlattice.edm.properties.PostgresTypeManager;
import com.openlattice.edm.schemas.SchemaQueryService;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.edm.schemas.postgres.PostgresSchemaQueryService;
import com.openlattice.graph.Graph;
import com.openlattice.graph.GraphQueryService;
import com.openlattice.graph.PostgresGraphQueryService;
import com.openlattice.graph.core.GraphService;
import com.openlattice.ids.HazelcastIdGenerationService;
import com.openlattice.linking.HazelcastListingService;
import com.openlattice.linking.HazelcastVertexMergingService;
import com.openlattice.neuron.Neuron;
import com.openlattice.neuron.pods.NeuronPod;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.roles.HazelcastPrincipalService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.postgres.PostgresTableManager;
import com.openlattice.requests.HazelcastRequestsManager;
import com.openlattice.requests.RequestQueryService;
import com.zaxxer.hikari.HikariDataSource;
import org.jdbi.v3.core.Jdbi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import static com.openlattice.datastore.util.Util.returnAndLog;

@Configuration
@Import({
        Auth0Pod.class,
        CassandraPod.class,
        NeuronPod.class
})
public class DatastoreServicesPod {

    @Inject
    private Jdbi jdbi;
    @Inject
    private PostgresTableManager tableManager;
    @Inject
    private HazelcastInstance hazelcastInstance;
    @Inject
    private HikariDataSource hikariDataSource;
    @Inject
    private Auth0Configuration auth0Configuration;
    @Inject
    private ListeningExecutorService executor;
    @Inject
    private EventBus eventBus;
    @Inject
    private Neuron neuron;

    @Bean
    public PostgresUserApi pgUserApi() {
        return jdbi.onDemand(PostgresUserApi.class);
    }

    @Bean
    public ObjectMapper defaultObjectMapper() {
        ObjectMapper mapper = ObjectMappers.getJsonMapper();
        FullQualifiedNameJacksonSerializer.registerWithMapper(mapper);

        return mapper;
    }

    @Bean
    public AuthorizationQueryService authorizationQueryService() {
        return new AuthorizationQueryService(hikariDataSource, hazelcastInstance);
    }

    @Bean
    public GraphQueryService graphQueryService() {
        return new PostgresGraphQueryService(
                hikariDataSource,
                dataModelService(),
                authorizationManager(),
                defaultObjectMapper()
        );
    }

    @Bean
    public AuthorizationManager authorizationManager() {
        return new HazelcastAuthorizationService(hazelcastInstance, authorizationQueryService(), eventBus);
    }

    @Bean
    public AbstractSecurableObjectResolveTypeService securableObjectTypes() {
        return new HazelcastAbstractSecurableObjectResolveTypeService(hazelcastInstance);
    }

    @Bean
    public SchemaQueryService schemaQueryService() {
        return new PostgresSchemaQueryService(hikariDataSource);
    }

    @Bean
    public PostgresEntitySetManager entitySetManager() {
        return new PostgresEntitySetManager(hikariDataSource);
    }

    @Bean
    public HazelcastSchemaManager schemaManager() {
        return new HazelcastSchemaManager(
                hazelcastInstance,
                schemaQueryService());
    }

    @Bean
    public PostgresTypeManager entityTypeManager() {
        return new PostgresTypeManager(hikariDataSource);
    }

    @Bean
    public PostgresEntityDataQueryService dataQueryService() {
        return new PostgresEntityDataQueryService(hikariDataSource);
    }

    @Bean
    public EdmManager dataModelService() {
        return new EdmService(
                hikariDataSource,
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                entitySetManager(),
                entityTypeManager(),
                schemaManager());
    }

    @Bean
    public HazelcastAclKeyReservationService aclKeyReservationService() {
        return new HazelcastAclKeyReservationService(hazelcastInstance);
    }

    @Bean
    public HazelcastListingService hazelcastListingService() {
        return new HazelcastListingService(hazelcastInstance);
    }


    @Bean
    public ODataStorageService odataStorageService() {
        return new ODataStorageService(
                hazelcastInstance,
                dataModelService());
    }

    @Bean
    public PostgresDataManager postgresDataManager() {
        return new PostgresDataManager(hikariDataSource);
    }

    @Bean
    public EntityDatastore entityDatastore() {
        return new HazelcastEntityDatastore(
                hazelcastInstance,
                executor,
                defaultObjectMapper(),
                idService(),
                postgresDataManager(), dataQueryService());
    }

    @Bean
    public SecurePrincipalsManager principalService() {
        return new HazelcastPrincipalService(hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager());
    }

    @Bean
    public HazelcastOrganizationService organizationsManager() {
        return new HazelcastOrganizationService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                userDirectoryService(),
                principalService());
    }

    @Bean
    public UserDirectoryService userDirectoryService() {
        return new UserDirectoryService(auth0TokenProvider(), hazelcastInstance);
    }

    @Bean
    public SearchService searchService() {
        return new SearchService();
    }

    @Bean
    public EdmAuthorizationHelper edmAuthorizationHelper() {
        return new EdmAuthorizationHelper(dataModelService(), authorizationManager());
    }

    @Bean
    public SyncTicketService sts() {
        return new SyncTicketService(hazelcastInstance);
    }

    @Bean
    public RequestQueryService rqs() {
        return new RequestQueryService(hikariDataSource);
    }

    @Bean
    public HazelcastRequestsManager hazelcastRequestsManager() {
        return new HazelcastRequestsManager(hazelcastInstance, rqs(), neuron);
    }

    @Bean
    public AnalysisService analysisService() {
        return new AnalysisService();
    }

    @Bean
    public GraphService graphApi() {
        return new Graph(hikariDataSource, dataModelService());
    }

    @Bean
    public HazelcastIdGenerationService idGenerationService() {
        return new HazelcastIdGenerationService(hazelcastInstance);
    }

    @Bean
    public EntityKeyIdService idService() {
        return new PostgresEntityKeyIdService(hazelcastInstance, hikariDataSource, idGenerationService());
    }

    @Bean
    public DataGraphManager dataGraphService() {
        return new DataGraphService(
                hazelcastInstance,
                eventBus,
                graphApi(),
                idService(),
                entityDatastore(),
                dataModelService());
    }

    @Bean
    public DbCredentialService dcs() {
        return new DbCredentialService(hazelcastInstance, pgUserApi());
    }

    @Bean
    public HazelcastVertexMergingService vms() {
        return new HazelcastVertexMergingService(hazelcastInstance);
    }

    @Bean
    public AppService appService() {
        return returnAndLog(new AppService(hazelcastInstance,
                dataModelService(),
                organizationsManager(),
                authorizationQueryService(),
                authorizationManager(),
                principalService(),
                aclKeyReservationService()), "Checkpoint app service");
    }

    @Bean
    public PostgresEdmManager pgEdmManager() {
        PostgresEdmManager pgEdmManager = new PostgresEdmManager(tableManager, hikariDataSource);
        eventBus.register(pgEdmManager);
        return pgEdmManager;
    }

    @Bean
    public Auth0TokenProvider auth0TokenProvider() {
        return new Auth0TokenProvider(auth0Configuration);
    }

    @Bean
    public ConductorElasticsearchApi conductorElasticsearchApi() {
        return new DatastoreConductorElasticsearchApi(hazelcastInstance);
    }

    @PostConstruct
    void initPrincipals() {
        Principals.init(principalService());
    }
}
