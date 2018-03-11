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

import static com.openlattice.datastore.util.Util.returnAndLog;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.auth0.Auth0TokenProvider;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.authorization.AbstractSecurableObjectResolveTypeService;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizationQueryService;
import com.openlattice.authorization.DbCredentialService;
import com.openlattice.authorization.EdmAuthorizationHelper;
import com.openlattice.authorization.HazelcastAbstractSecurableObjectResolveTypeService;
import com.openlattice.authorization.HazelcastAclKeyReservationService;
import com.openlattice.authorization.HazelcastAuthorizationService;
import com.openlattice.authorization.PostgresUserApi;
import com.openlattice.authorization.Principals;
import com.openlattice.clustering.DistributedClusterer;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DataGraphService;
import com.openlattice.data.DatasourceManager;
import com.openlattice.data.ids.HazelcastEntityKeyIdService;
import com.openlattice.data.serializers.FullQualifedNameJacksonDeserializer;
import com.openlattice.data.serializers.FullQualifedNameJacksonSerializer;
import com.openlattice.data.storage.HazelcastEntityDatastore;
import com.openlattice.datastore.apps.services.AppService;
import com.openlattice.datastore.services.AnalysisService;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.LinkingService;
import com.openlattice.datastore.services.ODataStorageService;
import com.openlattice.datastore.services.PostgresEntitySetManager;
import com.openlattice.datastore.services.SearchService;
import com.openlattice.datastore.services.SyncTicketService;
import com.openlattice.directory.UserDirectoryService;
import com.openlattice.edm.properties.PostgresTypeManager;
import com.openlattice.edm.schemas.SchemaQueryService;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.edm.schemas.postgres.PostgresSchemaQueryService;
import com.openlattice.graph.core.Graph;
import com.openlattice.linking.HazelcastLinkingGraphs;
import com.openlattice.linking.HazelcastListingService;
import com.openlattice.linking.HazelcastVertexMergingService;
import com.openlattice.linking.components.Clusterer;
import com.openlattice.matching.DistributedMatcher;
import com.openlattice.merging.DistributedMerger;
import com.openlattice.neuron.Neuron;
import com.openlattice.neuron.pods.NeuronPod;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.roles.HazelcastPrincipalService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.requests.HazelcastRequestsManager;
import com.openlattice.requests.RequestQueryService;
import com.zaxxer.hikari.HikariDataSource;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import( {
        Auth0Pod.class,
        CassandraPod.class,
        NeuronPod.class
} )
public class DatastoreServicesPod {

    @Inject
    Jdbi jdbi;
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
        return jdbi.onDemand( PostgresUserApi.class );
    }

    @Bean
    public ObjectMapper defaultObjectMapper() {
        ObjectMapper mapper = ObjectMappers.getJsonMapper();
        FullQualifedNameJacksonSerializer.registerWithMapper( mapper );
        FullQualifedNameJacksonDeserializer.registerWithMapper( mapper );
        return mapper;
    }

    @Bean
    public AuthorizationQueryService authorizationQueryService() {
        return new AuthorizationQueryService( hikariDataSource, hazelcastInstance );
    }

    @Bean
    public AuthorizationManager authorizationManager() {
        return new HazelcastAuthorizationService( hazelcastInstance, authorizationQueryService(), eventBus );
    }

    @Bean
    public AbstractSecurableObjectResolveTypeService securableObjectTypes() {
        return new HazelcastAbstractSecurableObjectResolveTypeService( hazelcastInstance );
    }

    @Bean
    public SchemaQueryService schemaQueryService() {
        return new PostgresSchemaQueryService( hikariDataSource );
    }

    @Bean
    public PostgresEntitySetManager entitySetManager() {
        return new PostgresEntitySetManager( hikariDataSource );
    }

    @Bean
    public HazelcastSchemaManager schemaManager() {
        return new HazelcastSchemaManager(
                hazelcastInstance,
                schemaQueryService() );
    }

    @Bean
    public PostgresTypeManager entityTypeManager() {
        return new PostgresTypeManager( hikariDataSource );
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
                schemaManager(),
                datasourceManager() );
    }

    @Bean
    public HazelcastAclKeyReservationService aclKeyReservationService() {
        return new HazelcastAclKeyReservationService( hazelcastInstance );
    }

    @Bean
    public HazelcastListingService hazelcastListingService() {
        return new HazelcastListingService( hazelcastInstance );
    }

    @Bean
    public HazelcastLinkingGraphs linkingGraph() {
        return new HazelcastLinkingGraphs( hazelcastInstance );
    }

    @Bean
    public ODataStorageService odataStorageService() {
        return new ODataStorageService(
                hazelcastInstance,
                dataModelService() );
    }

    @Bean
    public HazelcastEntityDatastore cassandraDataManager() {
        return new HazelcastEntityDatastore(
                hazelcastInstance,
                executor,
                defaultObjectMapper(),
                idService(),
                datasourceManager() );
    }

    @Bean
    public SecurePrincipalsManager principalService() {
        return new HazelcastPrincipalService( hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager() );
    }

    @Bean
    public HazelcastOrganizationService organizationsManager() {
        return new HazelcastOrganizationService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                userDirectoryService(),
                principalService() );
    }

    @Bean
    public DatasourceManager datasourceManager() {
        return new DatasourceManager( hikariDataSource, hazelcastInstance );
    }

    @Bean
    public UserDirectoryService userDirectoryService() {
        return new UserDirectoryService( auth0TokenProvider(), hazelcastInstance );
    }

    @Bean
    public SearchService searchService() {
        return new SearchService();
    }

    @Bean
    public EdmAuthorizationHelper edmAuthorizationHelper() {
        return new EdmAuthorizationHelper( dataModelService(), authorizationManager() );
    }

    @Bean
    public SyncTicketService sts() {
        return new SyncTicketService( hazelcastInstance );
    }

    @Bean
    public RequestQueryService rqs() {
        return new RequestQueryService( hikariDataSource );
    }

    @Bean
    public HazelcastRequestsManager hazelcastRequestsManager() {
        return new HazelcastRequestsManager( hazelcastInstance, rqs(), neuron );
    }

    @Bean
    public Clusterer clusterer() {
        return new DistributedClusterer( hazelcastInstance );
    }

    @Bean
    public DistributedMatcher matcher() {
        return new DistributedMatcher( hazelcastInstance, dataModelService() );
    }

    @Bean
    public DistributedMerger merger() {
        return new DistributedMerger( hazelcastInstance,
                hazelcastListingService(),
                dataModelService(),
                datasourceManager() );
    }

    @Bean
    public LinkingService linkingService() {
        return returnAndLog( new LinkingService(
                linkingGraph(),
                matcher(),
                clusterer(),
                merger(),
                eventBus,
                dataModelService(),
                datasourceManager() ), "Checkpoint linking service" );
    }

    @Bean
    public AnalysisService analysisService() {
        return new AnalysisService();
    }

    @Bean
    public Graph loomGraph() {
        return new Graph( executor, hazelcastInstance );
    }

    @Bean
    public HazelcastEntityKeyIdService idService() {
        return new HazelcastEntityKeyIdService( hazelcastInstance, executor );
    }

    @Bean
    public DataGraphManager dataGraphService() {
        return new DataGraphService(
                hazelcastInstance,
                cassandraDataManager(),
                loomGraph(),
                idService(),
                executor,
                eventBus );
    }

    @Bean
    public DbCredentialService dcs() {
        return new DbCredentialService( hazelcastInstance, pgUserApi() );
    }

    @Bean
    public HazelcastVertexMergingService vms() {
        return new HazelcastVertexMergingService( hazelcastInstance );
    }

    @Bean
    public AppService appService() {
        return returnAndLog( new AppService( hazelcastInstance,
                dataModelService(),
                organizationsManager(),
                authorizationQueryService(),
                authorizationManager(),
                principalService(),
                aclKeyReservationService() ), "Checkpoint app service" );
    }

    @Bean
    public Auth0TokenProvider auth0TokenProvider() {
        return new Auth0TokenProvider( auth0Configuration );
    }

    @PostConstruct
    void initPrincipals() {
        Principals.init( principalService() );
    }
}
