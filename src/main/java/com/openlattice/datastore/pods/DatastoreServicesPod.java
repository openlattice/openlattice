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

import com.amazonaws.services.s3.AmazonS3;
import com.codahale.metrics.MetricRegistry;
import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.rhizome.configuration.amazon.AmazonLaunchConfiguration;
import com.openlattice.analysis.AnalysisService;
import com.openlattice.assembler.Assembler;
import com.openlattice.assembler.AssemblerConfiguration;
import com.openlattice.assembler.AssemblerConnectionManager;
import com.openlattice.assembler.AssemblerDependencies;
import com.openlattice.assembler.pods.AssemblerConfigurationPod;
import com.openlattice.assembler.tasks.UserCredentialSyncTask;
import com.openlattice.auditing.AuditRecordEntitySetsManager;
import com.openlattice.auditing.AuditingConfiguration;
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
import com.openlattice.data.storage.*;
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
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.ids.HazelcastIdGenerationService;
import com.openlattice.linking.LinkingQueryService;
import com.openlattice.linking.PostgresLinkingFeedbackService;
import com.openlattice.linking.graph.PostgresLinkingQueryService;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.roles.HazelcastPrincipalService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.postgres.PostgresTableManager;
import com.openlattice.requests.HazelcastRequestsManager;
import com.openlattice.requests.RequestQueryService;
import com.openlattice.search.PersistentSearchService;
import com.openlattice.search.SearchService;
import com.openlattice.tasks.PostConstructInitializerTaskDependencies;
import com.openlattice.tasks.PostConstructInitializerTaskDependencies.PostConstructInitializerTask;
import com.zaxxer.hikari.HikariDataSource;
import org.jdbi.v3.core.Jdbi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import static com.openlattice.datastore.util.Util.returnAndLog;

@Configuration
@Import( {
        Auth0Pod.class,
        ByteBlobServicePod.class,
        AssemblerConfigurationPod.class,
} )
public class DatastoreServicesPod {

    @Inject
    private Jdbi                      jdbi;
    @Inject
    private PostgresTableManager      tableManager;
    @Inject
    private HazelcastInstance         hazelcastInstance;
    @Inject
    private HikariDataSource          hikariDataSource;
    @Inject
    private Auth0Configuration        auth0Configuration;
    @Inject
    private AuditingConfiguration     auditingConfiguration;
    @Inject
    private ListeningExecutorService  executor;
    @Inject
    private EventBus                  eventBus;
    @Autowired( required = false )
    private AmazonS3                  awsS3;
    @Autowired( required = false )
    private AmazonLaunchConfiguration awsLaunchConfig;

    @Inject
    private ByteBlobDataManager byteBlobDataManager;

    @Inject
    private AssemblerConfiguration assemblerConfiguration;

    @Inject
    private MetricRegistry metricRegistry;

    @Bean
    public PostgresUserApi pgUserApi() {
        return jdbi.onDemand( PostgresUserApi.class );
    }

    @Bean
    public ObjectMapper defaultObjectMapper() {
        ObjectMapper mapper = ObjectMappers.getJsonMapper();
        FullQualifiedNameJacksonSerializer.registerWithMapper( mapper );

        return mapper;
    }

    @Bean
    public AuthorizationQueryService authorizationQueryService() {
        return new AuthorizationQueryService( hikariDataSource, hazelcastInstance );
    }

    @Bean
    public GraphQueryService graphQueryService() {
        return new PostgresGraphQueryService(
                hikariDataSource,
                dataModelService(),
                authorizationManager(),
                byteBlobDataManager,
                defaultObjectMapper()
        );
    }

    @Bean
    public AuthorizationManager authorizationManager() {
        return new HazelcastAuthorizationService( hazelcastInstance, authorizationQueryService(), eventBus );
    }

    @Bean
    public SecurableObjectResolveTypeService securableObjectTypes() {
        return new HazelcastSecurableObjectResolveTypeService( hazelcastInstance );
    }

    @Bean
    public SchemaQueryService schemaQueryService() {
        return new PostgresSchemaQueryService( hikariDataSource );
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
                pgEdmManager(),
                entityTypeManager(),
                schemaManager(),
                auditingConfiguration,
                assembler() );
    }

    @Bean
    public AuditRecordEntitySetsManager auditRecordEntitySetsManager() {
        return dataModelService().getAuditRecordEntitySetsManager();
    }

    @Bean
    public HazelcastAclKeyReservationService aclKeyReservationService() {
        return new HazelcastAclKeyReservationService( hazelcastInstance );
    }

    @Bean
    public ODataStorageService odataStorageService() {
        return new ODataStorageService(
                hazelcastInstance,
                dataModelService() );
    }

    @Bean
    public IndexingMetadataManager postgresDataManager() {
        return new IndexingMetadataManager( hikariDataSource );
    }

    @Bean
    public EntityDatastore entityDatastore() {
        return new HazelcastEntityDatastore( idService(),
                postgresDataManager(),
                dataQueryService(),
                dataModelService(),
                assembler() );
    }

    @Bean
    public Assembler assembler() {
        return new Assembler( dcs(), hikariDataSource, metricRegistry, hazelcastInstance, eventBus );
    }

    @Bean
    public PostConstructInitializerTaskDependencies postInitializerDependencies() {
        return new PostConstructInitializerTaskDependencies();
    }

    @Bean
    public PostConstructInitializerTask postInitializerTask() {
        return new PostConstructInitializerTask();
    }

    @Bean
    public SecurePrincipalsManager principalService() {
        return new HazelcastPrincipalService( hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                eventBus );
    }

    @Bean
    public HazelcastOrganizationService organizationsManager() {
        return new HazelcastOrganizationService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                principalService(),
                assembler() );
    }

    @Bean
    public AssemblerDependencies assemblerDependencies() {
        return new AssemblerDependencies(
                assemblerConfiguration,
                hikariDataSource,
                principalService(),
                organizationsManager(),
                dcs(),
                hazelcastInstance.getMap( HazelcastMap.ENTITY_SETS.name() ),
                assemblerConnectionManager(),
                hazelcastInstance.getMap( HazelcastMap.SECURABLE_OBJECT_TYPES.name() ),
                metricRegistry );
    }

    @Bean
    public UserCredentialSyncTask userCredentialSyncTask() {
        return new UserCredentialSyncTask();
    }

    @Bean
    public UserDirectoryService userDirectoryService() {
        return new UserDirectoryService( auth0TokenProvider(), hazelcastInstance );
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
    public AnalysisService analysisService() {
        return new AnalysisService();
    }

    @Bean
    public GraphService graphApi() {
        return new Graph( hikariDataSource, dataModelService() );
    }

    @Bean
    public HazelcastIdGenerationService idGenerationService() {
        return new HazelcastIdGenerationService( hazelcastInstance );
    }

    @Bean
    public EntityKeyIdService idService() {
        return new PostgresEntityKeyIdService( hazelcastInstance, executor, hikariDataSource, idGenerationService() );
    }

    @Bean
    public DataGraphManager dataGraphService() {
        return new DataGraphService(
                eventBus,
                graphApi(),
                idService(),
                entityDatastore() );
    }

    @Bean
    public DbCredentialService dcs() {
        return new DbCredentialService( hazelcastInstance, pgUserApi() );
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
    public PostgresEdmManager pgEdmManager() {
        PostgresEdmManager pgEdmManager = new PostgresEdmManager( hikariDataSource, tableManager, hazelcastInstance );
        eventBus.register( pgEdmManager );
        return pgEdmManager;
    }

    @Bean
    public Auth0TokenProvider auth0TokenProvider() {
        return new Auth0TokenProvider( auth0Configuration );
    }

    @Bean
    public ConductorElasticsearchApi conductorElasticsearchApi() {
        return new DatastoreConductorElasticsearchApi( hazelcastInstance );
    }

    @Bean
    public SearchService searchService() {
        return new SearchService( eventBus );
    }

    @Bean
    public PostgresEntityDataQueryService dataQueryService() {
        return new PostgresEntityDataQueryService( hikariDataSource, byteBlobDataManager );
    }

    @Bean
    public AssemblerConnectionManager assemblerConnectionManager() {
        return new AssemblerConnectionManager( assemblerConfiguration,
                hikariDataSource,
                principalService(),
                authorizationManager(),
                edmAuthorizationHelper(),
                organizationsManager(),
                dcs(),
                hazelcastInstance,
                eventBus,
                metricRegistry );
    }

    @Bean
    public PersistentSearchService persistentSearchService() {
        return new PersistentSearchService( hikariDataSource, principalService() );
    }

    @Bean PostgresDataSinkService postgresDataSinkService() {
        return new PostgresDataSinkService();
    }

    @Bean AwsDataSinkService awsDataSinkService() {
        return new AwsDataSinkService( byteBlobDataManager, hikariDataSource );
    }

    @Bean
    public PostgresLinkingFeedbackService postgresLinkingFeedbackQueryService() {
        return new PostgresLinkingFeedbackService( hikariDataSource, hazelcastInstance );
    }

    @Bean
    public LinkingQueryService lqs() {
        return new PostgresLinkingQueryService( hikariDataSource );
    }

    @Bean
    public RequestQueryService rqs() {
        return new RequestQueryService( hikariDataSource );
    }

    @Bean
    public HazelcastRequestsManager hazelcastRequestsManager() {
        return new HazelcastRequestsManager( hazelcastInstance, rqs() );
    }

    @PostConstruct
    void initPrincipals() {
        Principals.init( principalService() );
    }
}
