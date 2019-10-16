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
import com.geekbeast.hazelcast.HazelcastClientProvider;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.rhizome.configuration.ConfigurationConstants;
import com.kryptnostic.rhizome.configuration.amazon.AmazonLaunchConfiguration;
import com.openlattice.analysis.AnalysisService;
import com.openlattice.assembler.*;
import com.openlattice.assembler.pods.AssemblerConfigurationPod;
import com.openlattice.assembler.tasks.UserCredentialSyncTask;
import com.openlattice.auditing.*;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.auth0.Auth0TokenProvider;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.authorization.*;
import com.openlattice.collections.CollectionsManager;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DataGraphService;
import com.openlattice.data.EntityKeyIdService;
import com.openlattice.data.graph.DataGraphServiceHelper;
import com.openlattice.data.ids.PostgresEntityKeyIdService;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.data.storage.*;
import com.openlattice.data.storage.aws.AwsDataSinkService;
import com.openlattice.data.storage.partitions.PartitionManager;
import com.openlattice.datastore.apps.services.AppService;
import com.openlattice.datastore.services.DatastoreConductorElasticsearchApi;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.SyncTicketService;
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
import com.openlattice.ids.HazelcastLongIdService;
import com.openlattice.linking.LinkingQueryService;
import com.openlattice.linking.PostgresLinkingFeedbackService;
import com.openlattice.linking.graph.PostgresLinkingQueryService;
import com.openlattice.notifications.sms.PhoneNumberService;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.roles.HazelcastPrincipalService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.postgres.PostgresTableManager;
import com.openlattice.requests.HazelcastRequestsManager;
import com.openlattice.requests.RequestQueryService;
import com.openlattice.search.PersistentSearchService;
import com.openlattice.search.SearchService;
import com.openlattice.subscriptions.PostgresSubscriptionService;
import com.openlattice.subscriptions.SubscriptionService;
import com.openlattice.tasks.PostConstructInitializerTaskDependencies;
import com.openlattice.tasks.PostConstructInitializerTaskDependencies.PostConstructInitializerTask;
import com.openlattice.twilio.TwilioConfiguration;
import com.openlattice.twilio.pods.TwilioConfigurationPod;
import com.zaxxer.hikari.HikariDataSource;
import org.jdbi.v3.core.Jdbi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import static com.openlattice.datastore.util.Util.returnAndLog;

@Configuration
@Import( {
        Auth0Pod.class,
        ByteBlobServicePod.class,
        AssemblerConfigurationPod.class,
        TwilioConfigurationPod.class
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
    private TwilioConfiguration twilioConfiguration;

    @Inject
    private MetricRegistry metricRegistry;

    @Inject
    private HazelcastClientProvider hazelcastClientProvider;

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
                dataQueryService()
        );
    }

    @Bean
    public SubscriptionService subscriptionService() {
        return new PostgresSubscriptionService(
                hikariDataSource,
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

    @Bean PartitionManager partitionManager() {
        return new PartitionManager( hazelcastInstance, hikariDataSource );
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
                partitionManager() );
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
    public IndexingMetadataManager indexingMetadataManager() {
        return new IndexingMetadataManager( hikariDataSource, partitionManager() );
    }

    @Bean
    public EntityDatastore entityDatastore() {
        return new PostgresEntityDatastore( dataQueryService(), dataModelService(), pgEdmManager() );
    }

    @Bean
    public Assembler assembler() {
        return new Assembler(
                dcs(),
                hikariDataSource,
                authorizationManager(),
                edmAuthorizationHelper(),
                principalService(),
                partitionManager(),
                metricRegistry,
                hazelcastInstance,
                eventBus
        );
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
    public PhoneNumberService phoneNumberService() {
        return new PhoneNumberService( hazelcastInstance );
    }

    @Bean
    public HazelcastOrganizationService organizationsManager() {
        return new HazelcastOrganizationService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                principalService(),
                phoneNumberService(),
                partitionManager(),
                assembler() );
    }

    @Bean
    public AssemblerDependencies assemblerDependencies() {
        return new AssemblerDependencies( hikariDataSource, dcs(), assemblerConnectionManager() );
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
        return new Graph( hikariDataSource, dataModelService(), partitionManager() );
    }

    @Bean
    public HazelcastIdGenerationService idGenerationService() {
        return new HazelcastIdGenerationService( hazelcastClientProvider );
    }

    @Bean
    public EntityKeyIdService idService() {
        return new PostgresEntityKeyIdService( hazelcastClientProvider,
                executor,
                hikariDataSource,
                idGenerationService(),
                partitionManager() );
    }

    @Bean
    public DataGraphManager dataGraphService() {
        return new DataGraphService( graphApi(), idService(), entityDatastore(), postgresEntitySetSizeCacheManager() );
    }

    @Bean
    public DataGraphServiceHelper dataGraphServiceHelper() {
        return new DataGraphServiceHelper( dataModelService() );
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
        final var pgEdmManager = new PostgresEdmManager( hikariDataSource, hazelcastInstance );
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
        return new SearchService( eventBus, metricRegistry );
    }

    @Bean
    public PostgresEntityDataQueryService dataQueryService() {
        return new PostgresEntityDataQueryService(
                hikariDataSource,
                byteBlobDataManager,
                partitionManager() );
    }

    @Bean
    public AssemblerConnectionManager assemblerConnectionManager() {
        return new AssemblerConnectionManager( assemblerConfiguration,
                hikariDataSource,
                principalService(),
                organizationsManager(),
                dcs(),
                eventBus,
                metricRegistry );
    }

    @Bean
    public AssemblerQueryService assemblerQueryService() {
        return new AssemblerQueryService( dataModelService() );
    }

    @Bean
    public PersistentSearchService persistentSearchService() {
        return new PersistentSearchService( hikariDataSource, principalService() );
    }

    @Bean PostgresDataSinkService postgresDataSinkService() {
        return new PostgresDataSinkService();
    }

    @Bean AwsDataSinkService awsDataSinkService() {
        return new AwsDataSinkService( partitionManager(), byteBlobDataManager, hikariDataSource );
    }

    @Bean
    public PostgresLinkingFeedbackService postgresLinkingFeedbackQueryService() {
        return new PostgresLinkingFeedbackService( hikariDataSource, hazelcastInstance );
    }

    @Bean
    public LinkingQueryService lqs() {
        return new PostgresLinkingQueryService( hikariDataSource, partitionManager() );
    }

    @Bean
    public RequestQueryService rqs() {
        return new RequestQueryService( hikariDataSource );
    }

    @Bean
    public HazelcastRequestsManager hazelcastRequestsManager() {
        return new HazelcastRequestsManager( hazelcastInstance, rqs() );
    }

    @Bean
    public PostgresEntitySetSizesTaskDependency postgresEntitySetSizesTaskDependency() {
        return new PostgresEntitySetSizesTaskDependency( hikariDataSource );
    }

    @Bean
    public PostgresEntitySetSizesTask postgresEntitySetSizeCacheManager() {
        return new PostgresEntitySetSizesTask();
    }

    @Bean
    public PostgresEntitySetSizesInitializationTask postgresEntitySetSizesInitializationTask() {
        return new PostgresEntitySetSizesInitializationTask();
    }

    @Bean
    public HazelcastLongIdService longIdService() {
        return new HazelcastLongIdService( hazelcastClientProvider, hazelcastInstance );
    }

    @Bean
    @Profile( { ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE,
            ConfigurationConstants.Profiles.AWS_TESTING_PROFILE, AuditingProfiles.LOCAL_AWS_AUDITING_PROFILE } )
    public AuditingManager s3AuditingService() {
        return new S3AuditingService( auditingConfiguration, longIdService(), defaultObjectMapper() );
    }

    @Bean
    @Profile( AuditingProfiles.LOCAL_AUDITING_PROFILE )
    public AuditingManager localAuditingService() {
        return new LocalAuditingService( dataGraphService(), auditRecordEntitySetsManager(), defaultObjectMapper() );
    }

    @Bean
    public CollectionsManager collectionsManager() {
        return new CollectionsManager( hazelcastInstance,
                dataModelService(),
                aclKeyReservationService(),
                schemaManager(),
                authorizationManager(),
                eventBus );
    }

    @PostConstruct
    void initPrincipals() {
        Principals.init( principalService() );
    }
}
