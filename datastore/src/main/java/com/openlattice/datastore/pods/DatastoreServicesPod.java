
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

import com.auth0.client.mgmt.ManagementAPI;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.geekbeast.auth0.ManagementApiProvider;
import com.geekbeast.mappers.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.geekbeast.hazelcast.HazelcastClientProvider;
import com.geekbeast.rhizome.jobs.HazelcastJobService;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.maps.GeoApiContext;
import com.hazelcast.core.HazelcastInstance;
import com.geekbeast.rhizome.configuration.ConfigurationConstants;
import com.openlattice.apps.services.AppService;
import com.openlattice.assembler.Assembler;
import com.openlattice.assembler.AssemblerConfiguration;
import com.openlattice.assembler.UserRoleSyncTaskDependencies;
import com.openlattice.assembler.pods.AssemblerConfigurationPod;
import com.openlattice.assembler.tasks.UserCredentialSyncTask;
import com.openlattice.auditing.AuditRecordEntitySetsManager;
import com.openlattice.auditing.AuditingConfiguration;
import com.openlattice.auditing.AuditingManager;
import com.openlattice.auditing.AuditingProfiles;
import com.openlattice.auditing.LocalAuditingService;
import com.openlattice.auditing.S3AuditingService;
import com.geekbeast.auth0.Auth0Pod;
import com.geekbeast.auth0.RefreshingAuth0TokenProvider;
import com.geekbeast.authentication.Auth0Configuration;
import com.openlattice.authorization.*;
import com.openlattice.authorization.mapstores.ResolvedPrincipalTreesMapLoader;
import com.openlattice.authorization.mapstores.SecurablePrincipalsMapLoader;
import com.openlattice.codex.CodexService;
import com.openlattice.collaborations.CollaborationDatabaseManager;
import com.openlattice.collaborations.CollaborationService;
import com.openlattice.collaborations.PostgresCollaborationDatabaseService;
import com.openlattice.collections.CollectionsManager;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.data.DataDeletionManager;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DataGraphService;
import com.openlattice.data.EntityKeyIdService;
import com.openlattice.data.graph.DataGraphServiceHelper;
import com.openlattice.data.ids.PostgresEntityKeyIdService;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.data.storage.ByteBlobDataManager;
import com.openlattice.data.storage.DataDeletionService;
import com.openlattice.data.storage.DataSourceResolver;
import com.openlattice.data.storage.EntityDatastore;
import com.openlattice.data.storage.IndexingMetadataManager;
import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService;
import com.openlattice.data.storage.postgres.PostgresEntityDatastore;
import com.openlattice.data.storage.PostgresEntitySetSizesTaskDependency;
import com.openlattice.data.storage.aws.AwsDataSinkService;
import com.openlattice.datasets.DataSetService;
import com.openlattice.datastore.configuration.DatastoreConfiguration;
import com.openlattice.datastore.configuration.ReadonlyDatasourceSupplier;
import com.openlattice.datastore.services.AnalysisService;
import com.openlattice.datastore.services.DatastoreKotlinElasticsearchImpl;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.EntitySetManager;
import com.openlattice.datastore.services.EntitySetService;
import com.openlattice.directory.Auth0UserDirectoryService;
import com.openlattice.directory.LocalUserDirectoryService;
import com.openlattice.directory.UserDirectoryService;
import com.openlattice.edm.PostgresEdmManager;
import com.openlattice.edm.properties.PostgresTypeManager;
import com.openlattice.edm.schemas.SchemaQueryService;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.graph.Graph;
import com.openlattice.graph.GraphQueryService;
import com.openlattice.graph.PostgresGraphQueryService;
import com.openlattice.graph.core.GraphService;
import com.openlattice.ids.HazelcastIdGenerationService;
import com.openlattice.ids.HazelcastLongIdService;
import com.geekbeast.jdbc.DataSourceManager;
import com.openlattice.linking.LinkingQueryService;
import com.openlattice.linking.PostgresLinkingFeedbackService;
import com.openlattice.linking.graph.PostgresLinkingQueryService;
import com.openlattice.notifications.sms.PhoneNumberService;
import com.openlattice.organizations.ExternalDatabaseManagementService;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.OrganizationExternalDatabaseConfiguration;
import com.openlattice.organizations.WarehousesService;
import com.openlattice.organizations.pods.OrganizationExternalDatabaseConfigurationPod;
import com.openlattice.organizations.roles.HazelcastPrincipalService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.postgres.PostgresTable;
import com.geekbeast.postgres.PostgresTableManager;
import com.openlattice.postgres.external.DatabaseQueryManager;
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager;
import com.openlattice.postgres.external.ExternalDatabasePermissioner;
import com.openlattice.postgres.external.ExternalDatabasePermissioningService;
import com.openlattice.postgres.external.PostgresDatabaseQueryService;
import com.openlattice.requests.HazelcastRequestsManager;
import com.openlattice.requests.RequestQueryService;
import com.openlattice.search.PersistentSearchService;
import com.openlattice.search.SearchService;
import com.openlattice.subscriptions.PostgresSubscriptionService;
import com.openlattice.subscriptions.SubscriptionService;
import com.geekbeast.tasks.PostConstructInitializerTaskDependencies;
import com.geekbeast.tasks.PostConstructInitializerTaskDependencies.PostConstructInitializerTask;
import com.openlattice.twilio.TwilioConfiguration;
import com.openlattice.twilio.pods.TwilioConfigurationPod;
import com.openlattice.users.Auth0SyncService;
import com.openlattice.users.Auth0UserListingService;
import com.openlattice.users.LocalUserListingService;
import com.openlattice.users.UserListingService;
import com.openlattice.users.export.Auth0ApiExtension;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

@Configuration
@Import( {
        Auth0Pod.class,
        ByteBlobServicePod.class,
        AssemblerConfigurationPod.class,
        // TransporterPod.class,
        TwilioConfigurationPod.class,
        OrganizationExternalDatabaseConfigurationPod.class
} )
public class DatastoreServicesPod {
    private static final Logger logger = LoggerFactory.getLogger( DatastoreServicesPod.class );

    @Inject
    private Jdbi                     jdbi;
    @Inject
    private PostgresTableManager     tableManager;
    @Inject
    private HazelcastInstance        hazelcastInstance;
    @Inject
    private HikariDataSource         hikariDataSource;
    @Inject
    private Auth0Configuration       auth0Configuration;
    @Inject
    private AuditingConfiguration    auditingConfiguration;
    @Inject
    private ListeningExecutorService executor;
    @Inject
    private EventBus                 eventBus;

    @Inject
    private DatastoreConfiguration datastoreConfiguration;

    @Inject
    private ByteBlobDataManager byteBlobDataManager;

    @Inject
    private AssemblerConfiguration assemblerConfiguration;

    @Inject
    private TwilioConfiguration twilioConfiguration;

    @Inject
    private MetricRegistry metricRegistry;

    @Inject
    private HealthCheckRegistry healthCheckRegistry;

    @Inject
    private HazelcastClientProvider hazelcastClientProvider;

    @Inject
    private OrganizationExternalDatabaseConfiguration organizationExternalDatabaseConfiguration;

    @Inject
    private SecurablePrincipalsMapLoader spml;

    @Inject
    private ResolvedPrincipalTreesMapLoader rptml;

    @Inject
    private ExternalDatabaseConnectionManager externalDbConnMan;

    // @Inject
    // private TransporterService transporterService;

    @Inject
    private DataSourceManager dataSourceManager;

    @Bean
    public PrincipalsMapManager principalsMapManager() {
        return new HazelcastPrincipalsMapManager( hazelcastInstance, aclKeyReservationService() );
    }

    @Bean
    public SecurePrincipalsManager securePrincipalsManager() {
        return new HazelcastPrincipalService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                principalsMapManager(),
                externalDatabasePermissionsManager()
        );
    }

    @Bean
    public ExternalDatabasePermissioningService externalDatabasePermissionsManager() {
        return new ExternalDatabasePermissioner(
                hazelcastInstance,
                externalDbConnMan,
                dcs(),
                principalsMapManager()
        );
    }

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
    public ManagementApiProvider managementApiProvider() {
        return new ManagementApiProvider(  auth0TokenProvider() , auth0Configuration);
    }

    @Bean
    public Auth0SyncService auth0SyncService() {
        return new Auth0SyncService( hazelcastInstance, securePrincipalsManager(), organizationsManager() );
    }

    @Bean
    public GraphQueryService graphQueryService() {
        return new PostgresGraphQueryService( hikariDataSource, entitySetManager(), dataQueryService() );
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
        return new HazelcastAuthorizationService( hazelcastInstance, eventBus, principalsMapManager() );
    }

    @Bean
    public SecurableObjectResolveTypeService securableObjectTypes() {
        return new HazelcastSecurableObjectResolveTypeService( hazelcastInstance );
    }

    @Bean
    public SchemaQueryService schemaQueryService() {
        return entityTypeManager();
    }

    @Bean
    public HazelcastSchemaManager schemaManager() {
        return new HazelcastSchemaManager(
                hazelcastInstance,
                schemaQueryService() );
    }

    @Bean
    public PostgresTypeManager entityTypeManager() {
        return new PostgresTypeManager( hikariDataSource, hazelcastInstance );
    }

    @Bean
    DataSetService dataSetService() {
        return new DataSetService( hazelcastInstance, elasticsearchApi() );
    }

    @Bean
    public EdmManager dataModelService() {
        return new EdmService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                entityTypeManager(),
                schemaManager(),
                dataSetService()
        );
    }

    @Bean
    public EntitySetManager entitySetManager() {
        return new EntitySetService(
                hazelcastInstance,
                eventBus,
                aclKeyReservationService(),
                authorizationManager(),
                dataModelService(),
                hikariDataSource,
                dataSetService(),
                auditingConfiguration
        );
    }

    @Bean
    public AuditRecordEntitySetsManager auditRecordEntitySetsManager() {
        return entitySetManager().getAuditRecordEntitySetsManager();
    }

    @Bean
    public HazelcastAclKeyReservationService aclKeyReservationService() {
        return new HazelcastAclKeyReservationService( hazelcastInstance );
    }

    @Bean
    public IndexingMetadataManager indexingMetadataManager() {
        return new IndexingMetadataManager( dataSourceResolver() );
    }

    @Bean
    public EntityDatastore entityDatastore() {
        return new PostgresEntityDatastore(
                dataQueryService(),
                dataModelService(),
                entitySetManager(),
                metricRegistry,
                eventBus,
                postgresLinkingFeedbackQueryService(),
                lqs()
        );
    }

    @Bean
    public Assembler assembler() {
        return new Assembler(
                dcs(),
                authorizationManager(),
                securePrincipalsManager(),
                dbQueryManager(),
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
    public PhoneNumberService phoneNumberService() {
        return new PhoneNumberService( hazelcastInstance );
    }

    @Bean
    public DatabaseQueryManager dbQueryManager() {
        return new PostgresDatabaseQueryService(
                assemblerConfiguration,
                externalDbConnMan,
                securePrincipalsManager(),
                dcs()
        );
    }

    @Bean
    public CollaborationDatabaseManager collaborationDatabaseManager() {
        return new PostgresCollaborationDatabaseService(
                hazelcastInstance,
                dbQueryManager(),
                externalDbConnMan,
                authorizationManager(),
                externalDatabasePermissionsManager(),
                securePrincipalsManager(),
                dcs(),
                assemblerConfiguration
        );
    }

    @Bean
    public CollaborationService collaborationService() {
        return new CollaborationService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                securePrincipalsManager(),
                collaborationDatabaseManager()
        );
    }

    @Bean
    public HazelcastOrganizationService organizationsManager() {
        return new HazelcastOrganizationService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                securePrincipalsManager(),
                phoneNumberService(),
                assembler(),
                collaborationService()
        );
    }

    @Bean
    public UserRoleSyncTaskDependencies assemblerDependencies() {
        return new UserRoleSyncTaskDependencies(
                dcs(),
                externalDbConnMan,
                externalDatabasePermissionsManager(),
                securePrincipalsManager()
        );
    }

    @Bean
    public UserCredentialSyncTask userCredentialSyncTask() {
        return new UserCredentialSyncTask();
    }

    @Bean
    public UserListingService userListingService() {
        if ( auth0Configuration.getManagementApiUrl().contains( Auth0Configuration.NO_SYNC_URL ) ) {
            return new LocalUserListingService( auth0Configuration );
        }

        var auth0Token = auth0TokenProvider().getToken();
        return new Auth0UserListingService(
                managementApiProvider(),
                new Auth0ApiExtension( auth0Configuration.getDomain(), auth0Token )
        );
    }


    @Bean
    public UserDirectoryService userDirectoryService() {
        if ( auth0Configuration.getManagementApiUrl().contains( Auth0Configuration.NO_SYNC_URL ) ) {
            return new LocalUserDirectoryService( auth0Configuration );
        }
        return new Auth0UserDirectoryService( auth0TokenProvider(), hazelcastInstance );
    }

    @Bean
    public EdmAuthorizationHelper edmAuthorizationHelper() {
        return new EdmAuthorizationHelper( dataModelService(), authorizationManager(), entitySetManager() );
    }

    @Bean
    public GraphService graphApi() {
        return new Graph( dataSourceResolver(),
                entitySetManager(),
                dataQueryService(),
                idService(),
                metricRegistry );
    }

    @Bean
    public HazelcastIdGenerationService idGenerationService() {
        return new HazelcastIdGenerationService( hazelcastClientProvider );
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
    public EntityKeyIdService idService() {
        return new PostgresEntityKeyIdService(
                dataSourceResolver(),
                idGenerationService()
        );
    }

    @Bean
    public HazelcastJobService jobService() {
        return new HazelcastJobService( hazelcastInstance );
    }

    @Bean
    public DataGraphManager dataGraphService() {
        return new DataGraphService( graphApi(), idService(), entityDatastore(), jobService() );
    }

    @Bean
    public DataGraphServiceHelper dataGraphServiceHelper() {
        return new DataGraphServiceHelper( entitySetManager() );
    }

    @Bean
    public DbCredentialService dcs() {
        return new DbCredentialService( hazelcastInstance, longIdService() );
    }

    @Bean
    public AppService appService() {
        logger.info( "Checkpoint app service" );
        return new AppService(
                hazelcastInstance,
                dataModelService(),
                organizationsManager(),
                authorizationManager(),
                securePrincipalsManager(),
                aclKeyReservationService(),
                collectionsManager(),
                entitySetManager()
        );
    }

    @Bean
    public PostgresEdmManager pgEdmManager() {
        return new PostgresEdmManager( hikariDataSource );
    }

    @Bean
    public RefreshingAuth0TokenProvider auth0TokenProvider() {
        return new RefreshingAuth0TokenProvider( auth0Configuration );
    }

    @Bean
    public ConductorElasticsearchApi elasticsearchApi() {
        return new DatastoreKotlinElasticsearchImpl( datastoreConfiguration.getSearchConfiguration() );
    }

    @Bean
    public SearchService searchService() {
        return new SearchService(
                eventBus,
                metricRegistry,
                authorizationManager(),
                elasticsearchApi(),
                dataModelService(),
                entitySetManager(),
                graphApi(),
                entityDatastore(),
                indexingMetadataManager(),
                dataSetService()
        );
    }

    @Bean
    public ReadonlyDatasourceSupplier rds() {
        var pgConfig = datastoreConfiguration.getReadOnlyReplica();
        HikariDataSource reader;

        if ( pgConfig.isEmpty() ) {
            reader = hikariDataSource;
        } else {
            HikariConfig hc = new HikariConfig( pgConfig );
            logger.info( "Read only replica JDBC URL = {}", hc.getJdbcUrl() );
            reader = new HikariDataSource( hc );
            reader.setHealthCheckRegistry( healthCheckRegistry );
            reader.setMetricRegistry( metricRegistry );
        }

        return new ReadonlyDatasourceSupplier( reader );
    }

    @Bean
    public PostgresEntityDataQueryService dataQueryService() {
        return new PostgresEntityDataQueryService(
                dataSourceResolver(),
                byteBlobDataManager
        );
    }

    @Bean
    public PersistentSearchService persistentSearchService() {
        return new PersistentSearchService( hikariDataSource, securePrincipalsManager() );
    }

    @Bean AwsDataSinkService awsDataSinkService() {
        return new AwsDataSinkService(
                byteBlobDataManager,
                dataSourceResolver()
        );
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

    @Bean
    public PostgresEntitySetSizesTaskDependency postgresEntitySetSizesTaskDependency() {
        return new PostgresEntitySetSizesTaskDependency( hikariDataSource );
    }

    @Bean
    public HazelcastLongIdService longIdService() {
        return new HazelcastLongIdService( hazelcastClientProvider );
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
        return new CollectionsManager(
                hazelcastInstance,
                dataModelService(),
                entitySetManager(),
                aclKeyReservationService(),
                schemaManager(),
                authorizationManager(),
                eventBus
        );
    }

    @Bean
    public DataDeletionManager dataDeletionManager() {
        return new DataDeletionService(
                entitySetManager(),
                authorizationManager(),
                entityDatastore(),
                graphApi(),
                jobService()
        );
    }

    @Bean
    public ExternalDatabaseManagementService edms() {
        return new ExternalDatabaseManagementService(
                hazelcastInstance,
                externalDbConnMan,
                principalsMapManager(),
                aclKeyReservationService(),
                authorizationManager(),
                organizationExternalDatabaseConfiguration,
                externalDatabasePermissionsManager(),
                dcs(),
                hikariDataSource,
                dataSetService()
        );
    }

    @Bean
    public CodexService codexService() {
        return new CodexService(
                aclKeyReservationService(),
                twilioConfiguration,
                hazelcastInstance,
                dataModelService(),
                dataGraphService(),
                idService(),
                securePrincipalsManager(),
                organizationsManager(),
                collectionsManager(),
                executor,
                hikariDataSource,
                searchService()
        );
    }

    @Bean
    public WarehousesService warehousesService() {
        return new WarehousesService(
                hazelcastInstance,
                authorizationManager(),
                aclKeyReservationService()
        );
    }

    @Bean
    public GeoApiContext geoApiContext() {
        return new GeoApiContext.Builder().apiKey( datastoreConfiguration.getGoogleMapsApiKey() ).build();
    }

    @Bean
    public AnalysisService analysisService() {
        return new AnalysisService( dataGraphService(),
                authorizationManager(),
                dataModelService(),
                entitySetManager() );
    }

    @PostConstruct
    void initPrincipals() {
        Principals.init( securePrincipalsManager(), hazelcastInstance );
        dataGraphService();
    }
}
