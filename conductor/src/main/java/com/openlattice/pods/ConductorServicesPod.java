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

package com.openlattice.pods;

import com.auth0.client.mgmt.ManagementAPI;
import com.codahale.metrics.MetricRegistry;
import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.geekbeast.hazelcast.HazelcastClientProvider;
import com.geekbeast.rhizome.jobs.HazelcastJobService;
import com.geekbeast.rhizome.jobs.ResumeJobDependencies;
import com.geekbeast.rhizome.jobs.ResumeJobsInitializationTask;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.rhizome.configuration.ConfigurationConstants;
import com.kryptnostic.rhizome.pods.ConfigurationLoader;
import com.openlattice.assembler.Assembler;
import com.openlattice.assembler.Assembler.EntitySetViewsInitializerTask;
import com.openlattice.assembler.Assembler.OrganizationAssembliesInitializerTask;
import com.openlattice.assembler.AssemblerConfiguration;
import com.openlattice.assembler.MaterializedEntitySetsDependencies;
import com.openlattice.assembler.UserRoleSyncTaskDependencies;
import com.openlattice.assembler.pods.AssemblerConfigurationPod;
import com.openlattice.assembler.tasks.UsersAndRolesInitializationTask;
import com.openlattice.auditing.AuditInitializationTask;
import com.openlattice.auditing.AuditTaskDependencies;
import com.openlattice.auditing.AuditingConfiguration;
import com.openlattice.auditing.pods.AuditingConfigurationPod;
import com.openlattice.auth0.Auth0TokenProvider;
import com.openlattice.auth0.AwsAuth0TokenProvider;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.authorization.*;
import com.openlattice.authorization.initializers.AuthorizationInitializationDependencies;
import com.openlattice.authorization.initializers.AuthorizationInitializationTask;
import com.openlattice.authorization.mapstores.ResolvedPrincipalTreesMapLoader;
import com.openlattice.authorization.mapstores.SecurablePrincipalsMapLoader;
import com.openlattice.collaborations.CollaborationDatabaseManager;
import com.openlattice.collaborations.CollaborationService;
import com.openlattice.collaborations.PostgresCollaborationDatabaseService;
import com.openlattice.collections.CollectionsManager;
import com.openlattice.conductor.rpc.ConductorConfiguration;
import com.openlattice.conductor.rpc.MapboxConfiguration;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DataGraphService;
import com.openlattice.data.EntityKeyIdService;
import com.openlattice.data.ids.PostgresEntityKeyIdService;
import com.openlattice.data.storage.ByteBlobDataManager;
import com.openlattice.data.storage.EntityDatastore;
import com.openlattice.data.storage.IndexingMetadataManager;
import com.openlattice.data.storage.PostgresEntityDataQueryService;
import com.openlattice.data.storage.PostgresEntityDatastore;
import com.openlattice.data.storage.PostgresEntitySetSizesInitializationTask;
import com.openlattice.data.storage.PostgresEntitySetSizesTaskDependency;
import com.openlattice.data.storage.partitions.PartitionManager;
import com.openlattice.datastore.pods.ByteBlobServicePod;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.EntitySetManager;
import com.openlattice.datastore.services.EntitySetService;
import com.openlattice.directory.Auth0UserDirectoryService;
import com.openlattice.directory.LocalUserDirectoryService;
import com.openlattice.directory.UserDirectoryService;
import com.openlattice.edm.properties.PostgresTypeManager;
import com.openlattice.edm.schemas.SchemaQueryService;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.graph.Graph;
import com.openlattice.graph.GraphQueryService;
import com.openlattice.graph.PostgresGraphQueryService;
import com.openlattice.graph.core.GraphService;
import com.openlattice.hazelcast.HazelcastClient;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.hazelcast.HazelcastQueue;
import com.openlattice.ids.HazelcastIdGenerationService;
import com.openlattice.ids.HazelcastLongIdService;
import com.openlattice.ids.tasks.IdGenerationCatchUpTask;
import com.openlattice.ids.tasks.IdGenerationCatchupDependency;
import com.openlattice.linking.LinkingQueryService;
import com.openlattice.linking.PostgresLinkingFeedbackService;
import com.openlattice.linking.graph.PostgresLinkingQueryService;
import com.openlattice.mail.MailServiceClient;
import com.openlattice.mail.config.MailServiceRequirements;
import com.openlattice.notifications.sms.PhoneNumberService;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.OrganizationMetadataEntitySetsService;
import com.openlattice.organizations.roles.HazelcastPrincipalService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.organizations.tasks.OrganizationsInitializationDependencies;
import com.openlattice.organizations.tasks.OrganizationsInitializationTask;
import com.openlattice.postgres.external.DatabaseQueryManager;
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager;
import com.openlattice.postgres.external.ExternalDatabasePermissioner;
import com.openlattice.postgres.external.ExternalDatabasePermissioningService;
import com.openlattice.postgres.external.PostgresDatabaseQueryService;
import com.openlattice.postgres.tasks.PostgresMetaDataPropertiesInitializationDependency;
import com.openlattice.postgres.tasks.PostgresMetaDataPropertiesInitializationTask;
import com.openlattice.scheduling.ScheduledTaskService;
import com.openlattice.scheduling.ScheduledTaskServiceDependencies;
import com.openlattice.subscriptions.PostgresSubscriptionService;
import com.openlattice.subscriptions.SubscriptionNotificationDependencies;
import com.openlattice.subscriptions.SubscriptionNotificationTask;
import com.openlattice.subscriptions.SubscriptionService;
import com.openlattice.tasks.PostConstructInitializerTaskDependencies;
import com.openlattice.tasks.PostConstructInitializerTaskDependencies.PostConstructInitializerTask;
import com.openlattice.transporter.pods.TransporterInitPod;
import com.openlattice.transporter.pods.TransporterPod;
import com.openlattice.transporter.services.TransporterService;
import com.openlattice.users.Auth0SyncInitializationTask;
import com.openlattice.users.Auth0SyncService;
import com.openlattice.users.Auth0SyncTask;
import com.openlattice.users.Auth0SyncTaskDependencies;
import com.openlattice.users.Auth0UserListingService;
import com.openlattice.users.DefaultAuth0SyncTask;
import com.openlattice.users.LocalAuth0SyncTask;
import com.openlattice.users.LocalUserListingService;
import com.openlattice.users.UserListingService;
import com.openlattice.users.export.Auth0ApiExtension;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

@Configuration
@Import( { ByteBlobServicePod.class, AuditingConfigurationPod.class, AssemblerConfigurationPod.class,
        TransporterPod.class, TransporterInitPod.class } )
public class ConductorServicesPod {

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private Auth0Configuration auth0Configuration;

    @Inject
    private AuditingConfiguration auditingConfiguration;

    @Inject
    private HikariDataSource hikariDataSource;

    @Inject
    private ByteBlobDataManager byteBlobDataManager;

    @Inject
    private EventBus eventBus;

    @Inject
    private ListeningExecutorService executor;

    @Inject
    private AssemblerConfiguration assemblerConfiguration;

    @Inject
    private ConfigurationLoader configurationLoader;

    @Inject
    private MetricRegistry metricRegistry;

    @Inject
    private HazelcastClientProvider hazelcastClientProvider;

    @Inject
    private SecurablePrincipalsMapLoader spml;

    @Inject
    private ResolvedPrincipalTreesMapLoader rptml;

    @Inject
    private ExternalDatabaseConnectionManager externalDbConnMan;

    @Inject
    private TransporterService transporterService;

    @Bean
    public ObjectMapper defaultObjectMapper() {
        return ObjectMappers.getJsonMapper();
    }

    @Bean
    public ConductorConfiguration conductorConfiguration() {
        return configurationLoader.logAndLoad( "conductor", ConductorConfiguration.class );
    }

    @Bean
    public MapboxConfiguration mapboxConfiguration() {
        return configurationLoader.load( MapboxConfiguration.class );
    }

    @Bean
    public DbCredentialService dbCredService() {
        return new DbCredentialService( hazelcastInstance, longIdService() );
    }

    @Bean
    public PrincipalsMapManager principalsMapManager() {
        return new HazelcastPrincipalsMapManager( hazelcastInstance, aclKeyReservationService() );
    }

    @Bean
    public HazelcastAclKeyReservationService aclKeyReservationService() {
        return new HazelcastAclKeyReservationService( hazelcastInstance );
    }

    @Bean
    public HazelcastLongIdService longIdService() {
        return new HazelcastLongIdService( hazelcastClientProvider );
    }

    @Bean
    public HazelcastIdGenerationService idGenerationService() {
        return new HazelcastIdGenerationService( hazelcastClientProvider );
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
                dbCredService(),
                principalsMapManager()
        );
    }

    @Bean
    public AuthorizationManager authorizationManager() {
        return new HazelcastAuthorizationService( hazelcastInstance, eventBus );
    }

    @Bean
    public UserDirectoryService userDirectoryService() {
        if ( auth0Configuration.getManagementApiUrl().contains( Auth0Configuration.NO_SYNC_URL ) ) {
            return new LocalUserDirectoryService( auth0Configuration );
        }
        return new Auth0UserDirectoryService( auth0TokenProvider(), hazelcastInstance );
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
    public Assembler assembler() {
        return new Assembler(
                dbCredService(),
                authorizationManager(),
                securePrincipalsManager(),
                dbQueryManager(),
                metricRegistry,
                hazelcastInstance,
                eventBus
        );
    }

    @Bean
    public OrganizationsInitializationDependencies organizationBootstrapDependencies() {
        return new OrganizationsInitializationDependencies( organizationsManager(),
                securePrincipalsManager(),
                partitionManager(),
                conductorConfiguration() );
    }

    @Bean
    public AuthorizationInitializationDependencies authorizationBootstrapDependencies() {
        return new AuthorizationInitializationDependencies( securePrincipalsManager() );
    }

    @Bean
    public UserRoleSyncTaskDependencies assemblerDependencies() {
        return new UserRoleSyncTaskDependencies(
                dbCredService(),
                externalDbConnMan,
                externalDatabasePermissionsManager(),
                securePrincipalsManager()
        );
    }

    @Bean
    public MaterializedEntitySetsDependencies materializedEntitySetsDependencies() {
        return new MaterializedEntitySetsDependencies(
                assembler(),
                HazelcastMap.MATERIALIZED_ENTITY_SETS.getMap( hazelcastInstance ),
                organizationsManager(),
                dataModelService(),
                authorizingComponent(),
                hikariDataSource
        );
    }

    @Bean
    public AuthorizationInitializationTask authorizationBootstrap() {
        return new AuthorizationInitializationTask();
    }

    @Bean
    public UsersAndRolesInitializationTask assemblerInitializationTask() {
        return new UsersAndRolesInitializationTask();
    }

    @Bean
    public OrganizationAssembliesInitializerTask organizationAssembliesInitializerTask() {
        return new OrganizationAssembliesInitializerTask();
    }

    @Bean
    public EntitySetViewsInitializerTask entityViewsInitializerTask() {
        return new EntitySetViewsInitializerTask();
    }

    @Bean
    public AuditTaskDependencies auditTaskDependencies() {
        return new AuditTaskDependencies(
                securePrincipalsManager(),
                entitySetManager(),
                authorizationManager(),
                partitionManager(),
                organizationsManager()
        );
    }

    @Bean
    public DatabaseQueryManager dbQueryManager() {
        return new PostgresDatabaseQueryService(
                assemblerConfiguration,
                externalDbConnMan,
                securePrincipalsManager(),
                dbCredService()
        );
    }

    @Bean
    public PhoneNumberService phoneNumberService() {
        return new PhoneNumberService( hazelcastInstance );
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
                dbCredService(),
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
                partitionManager(),
                assembler(),
                organizationMetadataEntitySetsService(),
                collaborationService() );
    }

    @Bean
    public OrganizationsInitializationTask organizationBootstrap() {
        return new OrganizationsInitializationTask();
    }

    @Bean
    public Auth0TokenProvider auth0TokenProvider() {
        return new AwsAuth0TokenProvider( auth0Configuration );
    }

    @Bean
    public Auth0SyncService auth0SyncService() {
        return new Auth0SyncService( hazelcastInstance, securePrincipalsManager(), organizationsManager() );
    }

    @Bean
    public UserListingService userListingService() {
        if ( auth0Configuration.getManagementApiUrl().contains( Auth0Configuration.NO_SYNC_URL ) ) {
            return new LocalUserListingService( auth0Configuration );
        }

        var auth0Token = auth0TokenProvider().getToken();
        return new Auth0UserListingService(
                new ManagementAPI( auth0Configuration.getDomain(), auth0Token ),
                new Auth0ApiExtension( auth0Configuration.getDomain(), auth0Token )
        );
    }

    @Bean
    public Auth0SyncTaskDependencies auth0SyncTaskDependencies() {
        return new Auth0SyncTaskDependencies( auth0SyncService(), userListingService(), executor );
    }

    @Bean( name = "auth0SyncTask" )
    @Profile( { ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE } )
    public Auth0SyncTask localAuth0SyncTask() {
        LoggerFactory.getLogger( ConductorServicesPod.class ).info( "Constructing local auth0sync task" );
        return new LocalAuth0SyncTask();
    }

    @Bean( name = "auth0SyncTask" )
    @Profile( {
            ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE,
            ConfigurationConstants.Profiles.AWS_TESTING_PROFILE,
            ConfigurationConstants.Profiles.KUBERNETES_CONFIGURATION_PROFILE
    } )
    public Auth0SyncTask defaultAuth0SyncTask() {
        LoggerFactory.getLogger( ConductorServicesPod.class ).info( "Constructing DEFAULT auth0sync task" );
        return new DefaultAuth0SyncTask();
    }

    @Bean( name = "auth0SyncInitializationTask" )
    @Profile( { ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE } )
    public Auth0SyncInitializationTask localAuth0SyncInitializationTask() {
        return new Auth0SyncInitializationTask<LocalAuth0SyncTask>( LocalAuth0SyncTask.class );
    }

    @Bean( name = "auth0SyncInitializationTask" )
    @Profile( {
            ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE,
            ConfigurationConstants.Profiles.AWS_TESTING_PROFILE,
            ConfigurationConstants.Profiles.KUBERNETES_CONFIGURATION_PROFILE
    } )
    public Auth0SyncInitializationTask defaultAuth0SyncInitializationTask() {
        return new Auth0SyncInitializationTask<DefaultAuth0SyncTask>( DefaultAuth0SyncTask.class );
    }

    @Bean
    public MailServiceClient mailServiceClient() {
        return new MailServiceClient( mailServiceRequirements().getEmailQueue() );
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
        return new HazelcastSchemaManager( hazelcastInstance, schemaQueryService() );
    }

    @Bean
    public PostgresTypeManager entityTypeManager() {
        return new PostgresTypeManager( hikariDataSource, hazelcastInstance );
    }

    @Bean
    public MailServiceRequirements mailServiceRequirements() {
        return () -> HazelcastQueue.EMAIL_SPOOL.getQueue( hazelcastInstance );
    }

    @Bean
    public PostgresEntityDataQueryService dataQueryService() {
        return new PostgresEntityDataQueryService(
                hikariDataSource,
                hikariDataSource,
                byteBlobDataManager,
                partitionManager()
        );
    }

    @Bean
    PartitionManager partitionManager() {
        return new PartitionManager( hazelcastInstance, hikariDataSource );
    }

    @Bean
    public EdmManager dataModelService() {
        return new EdmService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                entityTypeManager(),
                schemaManager()
        );
    }

    @Bean
    public DataGraphManager dataGraphService() {
        return new DataGraphService( graphService(), idService(), entityDatastore(), jobService() );
    }

    @Bean
    public OrganizationMetadataEntitySetsService organizationMetadataEntitySetsService() {
        return new OrganizationMetadataEntitySetsService(
                hazelcastInstance,
                dataModelService(),
                principalsMapManager(),
                authorizationManager()
        );
    }

    @Bean
    public EntitySetManager entitySetManager() {
        return new EntitySetService(
                hazelcastInstance,
                eventBus,
                aclKeyReservationService(),
                authorizationManager(),
                partitionManager(),
                dataModelService(),
                hikariDataSource,
                organizationMetadataEntitySetsService(),
                auditingConfiguration
        );
    }

    @Bean
    public GraphService graphService() {
        return new Graph( hikariDataSource,
                hikariDataSource,
                entitySetManager(),
                partitionManager(),
                dataQueryService(),
                idService(),
                metricRegistry );
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
    public EntityKeyIdService idService() {
        return new PostgresEntityKeyIdService(
                hikariDataSource,
                idGenerationService(),
                partitionManager() );
    }

    @Bean
    public IndexingMetadataManager indexingMetadataManager() {
        return new IndexingMetadataManager( hikariDataSource, partitionManager() );
    }

    @Bean
    public EdmAuthorizationHelper authorizingComponent() {
        return new EdmAuthorizationHelper( dataModelService(), authorizationManager(), entitySetManager() );
    }

    @Bean
    public AuditInitializationTask auditInitializationTask() {
        return new AuditInitializationTask( hazelcastInstance );
    }

    @Bean
    public PostgresEntitySetSizesTaskDependency postgresEntitySetSizesTaskDependency() {
        return new PostgresEntitySetSizesTaskDependency( hikariDataSource );
    }

    @Bean
    public PostgresEntitySetSizesInitializationTask postgresEntitySetSizesInitializationTask() {
        return new PostgresEntitySetSizesInitializationTask();
    }

    @Bean
    public LinkingQueryService lqs() {
        return new PostgresLinkingQueryService( hikariDataSource, partitionManager() );
    }

    @Bean
    public PostgresLinkingFeedbackService postgresLinkingFeedbackQueryService() {
        return new PostgresLinkingFeedbackService( hikariDataSource, hazelcastInstance );
    }

    @Bean
    public IdGenerationCatchupDependency idgenCatchupDependency() {
        return new IdGenerationCatchupDependency(
                HazelcastMap.ID_GENERATION.getMap( hazelcastClientProvider.getClient( HazelcastClient.IDS.name() ) ),
                hikariDataSource );
    }

    @Bean
    public IdGenerationCatchUpTask idgenCatchupTask() {
        return new IdGenerationCatchUpTask();
    }

    @Bean
    public PostgresMetaDataPropertiesInitializationDependency postgresMetaDataPropertiesInitializationDependency() {
        return new PostgresMetaDataPropertiesInitializationDependency( dataModelService() );
    }

    @Bean
    public PostgresMetaDataPropertiesInitializationTask postgresMetaDataPropertiesInitializationTask() {
        return new PostgresMetaDataPropertiesInitializationTask();
    }

    @Bean
    public GraphQueryService gqs() {
        return new PostgresGraphQueryService( hikariDataSource, entitySetManager(), dataQueryService() );
    }

    @Bean
    public SubscriptionService subscriptionService() {
        return new PostgresSubscriptionService( hikariDataSource, defaultObjectMapper() );
    }

    @Bean
    public SubscriptionNotificationDependencies subscriptionNotificationDependencies() {
        return new SubscriptionNotificationDependencies( hikariDataSource,
                securePrincipalsManager(),
                authorizationManager(),
                authorizingComponent(),
                mailServiceClient(),
                subscriptionService(),
                gqs(),
                HazelcastQueue.TWILIO_FEED.getQueue( hazelcastInstance )
        );
    }

    @Bean
    public ResumeJobsInitializationTask resumeJobsInitializationTask() {
        return new ResumeJobsInitializationTask();
    }

    @Bean
    public HazelcastJobService jobService() {
        return new HazelcastJobService( hazelcastInstance );
    }

    @Bean
    public ResumeJobDependencies resumeJobDependencies() {
        return new ResumeJobDependencies( jobService() );
    }

    @Bean
    public SubscriptionNotificationTask subscriptionNotificationTask() {
        return new SubscriptionNotificationTask();
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
    public ScheduledTaskServiceDependencies scheduledTaskServiceDependencies() {
        return new ScheduledTaskServiceDependencies( hazelcastInstance, executor );
    }

    @Bean
    public ScheduledTaskService scheduledTaskService() {
        return new ScheduledTaskService();
    }

    @PostConstruct
    void initPrincipals() {
        Principals.init( securePrincipalsManager(), hazelcastInstance );
        organizationMetadataEntitySetsService().dataGraphManager = dataGraphService();
    }
}
