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
import com.kryptnostic.rhizome.pods.ConfigurationLoader;
import com.openlattice.assembler.Assembler;
import com.openlattice.assembler.Assembler.EntitySetViewsInitializerTask;
import com.openlattice.assembler.Assembler.OrganizationAssembliesInitializerTask;
import com.openlattice.assembler.AssemblerConfiguration;
import com.openlattice.assembler.AssemblerConnectionManager;
import com.openlattice.assembler.AssemblerDependencies;
import com.openlattice.assembler.MaterializedEntitySetsDependencies;
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
import com.openlattice.collections.CollectionsManager;
import com.openlattice.conductor.rpc.ConductorConfiguration;
import com.openlattice.conductor.rpc.MapboxConfiguration;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DataGraphService;
import com.openlattice.data.EntityKeyIdService;
import com.openlattice.data.ids.PostgresEntityKeyIdService;
import com.openlattice.data.storage.*;
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
import com.openlattice.edm.schemas.postgres.PostgresSchemaQueryService;
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
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager;
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
import com.openlattice.users.*;
import com.openlattice.users.export.Auth0ApiExtension;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

@Configuration
@Import( { ByteBlobServicePod.class, AuditingConfigurationPod.class, AssemblerConfigurationPod.class } )
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
    public HazelcastLongIdService longIdService() {
        return new HazelcastLongIdService( hazelcastClientProvider );
    }

    @Bean
    public DbCredentialService dbCredService() {
        return new DbCredentialService( hazelcastInstance, longIdService() );
    }

    @Bean
    public HazelcastAclKeyReservationService aclKeyReservationService() {
        return new HazelcastAclKeyReservationService( hazelcastInstance );
    }

    @Bean
    public SecurePrincipalsManager principalService() {
        return new HazelcastPrincipalService( hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                eventBus );
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
                hikariDataSource,
                authorizationManager(),
                principalService(),
                metricRegistry,
                hazelcastInstance,
                eventBus
        );
    }

    @Bean
    public OrganizationsInitializationDependencies organizationBootstrapDependencies() {
        return new OrganizationsInitializationDependencies( organizationsManager(),
                principalService(),
                partitionManager(),
                conductorConfiguration() );
    }

    @Bean
    public AuthorizationInitializationDependencies authorizationBootstrapDependencies() {
        return new AuthorizationInitializationDependencies( principalService() );
    }

    @Bean
    public AssemblerDependencies assemblerDependencies() {
        return new AssemblerDependencies( hikariDataSource,
                dbCredService(),
                externalDbConnMan,
                assemblerConnectionManager() );
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
                principalService(),
                entitySetManager(),
                authorizationManager(),
                partitionManager(),
                organizationsManager()
        );
    }

    @Bean
    public AssemblerConnectionManager assemblerConnectionManager() {
        return new AssemblerConnectionManager( assemblerConfiguration,
                externalDbConnMan,
                hikariDataSource,
                principalService(),
                organizationsManager(),
                dbCredService(),
                eventBus,
                metricRegistry );
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
                assembler(),
                organizationMetadataEntitySetsService() );
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
        return new Auth0SyncService( hazelcastInstance, principalService(), organizationsManager() );
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

    @Bean
    public Auth0SyncTask auth0SyncTask() {
        return new Auth0SyncTask();
    }

    @Bean
    public Auth0SyncInitializationTask auth0SyncInitializationTask() {
        return new Auth0SyncInitializationTask();
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
        return new PostgresSchemaQueryService( hikariDataSource );
    }

    @Bean
    public HazelcastSchemaManager schemaManager() {
        return new HazelcastSchemaManager( hazelcastInstance, schemaQueryService() );
    }

    @Bean
    public PostgresTypeManager entityTypeManager() {
        return new PostgresTypeManager( hikariDataSource );
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
        return new OrganizationMetadataEntitySetsService( dataModelService() );
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
    public HazelcastIdGenerationService idGenerationService() {
        return new HazelcastIdGenerationService( hazelcastClientProvider );
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
                principalService(),
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
        Principals.init( principalService(), hazelcastInstance );
        organizationMetadataEntitySetsService().dataGraphManager = dataGraphService();
    }
}
