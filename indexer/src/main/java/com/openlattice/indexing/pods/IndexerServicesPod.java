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

import com.codahale.metrics.MetricRegistry;
import com.geekbeast.mappers.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.geekbeast.hazelcast.HazelcastClientProvider;
import com.geekbeast.rhizome.jobs.HazelcastJobService;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.geekbeast.rhizome.configuration.ConfigurationConstants;
import com.openlattice.assembler.Assembler;
import com.openlattice.assembler.AssemblerConfiguration;
import com.openlattice.assembler.pods.AssemblerConfigurationPod;
import com.openlattice.auditing.AuditRecordEntitySetsManager;
import com.openlattice.auditing.AuditingConfiguration;
import com.openlattice.auditing.AuditingManager;
import com.openlattice.auditing.AuditingProfiles;
import com.openlattice.auditing.LocalAuditingService;
import com.openlattice.auditing.S3AuditingService;
import com.openlattice.auditing.pods.AuditingConfigurationPod;
import com.openlattice.authorization.*;
import com.openlattice.collaborations.CollaborationDatabaseManager;
import com.openlattice.collaborations.CollaborationService;
import com.openlattice.collaborations.PostgresCollaborationDatabaseService;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.data.DataDeletionManager;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DataGraphService;
import com.openlattice.data.EntityKeyIdService;
import com.openlattice.data.ids.PostgresEntityKeyIdService;
import com.openlattice.data.storage.ByteBlobDataManager;
import com.openlattice.data.storage.DataDeletionService;
import com.openlattice.data.storage.DataSourceResolver;
import com.openlattice.data.storage.EntityDatastore;
import com.openlattice.data.storage.postgres.PostgresEntityDataQueryService;
import com.openlattice.data.storage.postgres.PostgresEntityDatastore;
import com.openlattice.datasets.DataSetService;
import com.openlattice.datastore.pods.ByteBlobServicePod;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.EntitySetManager;
import com.openlattice.datastore.services.EntitySetService;
import com.openlattice.edm.properties.PostgresTypeManager;
import com.openlattice.edm.schemas.SchemaQueryService;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.graph.Graph;
import com.openlattice.graph.core.GraphService;
import com.openlattice.ids.HazelcastIdGenerationService;
import com.openlattice.ids.HazelcastLongIdService;
import com.openlattice.indexing.configuration.IndexerConfiguration;
import com.openlattice.jdbc.DataSourceManager;
import com.openlattice.linking.LinkingQueryService;
import com.openlattice.linking.PostgresLinkingFeedbackService;
import com.openlattice.linking.graph.PostgresLinkingQueryService;
import com.openlattice.notifications.sms.PhoneNumberService;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.OrganizationExternalDatabaseConfiguration;
import com.openlattice.organizations.pods.OrganizationExternalDatabaseConfigurationPod;
import com.openlattice.organizations.roles.HazelcastPrincipalService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.external.DatabaseQueryManager;
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager;
import com.openlattice.postgres.external.ExternalDatabasePermissioner;
import com.openlattice.postgres.external.ExternalDatabasePermissioningService;
import com.openlattice.postgres.external.PostgresDatabaseQueryService;
import com.openlattice.scrunchie.search.ConductorElasticsearchImpl;
import com.zaxxer.hikari.HikariDataSource;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

@Configuration
@Import( {
        IndexerConfigurationPod.class,
        AuditingConfigurationPod.class,
        AssemblerConfigurationPod.class,
        ByteBlobServicePod.class,
        OrganizationExternalDatabaseConfigurationPod.class
        // TransporterPod.class
} )
public class IndexerServicesPod {

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private HikariDataSource hikariDataSource;

    @Inject
    private EventBus eventBus;

    @Inject
    private IndexerConfiguration indexerConfiguration;

    @Inject
    private AuditingConfiguration auditingConfiguration;

    @Inject
    private AssemblerConfiguration assemblerConfiguration;

    @Inject
    private MetricRegistry metricRegistry;

    @Inject
    private HazelcastClientProvider hazelcastClientProvider;

    @Inject
    private ListeningExecutorService executor;

    @Inject
    private ByteBlobDataManager byteBlobDataManager;

    @Inject
    private OrganizationExternalDatabaseConfiguration organizationExternalDatabaseConfiguration;

    @Inject
    private ExternalDatabaseConnectionManager externalDbConnMan;

    @Inject
    private DataSourceManager dataSourceManager;

    @Bean
    public ConductorElasticsearchApi elasticsearchApi() {
        return new ConductorElasticsearchImpl( indexerConfiguration.getSearchConfiguration() );
    }

    @Bean
    public ObjectMapper defaultObjectMapper() {
        return ObjectMappers.getJsonMapper();
    }

    @Bean
    public DbCredentialService dbcs() {
        return new DbCredentialService( hazelcastInstance, longIdService() );
    }

    @Bean
    public HazelcastAclKeyReservationService aclKeyReservationService() {
        return new HazelcastAclKeyReservationService( hazelcastInstance );
    }

    @Bean
    public AuthorizationManager authorizationManager() {
        return new HazelcastAuthorizationService( hazelcastInstance, eventBus, principalsMapManager() );
    }

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
                dbcs(),
                principalsMapManager()
        );
    }

    @Bean
    public Assembler assembler() {
        return new Assembler(
                dbcs(),
                authorizationManager(),
                securePrincipalsManager(),
                dbQueryManager(),
                metricRegistry,
                hazelcastInstance,
                eventBus
        );
    }

    @Bean
    public DatabaseQueryManager dbQueryManager() {
        return new PostgresDatabaseQueryService(
                assemblerConfiguration,
                externalDbConnMan,
                securePrincipalsManager(),
                dbcs()
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
                dbcs(),
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
    public EdmAuthorizationHelper edmAuthorizationHelper() {
        return new EdmAuthorizationHelper( dataModelService(), authorizationManager(), entitySetManager() );
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
    public DataSetService dataSetService() {
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
    public HazelcastIdGenerationService idGeneration() {
        return new HazelcastIdGenerationService( hazelcastClientProvider );
    }

    @Bean
    public EntityKeyIdService idService() {
        return new PostgresEntityKeyIdService(
                dataSourceResolver(),
                idGeneration()
        );
    }

    @Bean
    public PostgresEntityDataQueryService dataQueryService() {
        return new PostgresEntityDataQueryService(
                dataSourceResolver(),
                byteBlobDataManager
        );
    }

    @Bean
    public EntityDatastore entityDatastore() {
        return new PostgresEntityDatastore(
                dataQueryService(),
                dataModelService(),
                entitySetManager(),
                metricRegistry,
                eventBus,
                postgresLinkingFeedbackService(),
                lqs()
        );
    }

    @Bean
    public GraphService graphApi() {
        return new Graph(
                dataSourceResolver(),
                entitySetManager(),
                dataQueryService(),
                idService(),
                metricRegistry
        );
    }

    @Bean
    public HazelcastLongIdService longIdService() {
        return new HazelcastLongIdService( hazelcastClientProvider );
    }

    @Bean
    public AuditRecordEntitySetsManager auditRecordEntitySetsManager() {
        return entitySetManager().getAuditRecordEntitySetsManager();
    }

    @Bean
    public HazelcastJobService jobService() {
        return new HazelcastJobService( hazelcastInstance );
    }

    @Bean
    public DataGraphManager dataGraphService() {
        return new DataGraphService( graphApi(), idService(), entityDatastore(), jobService() );
    }

    @Bean( name = "auditingManager" )
    @Profile( { ConfigurationConstants.Profiles.AWS_CONFIGURATION_PROFILE,
            ConfigurationConstants.Profiles.AWS_TESTING_PROFILE, AuditingProfiles.LOCAL_AWS_AUDITING_PROFILE } )
    public AuditingManager s3AuditingService() {
        return new S3AuditingService( auditingConfiguration, longIdService(), defaultObjectMapper() );
    }

    @Bean( name = "auditingManager" )
    @Profile( AuditingProfiles.LOCAL_AUDITING_PROFILE )
    public AuditingManager localAuditingService() {
        return new LocalAuditingService( dataGraphService(), auditRecordEntitySetsManager(), defaultObjectMapper() );
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
    public LinkingQueryService lqs() {
        return new PostgresLinkingQueryService( hikariDataSource );
    }

    @Bean
    public PostgresLinkingFeedbackService postgresLinkingFeedbackService() {
        return new PostgresLinkingFeedbackService( hikariDataSource, hazelcastInstance );
    }

    @Bean
    public DataSourceResolver dataSourceResolver() {
        dataSourceManager.registerTablesWithAllDatasources( PostgresTable.DATA );
        dataSourceManager.registerTablesWithAllDatasources( PostgresTable.E );
        dataSourceManager.registerTablesWithAllDatasources( PostgresTable.IDS );
        dataSourceManager.registerTablesWithAllDatasources( PostgresTable.SYNC_IDS );
        return new DataSourceResolver( hazelcastInstance, dataSourceManager );
    }

    @PostConstruct
    void initPrincipals() {
        Principals.init( securePrincipalsManager(), hazelcastInstance );
    }
}
