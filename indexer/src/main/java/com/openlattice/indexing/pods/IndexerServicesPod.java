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
import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.geekbeast.hazelcast.HazelcastClientProvider;
import com.geekbeast.rhizome.jobs.HazelcastJobService;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.rhizome.configuration.ConfigurationConstants;
import com.openlattice.assembler.Assembler;
import com.openlattice.assembler.AssemblerConfiguration;
import com.openlattice.assembler.AssemblerConnectionManager;
import com.openlattice.assembler.pods.AssemblerConfigurationPod;
import com.openlattice.auditing.*;
import com.openlattice.auditing.pods.AuditingConfigurationPod;
import com.openlattice.authorization.*;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.data.DataDeletionManager;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DataGraphService;
import com.openlattice.data.EntityKeyIdService;
import com.openlattice.data.ids.PostgresEntityKeyIdService;
import com.openlattice.data.storage.ByteBlobDataManager;
import com.openlattice.data.storage.DataDeletionService;
import com.openlattice.data.storage.EntityDatastore;
import com.openlattice.data.storage.PostgresEntityDataQueryService;
import com.openlattice.data.storage.PostgresEntityDatastore;
import com.openlattice.data.storage.partitions.PartitionManager;
import com.openlattice.datastore.pods.ByteBlobServicePod;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.EntitySetManager;
import com.openlattice.datastore.services.EntitySetService;
import com.openlattice.edm.properties.PostgresTypeManager;
import com.openlattice.edm.schemas.SchemaQueryService;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.edm.schemas.postgres.PostgresSchemaQueryService;
import com.openlattice.graph.Graph;
import com.openlattice.graph.core.GraphService;
import com.openlattice.ids.HazelcastIdGenerationService;
import com.openlattice.ids.HazelcastLongIdService;
import com.openlattice.indexing.configuration.IndexerConfiguration;
import com.openlattice.linking.LinkingQueryService;
import com.openlattice.linking.PostgresLinkingFeedbackService;
import com.openlattice.linking.graph.PostgresLinkingQueryService;
import com.openlattice.notifications.sms.PhoneNumberService;
import com.openlattice.organizations.ExternalDatabaseManagementService;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.OrganizationExternalDatabaseConfiguration;
import com.openlattice.organizations.OrganizationMetadataEntitySetsService;
import com.openlattice.organizations.pods.OrganizationExternalDatabaseConfigurationPod;
import com.openlattice.organizations.roles.HazelcastPrincipalService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager;
import com.openlattice.postgres.external.ExternalDatabasePermissionsManager;
import com.openlattice.scrunchie.search.ConductorElasticsearchImpl;
import com.openlattice.transporter.types.TransporterDatastore;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

@Configuration
@Import( { IndexerConfigurationPod.class, AuditingConfigurationPod.class, AssemblerConfigurationPod.class,
        ByteBlobServicePod.class, OrganizationExternalDatabaseConfigurationPod.class } )
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
    private ExternalDatabasePermissionsManager extDbPermManager;

    @Inject
    private TransporterDatastore transporterDatastore;

    @Inject
    public SecurePrincipalsManager principalService;

    @Inject
    public HazelcastAclKeyReservationService aclKeyReservationService;

    @Inject
    public AuthorizationManager authorizationManager;

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
    public Assembler assembler() {
        return new Assembler(
                dbcs(),
                hikariDataSource,
                authorizationManager,
                principalService,
                metricRegistry,
                hazelcastInstance,
                eventBus
        );
    }

    @Bean
    public AssemblerConnectionManager assemblerConnectionManager() {
        return new AssemblerConnectionManager(
                assemblerConfiguration,
                externalDbConnMan,
                principalService,
                organizationsManager(),
                dbcs(),
                extDbPermManager,
                eventBus,
                metricRegistry
        );
    }

    @Bean
    public PartitionManager partitionManager() {
        return new PartitionManager( hazelcastInstance, hikariDataSource );
    }

    @Bean
    public PhoneNumberService phoneNumberService() {
        return new PhoneNumberService( hazelcastInstance );
    }

    @Bean
    public HazelcastOrganizationService organizationsManager() {
        return new HazelcastOrganizationService(
                hazelcastInstance,
                aclKeyReservationService,
                authorizationManager,
                principalService,
                phoneNumberService(),
                partitionManager(),
                assembler(),
                organizationMetadataEntitySetsService() );
    }

    @Bean
    public EdmAuthorizationHelper edmAuthorizationHelper() {
        return new EdmAuthorizationHelper( dataModelService(), authorizationManager, entitySetManager() );
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
        return new PostgresTypeManager( hikariDataSource , hazelcastInstance );
    }

    @Bean
    public EdmManager dataModelService() {
        return new EdmService(
                hazelcastInstance,
                aclKeyReservationService,
                authorizationManager,
                entityTypeManager(),
                schemaManager()
        );
    }

    @Bean
    public EntitySetManager entitySetManager() {
        return new EntitySetService(
                hazelcastInstance,
                eventBus,
                aclKeyReservationService,
                authorizationManager,
                partitionManager(),
                dataModelService(),
                hikariDataSource,
                organizationMetadataEntitySetsService(),
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
                hikariDataSource,
                idGeneration(),
                partitionManager() );
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
        return new Graph( hikariDataSource,
                hikariDataSource,
                entitySetManager(),
                partitionManager(),
                dataQueryService(),
                idService(),
                metricRegistry );
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

    @Bean
    public ExternalDatabaseManagementService edms() {
        return new ExternalDatabaseManagementService(
                hazelcastInstance,
                externalDbConnMan,
                principalService,
                aclKeyReservationService,
                authorizationManager,
                organizationExternalDatabaseConfiguration,
                transporterDatastore,
                dbcs(),
                hikariDataSource );
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
                dataModelService(),
                entitySetManager(),
                dataGraphService(),
                authorizationManager,
                auditRecordEntitySetsManager(),
                entityDatastore(),
                graphApi()
        );
    }

    @Bean
    public LinkingQueryService lqs() {
        return new PostgresLinkingQueryService( hikariDataSource, partitionManager() );
    }

    @Bean
    public PostgresLinkingFeedbackService postgresLinkingFeedbackService() {
        return new PostgresLinkingFeedbackService( hikariDataSource, hazelcastInstance );
    }
    @Bean
    public OrganizationMetadataEntitySetsService organizationMetadataEntitySetsService() {
        return new OrganizationMetadataEntitySetsService( dataModelService() );
    }

    @PostConstruct
    void initPrincipals() {
        Principals.init( principalService, hazelcastInstance );
    }
}
