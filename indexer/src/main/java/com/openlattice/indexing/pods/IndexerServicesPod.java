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
import com.openlattice.auth0.Auth0TokenProvider;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.authorization.*;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.data.DataDeletionManager;
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
import com.openlattice.directory.UserDirectoryService;
import com.openlattice.edm.PostgresEdmManager;
import com.openlattice.edm.properties.PostgresTypeManager;
import com.openlattice.edm.schemas.SchemaQueryService;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.edm.schemas.postgres.PostgresSchemaQueryService;
import com.openlattice.graph.Graph;
import com.openlattice.graph.core.GraphService;
import com.openlattice.hazelcast.HazelcastQueue;
import com.openlattice.ids.HazelcastIdGenerationService;
import com.openlattice.ids.HazelcastLongIdService;
import com.openlattice.indexing.configuration.IndexerConfiguration;
import com.openlattice.scrunchie.search.ConductorElasticsearchImpl;
import com.openlattice.mail.config.MailServiceRequirements;
import com.openlattice.notifications.sms.PhoneNumberService;
import com.openlattice.organizations.ExternalDatabaseManagementService;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.OrganizationExternalDatabaseConfiguration;
import com.openlattice.organizations.pods.OrganizationExternalDatabaseConfigurationPod;
import com.openlattice.organizations.roles.HazelcastPrincipalService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.postgres.PostgresTableManager;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static Logger logger = LoggerFactory.getLogger( IndexerServicesPod.class );

    @Inject
    private PostgresTableManager tableManager;

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private Auth0Configuration auth0Configuration;

    @Inject
    private HikariDataSource hikariDataSource;

    @Inject
    private PostgresUserApi pgUserApi;

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
        return new DbCredentialService( hazelcastInstance, pgUserApi );
    }

    @Bean
    public AuthorizationQueryService authorizationQueryService() {
        return new AuthorizationQueryService( hikariDataSource, hazelcastInstance );
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
        return new HazelcastAuthorizationService( hazelcastInstance, authorizationQueryService(), eventBus );
    }

    @Bean
    public Assembler assembler() {
        return new Assembler(
                dbcs(),
                hikariDataSource,
                authorizationManager(),
                edmAuthorizationHelper(),
                principalService(),
                metricRegistry,
                hazelcastInstance,
                eventBus
        );
    }

    @Bean
    public AssemblerConnectionManager assemblerConnectionManager() {
        return new AssemblerConnectionManager(
                assemblerConfiguration,
                hikariDataSource,
                principalService(),
                organizationsManager(),
                dbcs(),
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
                aclKeyReservationService(),
                authorizationManager(),
                principalService(),
                phoneNumberService(),
                partitionManager(),
                assembler() );
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
        return new PostgresSchemaQueryService( hikariDataSource );
    }

    @Bean
    public PostgresEdmManager edmManager() {
        return new PostgresEdmManager( hikariDataSource, hazelcastInstance );
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
    public EdmManager dataModelService() {
        return new EdmService(
                hikariDataSource,
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                edmManager(),
                entityTypeManager(),
                schemaManager()
        );
    }

    @Bean
    public EntitySetManager entitySetManager() {
        return new EntitySetService(
                hazelcastInstance,
                eventBus,
                edmManager(),
                aclKeyReservationService(),
                authorizationManager(),
                partitionManager(),
                dataModelService(),
                auditingConfiguration
        );
    }

    @Bean
    public HazelcastIdGenerationService idGeneration() {
        return new HazelcastIdGenerationService( hazelcastClientProvider, executor );
    }

    @Bean
    public EntityKeyIdService idService() {
        return new PostgresEntityKeyIdService( hazelcastClientProvider,
                executor,
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
        return new PostgresEntityDatastore( dataQueryService(), edmManager(), entitySetManager(), metricRegistry );
    }

    @Bean
    public PostgresEntitySetSizesTask postgresEntitySetSizeCacheManager() {
        return new PostgresEntitySetSizesTask();
    }

    @Bean
    public GraphService graphApi() {
        return new Graph( hikariDataSource, entitySetManager(), partitionManager() );
    }

    @Bean
    public HazelcastLongIdService longIdService() {
        return new HazelcastLongIdService( hazelcastClientProvider, hazelcastInstance );
    }

    @Bean
    public AuditRecordEntitySetsManager auditRecordEntitySetsManager() {
        return entitySetManager().getAuditRecordEntitySetsManager();
    }

    @Bean
    public DataGraphManager dataGraphService() {
        return new DataGraphService( graphApi(), idService(), entityDatastore(), postgresEntitySetSizeCacheManager() );
    }

    @Bean ExternalDatabaseManagementService edms() {
        return new ExternalDatabaseManagementService(
                hazelcastInstance,
                assemblerConnectionManager(),
                principalService(),
                aclKeyReservationService(),
                authorizationManager(),
                organizationExternalDatabaseConfiguration,
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
                authorizationManager(),
                auditRecordEntitySetsManager(),
                entityDatastore(),
                graphApi()
        );
    }

    @PostConstruct
    void initPrincipals() {
        Principals.init( principalService(), hazelcastInstance );
    }
}
