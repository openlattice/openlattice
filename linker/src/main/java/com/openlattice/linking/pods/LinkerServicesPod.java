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

package com.openlattice.linking.pods;

import com.codahale.metrics.MetricRegistry;
import com.geekbeast.mappers.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.geekbeast.hazelcast.HazelcastClientProvider;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.openlattice.assembler.Assembler;
import com.openlattice.assembler.AssemblerConfiguration;
import com.openlattice.assembler.pods.AssemblerConfigurationPod;
import com.openlattice.auditing.AuditingConfiguration;
import com.openlattice.auditing.pods.AuditingConfigurationPod;
import com.openlattice.authorization.*;
import com.openlattice.collaborations.CollaborationDatabaseManager;
import com.openlattice.collaborations.CollaborationService;
import com.openlattice.collaborations.PostgresCollaborationDatabaseService;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.datasets.DataSetService;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.EntitySetManager;
import com.openlattice.datastore.services.EntitySetService;
import com.openlattice.edm.properties.PostgresTypeManager;
import com.openlattice.edm.schemas.SchemaQueryService;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.ids.HazelcastLongIdService;
import com.openlattice.linking.LinkingConfiguration;
import com.openlattice.linking.LinkingLogService;
import com.openlattice.linking.PostgresLinkingFeedbackService;
import com.openlattice.linking.PostgresLinkingLogService;
import com.openlattice.linking.matching.Matcher;
import com.openlattice.linking.matching.SocratesMatcher;
import com.openlattice.linking.util.PersonProperties;
import com.openlattice.notifications.sms.PhoneNumberService;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.roles.HazelcastPrincipalService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.postgres.external.DatabaseQueryManager;
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager;
import com.openlattice.postgres.external.ExternalDatabasePermissioner;
import com.openlattice.postgres.external.ExternalDatabasePermissioningService;
import com.openlattice.postgres.external.PostgresDatabaseQueryService;
import com.openlattice.scrunchie.search.ConductorElasticsearchImpl;
import com.zaxxer.hikari.HikariDataSource;
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport;
import org.deeplearning4j.nn.modelimport.keras.exceptions.InvalidKerasConfigurationException;
import org.deeplearning4j.nn.modelimport.keras.exceptions.UnsupportedKerasConfigurationException;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.util.ModelSerializer;
import org.nd4j.linalg.io.ClassPathResource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.IOException;

import static com.openlattice.linking.matching.Matcher.DL4J;
import static com.openlattice.linking.matching.Matcher.KERAS;

@Configuration
@Import( { LinkerConfigurationPod.class, AuditingConfigurationPod.class, AssemblerConfigurationPod.class } )
public class LinkerServicesPod {

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private HikariDataSource hikariDataSource;

    @Inject
    private EventBus eventBus;

    @Inject
    private LinkingConfiguration linkingConfiguration;

    @Inject
    private AuditingConfiguration auditingConfiguration;

    @Inject
    private AssemblerConfiguration assemblerConfiguration;

    @Inject
    private HazelcastClientProvider hazelcastClientProvider;

    @Inject
    private MetricRegistry metricRegistry;

    @Inject
    private ExternalDatabaseConnectionManager externalDbConnMan;

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
    public SecurePrincipalsManager principalService() {
        return new HazelcastPrincipalService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                principalsMapManager(),
                externalDatabasePermissionsManager()
        );
    }

    @Bean
    public ConductorElasticsearchApi elasticsearchApi() {
        return new ConductorElasticsearchImpl( linkingConfiguration.getSearchConfiguration() );
    }

    @Bean
    public ObjectMapper defaultObjectMapper() {
        return ObjectMappers.getJsonMapper();
    }

    @Bean
    public HazelcastLongIdService longIdService() {
        return new HazelcastLongIdService( hazelcastClientProvider );
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
    public Assembler assembler() {
        return new Assembler(
                dbcs(),
                authorizationManager(),
                principalService(),
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
                principalService(),
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
                principalService(),
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
                principalService(),
                collaborationDatabaseManager()
        );
    }

    @Bean
    public HazelcastOrganizationService organizationsManager() {
        return new HazelcastOrganizationService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                principalService(),
                phoneNumberService(),
                assembler(),
                collaborationService()
        );
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
    public LinkingLogService linkingLogService() {
        return new PostgresLinkingLogService(
                hikariDataSource,
                defaultObjectMapper()
        );
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
    @Profile( DL4J )
    public Matcher dl4jMatcher() throws IOException {
        var modelStream = Thread.currentThread().getContextClassLoader().getResourceAsStream( "model.bin" );
        var fqnToIdMap = dataModelService().getFqnToIdMap( PersonProperties.FQNS );
        return new SocratesMatcher(
                ModelSerializer.restoreMultiLayerNetwork( modelStream ),
                fqnToIdMap,
                postgresLinkingFeedbackQueryService() );
    }

    @Profile( KERAS )
    @Bean
    public Matcher kerasMatcher() throws IOException, InvalidKerasConfigurationException,
            UnsupportedKerasConfigurationException {
        String simpleMlp = new ClassPathResource( "model_2019-01-30.h5" ).getFile().getPath();
        MultiLayerNetwork model = KerasModelImport.importKerasSequentialModelAndWeights( simpleMlp );
        var fqnToIdMap = dataModelService().getFqnToIdMap( PersonProperties.FQNS );
        return new SocratesMatcher( model, fqnToIdMap, postgresLinkingFeedbackQueryService() );
    }

    @Bean
    public PostgresLinkingFeedbackService postgresLinkingFeedbackQueryService() {
        return new PostgresLinkingFeedbackService( hikariDataSource, hazelcastInstance );
    }

    @Bean
    public PrincipalsMapManager principalsMapManager() {
        return new HazelcastPrincipalsMapManager( hazelcastInstance, aclKeyReservationService() );
    }

    @PostConstruct
    void initPrincipals() {
        Principals.init( principalService(), hazelcastInstance );
    }
}
