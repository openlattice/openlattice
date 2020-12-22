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
import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.geekbeast.hazelcast.HazelcastClientProvider;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.openlattice.assembler.Assembler;
import com.openlattice.assembler.AssemblerConfiguration;
import com.openlattice.assembler.AssemblerConnectionManager;
import com.openlattice.assembler.pods.AssemblerConfigurationPod;
import com.openlattice.auditing.AuditingConfiguration;
import com.openlattice.auditing.pods.AuditingConfigurationPod;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.DbCredentialService;
import com.openlattice.authorization.HazelcastAclKeyReservationService;
import com.openlattice.authorization.HazelcastSecurableObjectResolveTypeService;
import com.openlattice.authorization.Principals;
import com.openlattice.authorization.SecurableObjectResolveTypeService;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.data.storage.partitions.PartitionManager;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.EntitySetManager;
import com.openlattice.datastore.services.EntitySetService;
import com.openlattice.edm.properties.PostgresTypeManager;
import com.openlattice.edm.schemas.SchemaQueryService;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.ids.HazelcastLongIdService;
import com.openlattice.linking.LinkingConfiguration;
import com.openlattice.linking.Matcher;
import com.openlattice.linking.PostgresLinkingFeedbackService;
import com.openlattice.linking.matching.SocratesMatcher;
import com.openlattice.linking.util.PersonProperties;
import com.openlattice.notifications.sms.PhoneNumberService;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.OrganizationMetadataEntitySetsService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.postgres.external.ExternalDatabaseConnectionManager;
import com.openlattice.postgres.external.ExternalDatabasePermissionsManager;
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

import static com.openlattice.linking.MatcherKt.DL4J;
import static com.openlattice.linking.MatcherKt.KERAS;

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

    @Inject
    private ExternalDatabasePermissionsManager extDbPermManager;

    @Inject
    public SecurePrincipalsManager principalService;

    @Inject
    public HazelcastAclKeyReservationService aclKeyReservationService;

    @Inject
    public AuthorizationManager authorizationManager;

    @Bean
    public PartitionManager partitionManager() {
        return new PartitionManager( hazelcastInstance, hikariDataSource );
    }

    @Bean
    public ConductorElasticsearchApi elasticsearchApi() throws IOException {
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
                metricRegistry
        );
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
    public OrganizationMetadataEntitySetsService organizationMetadataEntitySetsService() {
        return new OrganizationMetadataEntitySetsService( dataModelService() );
    }

    @Bean
    public PostgresLinkingFeedbackService postgresLinkingFeedbackQueryService() {
        return new PostgresLinkingFeedbackService( hikariDataSource, hazelcastInstance );
    }

    @PostConstruct
    void initPrincipals() {
        Principals.init( principalService, hazelcastInstance );
    }
}
