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

import static com.openlattice.linking.MatcherKt.DL4J;
import static com.openlattice.linking.MatcherKt.KERAS;

import com.codahale.metrics.MetricRegistry;
import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.openlattice.assembler.Assembler;
import com.openlattice.assembler.AssemblerConfiguration;
import com.openlattice.assembler.AssemblerConnectionManager;
import com.openlattice.assembler.pods.AssemblerConfigurationPod;
import com.openlattice.auditing.AuditingConfiguration;
import com.openlattice.auditing.pods.AuditingConfigurationPod;
import com.openlattice.auth0.Auth0TokenProvider;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.authorization.SecurableObjectResolveTypeService;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizationQueryService;
import com.openlattice.authorization.DbCredentialService;
import com.openlattice.authorization.HazelcastSecurableObjectResolveTypeService;
import com.openlattice.authorization.HazelcastAclKeyReservationService;
import com.openlattice.authorization.HazelcastAuthorizationService;
import com.openlattice.authorization.PostgresUserApi;
import com.openlattice.authorization.Principals;
import com.openlattice.conductor.rpc.ConductorConfiguration;
import com.openlattice.conductor.rpc.ConductorElasticsearchApi;
import com.openlattice.datastore.services.EdmManager;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.directory.UserDirectoryService;
import com.openlattice.edm.PostgresEdmManager;
import com.openlattice.edm.properties.PostgresTypeManager;
import com.openlattice.edm.schemas.SchemaQueryService;
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager;
import com.openlattice.edm.schemas.postgres.PostgresSchemaQueryService;
import com.openlattice.hazelcast.HazelcastQueue;
import com.openlattice.kindling.search.ConductorElasticsearchImpl;
import com.openlattice.linking.Matcher;
import com.openlattice.linking.matching.SocratesMatcher;
import com.openlattice.linking.util.PersonProperties;
import com.openlattice.mail.config.MailServiceRequirements;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.roles.HazelcastPrincipalService;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.postgres.PostgresTableManager;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport;
import org.deeplearning4j.nn.modelimport.keras.exceptions.InvalidKerasConfigurationException;
import org.deeplearning4j.nn.modelimport.keras.exceptions.UnsupportedKerasConfigurationException;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.util.ModelSerializer;
import org.nd4j.linalg.io.ClassPathResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

@Configuration
@Import( { LinkerConfigurationPod.class, AuditingConfigurationPod.class, AssemblerConfigurationPod.class } )
public class LinkerServicesPod {
    private static Logger logger = LoggerFactory.getLogger( LinkerServicesPod.class );

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
    private ConductorConfiguration conductorConfiguration;

    @Inject
    private AuditingConfiguration auditingConfiguration;

    @Inject
    private AssemblerConfiguration assemblerConfiguration;

    @Inject
    private MetricRegistry metricRegistry;

    @Bean
    public ConductorElasticsearchApi elasticsearchApi() throws IOException {
        return new ConductorElasticsearchImpl( conductorConfiguration.getSearchConfiguration() );
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
    public UserDirectoryService userDirectoryService() {
        return new UserDirectoryService( auth0TokenProvider(), hazelcastInstance );
    }

    @Bean
    public Assembler assembler() {
        return new Assembler( authorizationManager(),
                dbcs(),
                hikariDataSource,
                metricRegistry,
                hazelcastInstance,
                eventBus );
    }

    @Bean
    public AssemblerConnectionManager assemblerConnectionManager() {
        return new AssemblerConnectionManager( assemblerConfiguration,
                hikariDataSource,
                principalService(),
                organizationsManager(),
                dbcs(),
                hazelcastInstance,
                eventBus,
                metricRegistry );
    }

    @Bean
    public HazelcastOrganizationService organizationsManager() {
        return new HazelcastOrganizationService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                userDirectoryService(),
                principalService(),
                assembler() );
    }

    @Bean
    public Auth0TokenProvider auth0TokenProvider() {
        return new Auth0TokenProvider( auth0Configuration );
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
        return new PostgresEdmManager( hikariDataSource, tableManager, hazelcastInstance );
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
        return () -> hazelcastInstance.getQueue( HazelcastQueue.EMAIL_SPOOL.name() );
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
                schemaManager(),
                auditingConfiguration );
    }

    @Bean
    @Profile( DL4J )
    public Matcher dl4jMatcher() throws IOException {
        final var modelStream = Thread.currentThread().getContextClassLoader().getResourceAsStream( "model.bin" );
        final var fqnToIdMap = dataModelService().getFqnToIdMap( PersonProperties.FQNS );
        return new SocratesMatcher( ModelSerializer.restoreMultiLayerNetwork( modelStream ), fqnToIdMap );
    }

    @Profile( KERAS )
    @Bean
    public Matcher kerasMatcher() throws IOException, InvalidKerasConfigurationException,
            UnsupportedKerasConfigurationException {
        final String simpleMlp = new ClassPathResource( "model_2019-01-30.h5" ).getFile().getPath();
        final MultiLayerNetwork model = KerasModelImport.importKerasSequentialModelAndWeights( simpleMlp );
        final var fqnToIdMap = dataModelService().getFqnToIdMap( PersonProperties.FQNS );
        return new SocratesMatcher( model, fqnToIdMap );
    }

    @PostConstruct
    void initPrincipals() {
        Principals.init( principalService() );
    }
}
