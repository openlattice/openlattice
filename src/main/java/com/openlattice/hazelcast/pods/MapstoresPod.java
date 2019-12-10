

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

package com.openlattice.hazelcast.pods;

import com.auth0.json.mgmt.users.User;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.kryptnostic.rhizome.pods.hazelcast.QueueConfigurer;
import com.openlattice.apps.App;
import com.openlattice.apps.AppConfigKey;
import com.openlattice.apps.AppType;
import com.openlattice.apps.AppTypeSetting;
import com.openlattice.assembler.EntitySetAssemblyKey;
import com.openlattice.assembler.MaterializedEntitySet;
import com.openlattice.assembler.OrganizationAssembly;
import com.openlattice.auditing.AuditRecordEntitySetConfiguration;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.auth0.Auth0TokenProvider;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.AceValue;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.PostgresUserApi;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.authorization.mapstores.PermissionMapstore;
import com.openlattice.authorization.mapstores.PostgresCredentialMapstore;
import com.openlattice.authorization.mapstores.PrincipalMapstore;
import com.openlattice.authorization.mapstores.PrincipalTreesMapstore;
import com.openlattice.authorization.mapstores.UserMapstore;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.collections.CollectionTemplateKey;
import com.openlattice.collections.EntitySetCollection;
import com.openlattice.collections.EntityTypeCollection;
import com.openlattice.collections.mapstores.EntitySetCollectionConfigMapstore;
import com.openlattice.collections.mapstores.EntitySetCollectionMapstore;
import com.openlattice.collections.mapstores.EntityTypeCollectionMapstore;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.set.EntitySetPropertyKey;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.hazelcast.HazelcastQueue;
import com.openlattice.ids.HazelcastIdGenerationService;
import com.openlattice.ids.IdGenerationMapstore;
import com.openlattice.ids.Range;
import com.openlattice.linking.mapstores.LinkingFeedbackMapstore;
import com.openlattice.notifications.sms.SmsInformationMapstore;
import com.openlattice.organization.OrganizationExternalDatabaseColumn;
import com.openlattice.organization.OrganizationExternalDatabaseTable;
import com.openlattice.organizations.Organization;
import com.openlattice.organizations.mapstores.OrganizationExternalDatabaseColumnMapstore;
import com.openlattice.organizations.mapstores.OrganizationExternalDatabaseTableMapstore;
import com.openlattice.organizations.mapstores.OrganizationsMapstore;
import com.openlattice.postgres.PostgresPod;
import com.openlattice.postgres.PostgresTableManager;
import com.openlattice.postgres.mapstores.*;
import com.openlattice.requests.Status;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.zaxxer.hikari.HikariDataSource;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.inject.Inject;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

@Configuration
@Import( { PostgresPod.class, Auth0Pod.class } )
public class MapstoresPod {
    private static final Logger           logger = LoggerFactory.getLogger( MapstoresPod.class );
    @Inject
    private              HikariDataSource hikariDataSource;

    @Inject
    private PostgresTableManager ptMgr;

    @Inject
    private Auth0Configuration auth0Configuration;

    @Inject
    private Jdbi jdbi;

    @Bean
    public PostgresUserApi pgUserApi() {
        try ( Connection conn = hikariDataSource.getConnection(); Statement stmt = conn.createStatement() ) {
            String createUserSql = Resources.toString( Resources.getResource( "create_user.sql" ), Charsets.UTF_8 );
            String alterUserSql = Resources.toString( Resources.getResource( "alter_user.sql" ), Charsets.UTF_8 );
            String deleteUserSql = Resources.toString( Resources.getResource( "delete_user.sql" ), Charsets.UTF_8 );
            stmt.addBatch( createUserSql );
            stmt.addBatch( alterUserSql );
            stmt.addBatch( deleteUserSql );
            stmt.executeBatch();
        } catch ( SQLException | IOException e ) {
            logger.error( "Unable to configure postgres functions for user management." );
        }

        return jdbi.onDemand( PostgresUserApi.class );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, OrganizationAssembly> organizationAssemblies() {
        return new OrganizationAssemblyMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<EntitySetAssemblyKey, MaterializedEntitySet> materializedEntitySetsMapStore() {
        return new MaterializedEntitySetMapStore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<AceKey, AceValue> permissionMapstore() {
        return new PermissionMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<AclKey, SecurableObjectType> securableObjectTypeMapstore() {
        return new SecurableObjectTypeMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, PropertyType> propertyTypeMapstore() {
        return new com.openlattice.postgres.mapstores.PropertyTypeMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, EntityType> entityTypeMapstore() {
        return new EntityTypeMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<AclKey, AuditRecordEntitySetConfiguration> auditRecordEntitySetConfigurationMapstore() {
        return new AuditRecordEntitySetConfigurationMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, EntitySet> entitySetMapstore() {
        return new EntitySetMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<String, DelegatedStringSet> schemaMapstore() {
        return new SchemasMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<String, UUID> aclKeysMapstore() {
        return new AclKeysMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, String> namesMapstore() {
        return new NamesMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<AceKey, Status> requestMapstore() {
        return new RequestsMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, AssociationType> edgeTypeMapstore() {
        return new AssociationTypeMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<AclKey, SecurablePrincipal> principalsMapstore() {
        return new PrincipalMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<String, String> dbCredentialsMapstore() {
        return new PostgresCredentialMapstore( hikariDataSource, pgUserApi() );
    }

    @Bean
    public SelfRegisteringMapStore<String, User> userMapstore() {
        return new UserMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, Organization> organizationsMapstore() {
        return new OrganizationsMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<EntitySetPropertyKey, EntitySetPropertyMetadata> entitySetPropertyMetadataMapstore() {
        return new EntitySetPropertyMetadataMapstore( hikariDataSource );
    }

    @Bean
    public QueueConfigurer defaultQueueConfigurer() {
        return config -> config.setMaxSize( 10_000 ).setEmptyQueueTtl( 60 );
    }

    @Bean
    public QueueConfigurer idGenerationQueueConfigurer() {
        return config -> config.setName( HazelcastQueue.ID_GENERATION.name() )
                .setMaxSize( (int) ( HazelcastIdGenerationService.NUM_PARTITIONS * 3 ) )
                .setBackupCount( 1 );
    }

    @Bean
    public QueueConfigurer twilioQueueConfigurer() {
        return config -> config.setName( HazelcastQueue.TWILIO.name() ).setMaxSize( 100_000 ).setBackupCount( 1 );
    }

    @Bean
    public QueueConfigurer indexingQueueConfigurer() {
        return config -> config.setName( HazelcastQueue.INDEXING.name() ).setMaxSize( 100_000 ).setBackupCount( 1 );
    }

    @Bean
    public QueueConfigurer linkingQueueConfigurer() {
        return config -> config
                .setName( HazelcastQueue.LINKING_CANDIDATES.name() )
                .setMaxSize( 1_000 )
                .setBackupCount( 1 );
    }

    @Bean
    public QueueConfigurer linkingIndexingQueueConfigurer() {
        return config -> config
                .setName( HazelcastQueue.LINKING_INDEXING.name() )
                .setMaxSize( 10_000 )
                .setBackupCount( 1 );
    }

    @Bean
    public QueueConfigurer linkingUnIndexingQueueConfigurer() {
        return config -> config
                .setName( HazelcastQueue.LINKING_UNINDEXING.name() )
                .setMaxSize( 10_000 )
                .setBackupCount( 1 );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, App> appMapstore() {
        return new AppMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, AppType> appTypeMapstore() {
        return new AppTypeMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<AppConfigKey, AppTypeSetting> appConfigMapstore() {
        return new AppConfigMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<Long, Range> idGenerationMapstore() {
        return new IdGenerationMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, EntityTypeCollection> entityTypeCollectionMapstore() {
        return new EntityTypeCollectionMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, EntitySetCollection> entitySetCollectionMapstore() {
        return new EntitySetCollectionMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<CollectionTemplateKey, UUID> entitySetCollectionConfigMapstore() {
        return new EntitySetCollectionConfigMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, OrganizationExternalDatabaseTable> organizationExternalDatabaseTableMapstore() {
        return new OrganizationExternalDatabaseTableMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, OrganizationExternalDatabaseColumn> organizationExternalDatabaseColumnMapstore() {
        return new OrganizationExternalDatabaseColumnMapstore( hikariDataSource );
    }

    @Bean
    public Auth0TokenProvider auth0TokenProvider() {
        return new Auth0TokenProvider( auth0Configuration );
    }

    @Bean
    public PrincipalTreesMapstore principalTreesMapstore() {
        return new PrincipalTreesMapstore( hikariDataSource );
    }

    @Bean
    public LinkingFeedbackMapstore linkingFeedbackMapstore() {
        return new LinkingFeedbackMapstore( hikariDataSource );
    }

    @Bean
    public SmsInformationMapstore smsInformationMapstore() {
        return new SmsInformationMapstore( hikariDataSource );
    }

}
