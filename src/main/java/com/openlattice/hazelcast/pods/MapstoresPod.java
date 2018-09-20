

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

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore;
import com.kryptnostic.rhizome.pods.hazelcast.QueueConfigurer;
import com.openlattice.apps.App;
import com.openlattice.apps.AppConfigKey;
import com.openlattice.apps.AppType;
import com.openlattice.apps.AppTypeSetting;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.auth0.Auth0TokenProvider;
import com.openlattice.authentication.Auth0Configuration;
import com.openlattice.authorization.AceKey;
import com.openlattice.authorization.AceValue;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AclKeySet;
import com.openlattice.authorization.PostgresUserApi;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.authorization.mapstores.PermissionMapstore;
import com.openlattice.authorization.mapstores.PostgresCredentialMapstore;
import com.openlattice.authorization.mapstores.PrincipalMapstore;
import com.openlattice.authorization.mapstores.PrincipalTreeMapstore;
import com.openlattice.authorization.mapstores.PrincipalTreesMapstore;
import com.openlattice.authorization.mapstores.UserMapstore;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.directory.pojo.Auth0UserBasic;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.set.EntitySetPropertyKey;
import com.openlattice.edm.set.EntitySetPropertyMetadata;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.ComplexType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.EnumType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.ids.IdGenerationMapstore;
import com.openlattice.ids.Range;
import com.openlattice.linking.LinkingVertex;
import com.openlattice.linking.LinkingVertexKey;
import com.openlattice.linking.WeightedLinkingVertexKeySet;
import com.openlattice.organizations.PrincipalSet;
import com.openlattice.postgres.PostgresPod;
import com.openlattice.postgres.PostgresTableManager;
import com.openlattice.postgres.mapstores.AclKeysMapstore;
import com.openlattice.postgres.mapstores.AppConfigMapstore;
import com.openlattice.postgres.mapstores.AppMapstore;
import com.openlattice.postgres.mapstores.AppTypeMapstore;
import com.openlattice.postgres.mapstores.AssociationTypeMapstore;
import com.openlattice.postgres.mapstores.ComplexTypeMapstore;
import com.openlattice.postgres.mapstores.EdmVersionsMapstore;
import com.openlattice.postgres.mapstores.EntitySetMapstore;
import com.openlattice.postgres.mapstores.EntitySetPropertyMetadataMapstore;
import com.openlattice.postgres.mapstores.EntityTypeMapstore;
import com.openlattice.postgres.mapstores.EnumTypesMapstore;
import com.openlattice.postgres.mapstores.LinkedEntityTypesMapstore;
import com.openlattice.postgres.mapstores.LinkingVerticesMapstore;
import com.openlattice.postgres.mapstores.NamesMapstore;
import com.openlattice.postgres.mapstores.OrganizationAppsMapstore;
import com.openlattice.postgres.mapstores.OrganizationDescriptionsMapstore;
import com.openlattice.postgres.mapstores.OrganizationEmailDomainsMapstore;
import com.openlattice.postgres.mapstores.OrganizationMembersMapstore;
import com.openlattice.postgres.mapstores.OrganizationTitlesMapstore;
import com.openlattice.postgres.mapstores.RequestsMapstore;
import com.openlattice.postgres.mapstores.SchemasMapstore;
import com.openlattice.postgres.mapstores.SecurableObjectTypeMapstore;
import com.openlattice.postgres.mapstores.SyncIdsMapstore;
import com.openlattice.postgres.mapstores.VertexIdsAfterLinkingMapstore;
import com.openlattice.requests.Status;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDSet;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import javax.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

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
        try ( Connection conn = hikariDataSource.getConnection(); Statement stmt = conn.createStatement(); ) {
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
    public SelfRegisteringMapStore<UUID, ComplexType> complexTypeMapstore() {
        return new ComplexTypeMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, EnumType> enumTypeMapstore() {
        return new EnumTypesMapstore( hikariDataSource );
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
        AclKeysMapstore pakm = new AclKeysMapstore( hikariDataSource );

        //        com.dataloom.edm.mapstores.AclKeysMapstore akm = new com.dataloom.edm.mapstores.AclKeysMapstore( session );
        //        for ( String name : akm.loadAllKeys() ) {
        //            pakm.store( name, akm.load( name ) );
        //        }
        return pakm;
    }

    @Bean
    public SelfRegisteringMapStore<String, UUID> edmVersionMapstore() {
        return new EdmVersionsMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, String> namesMapstore() {
        NamesMapstore pnm = new NamesMapstore( hikariDataSource );

        //        com.dataloom.edm.mapstores.NamesMapstore nm = new com.dataloom.edm.mapstores.NamesMapstore( session );
        //        for ( UUID key : nm.loadAllKeys() ) {
        //            pnm.store( key, nm.load( key ) );
        //        }
        return pnm;
    }

    @Bean
    public SelfRegisteringMapStore<AceKey, Status> requestMapstore() {
        return new RequestsMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, String> orgTitlesMapstore() {
        OrganizationTitlesMapstore potm = new OrganizationTitlesMapstore( hikariDataSource );

        //        StringMapstore otm = new StringMapstore(
        //                HazelcastMap.ORGANIZATIONS_TITLES,
        //                session,
        //                Table.ORGANIZATIONS,
        //                CommonColumns.ID,
        //                CommonColumns.TITLE );
        //
        //        for ( UUID id : otm.loadAllKeys() ) {
        //            potm.store( id, otm.load( id ) );
        //        }
        return potm;
    }

    @Bean
    public SelfRegisteringMapStore<UUID, String> orgDescsMapstore() {
        OrganizationDescriptionsMapstore podm = new OrganizationDescriptionsMapstore( hikariDataSource );

        //        StringMapstore odm = new StringMapstore(
        //                HazelcastMap.ORGANIZATIONS_DESCRIPTIONS,
        //                session,
        //                Table.ORGANIZATIONS,
        //                CommonColumns.ID,
        //                CommonColumns.DESCRIPTION );
        //        for ( UUID id : odm.loadAllKeys() ) {
        //            podm.store( id, odm.load( id ) );
        //        }
        return podm;
    }

    @Bean
    public SelfRegisteringMapStore<UUID, DelegatedStringSet> aaEmailDomainsMapstore() {
        OrganizationEmailDomainsMapstore pedm = new OrganizationEmailDomainsMapstore( hikariDataSource );
        return pedm;
    }

    @Bean
    public SelfRegisteringMapStore<UUID, PrincipalSet> membersMapstore() {
        OrganizationMembersMapstore pmm = new OrganizationMembersMapstore( hikariDataSource );
        return pmm;
    }

    @Bean
    public SelfRegisteringMapStore<UUID, DelegatedUUIDSet> orgAppsMapstore() {
        return new OrganizationAppsMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, DelegatedUUIDSet> linkedEntityTypesMapstore() {
        return new LinkedEntityTypesMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, DelegatedUUIDSet> linkedEntitySetsMapstore() {
        com.openlattice.postgres.mapstores.LinkedEntitySetsMapstore plesm =
                new com.openlattice.postgres.mapstores.LinkedEntitySetsMapstore( hikariDataSource );
        return plesm;
    }

    @Bean
    public SelfRegisteringMapStore<LinkingVertexKey, LinkingVertex> linkingVerticesMapstore() {
        LinkingVerticesMapstore plvm = new LinkingVerticesMapstore( hikariDataSource );
        return plvm;
    }

    @Bean
    public SelfRegisteringMapStore<UUID, AssociationType> edgeTypeMapstore() {
        AssociationTypeMapstore patm = new AssociationTypeMapstore( hikariDataSource );
        return patm;
    }

    @Bean
    public SelfRegisteringMapStore<AclKey, SecurablePrincipal> principalsMapstore() {
        return new PrincipalMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<UUID, UUID> syncIdsMapstore() {
        return new SyncIdsMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<LinkingVertexKey, UUID> vertexIdsAfterLinkingMapstore() {
        return new VertexIdsAfterLinkingMapstore( hikariDataSource );
    }

    @Bean
    public SelfRegisteringMapStore<String, String> dbCredentialsMapstore() {
        return new PostgresCredentialMapstore( hikariDataSource, pgUserApi() );
    }

    @Bean
    public SelfRegisteringMapStore<String, Auth0UserBasic> userMapstore() {
        return new UserMapstore( auth0TokenProvider() );
    }

    @Bean
    public SelfRegisteringMapStore<EntitySetPropertyKey, EntitySetPropertyMetadata> entitySetPropertyMetadataMapstore() {
        EntitySetPropertyMetadataMapstore pespm = new EntitySetPropertyMetadataMapstore( hikariDataSource );
        return pespm;
    }

    @Bean
    public QueueConfigurer defaultQueueConfigurer() {
        return config -> config.setMaxSize( 10000 ).setEmptyQueueTtl( 60 );
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
    public Auth0TokenProvider auth0TokenProvider() {
        return new Auth0TokenProvider( auth0Configuration );
    }

    @Bean
    public PrincipalTreesMapstore principalTreesMapstore() {
        return new PrincipalTreesMapstore( hikariDataSource );
    }
}
