

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

package com.openlattice.authorization;

import com.dataloom.streams.StreamUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.openlattice.authorization.paging.AuthorizedObjectsSearchResult;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumn;
import com.openlattice.postgres.PostgresQuery;
import com.openlattice.postgres.PostgresTable;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AuthorizationQueryService {
    private static final Logger                 logger = LoggerFactory
            .getLogger( AuthorizationQueryService.class );
    private final        HikariDataSource       hds;
    private final        IMap<AceKey, AceValue> aces;

    public AuthorizationQueryService( HikariDataSource hds, HazelcastInstance hazelcastInstance ) {
        this.hds = hds;
        aces = HazelcastMap.PERMISSIONS.getMap( hazelcastInstance );
    }

    private String getAuthorizedAclKeyQuery(
            boolean securableObjectType,
            int numPrincipals,
            boolean limit,
            boolean offset ) {
        // Tables
        String PERMISSIONS_TABLE = PostgresTable.PERMISSIONS.getName();
        String SECURABLE_OBJECTS = PostgresTable.SECURABLE_OBJECTS.getName();

        // Columns
        String ACL_KEY = PostgresColumn.ACL_KEY.getName();
        String PRINCIPAL_TYPE = PostgresColumn.PRINCIPAL_TYPE.getName();
        String PRINCIPAL_ID = PostgresColumn.PRINCIPAL_ID.getName();
        String PERMISSIONS = PostgresColumn.PERMISSIONS.getName();
        String SECURABLE_OBJECT_TYPE = PostgresColumn.SECURABLE_OBJECT_TYPE.getName();
        String PT_ACL_KEY = PERMISSIONS_TABLE.concat( "." ).concat( ACL_KEY );
        String SO_ACL_KEY = SECURABLE_OBJECTS.concat( "." ).concat( ACL_KEY );

        // SELECT permissions.acl_key from permissions, securable_objects
        StringBuilder sql = new StringBuilder( PostgresQuery
                .selectDistinctFrom( ImmutableList.of( PERMISSIONS_TABLE, SECURABLE_OBJECTS ),
                        ImmutableList.of( PT_ACL_KEY ) ) );

        // WHERE permissions.acl_key = securable_objects.acl_key
        sql.append( PostgresQuery.whereColsAreEq( PT_ACL_KEY, SO_ACL_KEY ) );

        // AND [<SomePermission>, <AnotherPermission>] @< permissions
        sql.append( PostgresQuery.AND ).append( PostgresQuery.valuesInArray( PERMISSIONS ) );

        // maybe: AND securable_object_type = ?
        if ( securableObjectType ) {
            sql.append( PostgresQuery.AND ).append( PostgresQuery.eq( SECURABLE_OBJECT_TYPE ) );
        }

        // AND ( (principal_type = ? AND principal_id = ?) OR (principal_type = ? AND principal_id = ?) OR ... )
        String singlePrincipalCheck = "(".concat( PostgresQuery.eq( PRINCIPAL_TYPE ) ).concat( PostgresQuery.AND )
                .concat( PostgresQuery.eq( PRINCIPAL_ID ) ).concat( ")" );
        sql.append( PostgresQuery.AND ).append( "(" )
                .append( Stream.generate( () -> singlePrincipalCheck ).limit( numPrincipals )
                        .collect( Collectors.joining( PostgresQuery.OR ) ) ).append( ")" );

        // maybe: LIMIT ?
        if ( limit ) { sql.append( PostgresQuery.LIMIT ); }

        // maybe: OFFSET ?
        if ( offset ) { sql.append( PostgresQuery.OFFSET ); }

        sql.append( PostgresQuery.END );
        return sql.toString();
    }

    /**
     * get all authorized acl keys for a set of principals, of all object types, with desired permissions.
     */
    public Stream<AclKey> getAuthorizedAclKeys(
            Set<Principal> principals,
            EnumSet<Permission> desiredPermissions ) {
        return getAuthorizedAclKeysForPrincipals( principals, desiredPermissions, Optional.empty() );
    }

    @SuppressFBWarnings( value = "OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE", justification = "Resources will be closed on error" )
    private PreparedStatement prepareAuthorizedAclKeysQuery(
            Connection connection,
            Set<Principal> principals,
            EnumSet<Permission> permissions,
            Optional<SecurableObjectType> securableObjectType,
            Optional<Integer> limit,
            Optional<Integer> offset ) throws SQLException {
        String sql = getAuthorizedAclKeyQuery( securableObjectType.isPresent(),
                principals.size(),
                limit.isPresent(),
                offset.isPresent() );
        PreparedStatement ps = connection.prepareStatement( sql );
        int count = 1;

        ps.setArray( count, PostgresArrays
                .createTextArray( connection, permissions.stream().map( Enum::name ) ) );
        count++;

        if ( securableObjectType.isPresent() ) {
            ps.setString( count, securableObjectType.get().name() );
            count++;
        }

        for ( Principal principal : principals ) {
            ps.setString( count, principal.getType().name() );
            count++;
            ps.setString( count, principal.getId() );
            count++;
        }

        if ( limit.isPresent() ) {
            ps.setInt( count, limit.get() );
            count++;
        }

        if ( offset.isPresent() ) {
            ps.setInt( count, offset.get() );
        }

        return ps;

    }

    private Stream<AclKey> getAuthorizedAclKeysForPrincipals(
            Set<Principal> principals,
            EnumSet<Permission> desiredPermissions,
            Optional<SecurableObjectType> securableObjectType ) {
        return getAuthorizedAclKeysForPrincipals( principals,
                desiredPermissions,
                securableObjectType,
                Optional.empty(),
                Optional.empty() );
    }

    private Stream<AclKey> getAuthorizedAclKeysForPrincipals(
            Set<Principal> principals,
            EnumSet<Permission> desiredPermissions,
            Optional<SecurableObjectType> securableObjectType,
            Optional<Integer> limit,
            Optional<Integer> offset ) {
        try ( Connection connection = hds.getConnection() ) {
            PreparedStatement ps = prepareAuthorizedAclKeysQuery( connection,
                    principals,
                    desiredPermissions,
                    securableObjectType,
                    limit,
                    offset );
            List<AclKey> result = Lists.newArrayList();
            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.add( ResultSetAdapters.aclKey( rs ) );
            }
            rs.close();
            connection.close();
            return StreamUtil.stream( result );
        } catch ( SQLException e ) {
            logger.debug( "Unable to get authorized acl keys.", e );
            return Stream.empty();
        }
    }
}
