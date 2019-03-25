package com.openlattice.postgres.mapstores;

import static com.openlattice.postgres.PostgresColumn.ID;
import static com.openlattice.postgres.PostgresColumn.MEMBERS;
import static com.openlattice.postgres.PostgresTable.ORGANIZATIONS;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.organizations.PrincipalSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.openlattice.postgres.PostgresArrays;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang.RandomStringUtils;

/**
 *  There is currently an implication in the codebase that all Principals in this Mapstore are of type PrincipalType.USER
 */
public class OrganizationMembersMapstore extends AbstractBasePostgresMapstore<UUID, PrincipalSet> {

    public static final String ANY_PRINCIPAL_SET = "principals[any]";

    public OrganizationMembersMapstore( HikariDataSource hds ) {
        super( HazelcastMap.ORGANIZATIONS_MEMBERS.name(), ORGANIZATIONS, hds );
    }

    @Override
    public MapConfig getMapConfig() {
        return super
                .getMapConfig()
                .addMapIndexConfig( new MapIndexConfig(ANY_PRINCIPAL_SET, false) );
    }

    @Override
    protected List<PostgresColumnDefinition> initValueColumns() {
        return ImmutableList.of( MEMBERS );
    }

    @Override
    protected void bind( PreparedStatement ps, UUID key, PrincipalSet value ) throws SQLException {
        bind( ps, key, 1 );

        Array principalArray = PostgresArrays
                .createTextArray( ps.getConnection(), value.stream().map( Principal::getId ) );
        ps.setArray( 2, principalArray );

        // UPDATE
        ps.setArray( 3, principalArray );
    }

    @Override protected int bind( PreparedStatement ps, UUID key, int parameterIndex ) throws SQLException {
        ps.setObject( parameterIndex++, key );
        return parameterIndex;
    }

    @Override protected PrincipalSet mapToValue( ResultSet rs ) throws SQLException {
        Array arr = rs.getArray( MEMBERS.getName() );
        if ( arr != null ) {
            String[] value = (String[]) arr.getArray();
            if ( value != null ) {
                return PrincipalSet
                        .wrap( Arrays.stream( value ).map( user -> new Principal( PrincipalType.USER, user ) )
                                .collect( Collectors.toSet() ) );
            }
        }
        return PrincipalSet.wrap( Sets.newHashSet() );
    }

    @Override protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return ResultSetAdapters.id( rs );
    }

    @Override
    public Map<UUID, PrincipalSet> loadAll( Collection<UUID> keys ) {
        Map<UUID, PrincipalSet> result = Maps.newConcurrentMap();
        keys.parallelStream().forEach( id -> {
            PrincipalSet users = load( id );
            if ( users != null ) { result.put( id, users ); }
        } );
        return result;
    }

    @Override
    protected List<PostgresColumnDefinition> getInsertColumns() {
        return ImmutableList.of( ID, MEMBERS );
    }

    @Override public UUID generateTestKey() {
        return UUID.randomUUID();
    }

    @Override public PrincipalSet generateTestValue() {
        return PrincipalSet
                .wrap( ImmutableSet.of( new Principal( PrincipalType.USER, RandomStringUtils.randomAlphanumeric( 5 ) ),
                        new Principal( PrincipalType.USER, RandomStringUtils.randomAlphanumeric( 5 ) ),
                        new Principal( PrincipalType.USER, RandomStringUtils.randomAlphanumeric( 5 ) ) ) );
    }
}
