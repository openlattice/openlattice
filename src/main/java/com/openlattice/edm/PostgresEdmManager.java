/*
 * Copyright (C) 2017. OpenLattice, Inc
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

package com.openlattice.edm;

import com.google.common.collect.Maps;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.openlattice.data.PropertyUsageSummary;
import com.openlattice.hazelcast.HazelcastMap;
import com.openlattice.postgres.*;
import com.openlattice.postgres.mapstores.EntitySetMapstore;
import com.openlattice.postgres.streams.PostgresIterable;
import com.openlattice.postgres.streams.StatementHolder;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.*;
import static com.openlattice.postgres.PostgresTable.*;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PostgresEdmManager {
    private static final Logger logger = LoggerFactory.getLogger( PostgresEdmManager.class );

    private final HikariDataSource hds;

    public PostgresEdmManager( HikariDataSource hds ) {
        this.hds = hds;
    }

    public Iterable<PropertyUsageSummary> getPropertyUsageSummary( UUID propertyTypeId ) {
        final var wrappedEntitySetsTableName = "wrapped_entity_sets";
        final var getPropertyTypeSummary =
                String.format( "WITH %1$s AS (SELECT %2$s, %3$s AS %4$s, %5$s FROM %6$s) " +
                                "SELECT %2$s, %4$s, %7$s, COUNT(*) FROM %8$s LEFT JOIN %1$s ON %7$s = %1$s.id " +
                                "WHERE %9$s > 0 AND %10$s = ? GROUP BY ( %2$s , %4$s, %7$s )",
                        wrappedEntitySetsTableName,
                        ENTITY_TYPE_ID.getName(),
                        NAME.getName(),
                        ENTITY_SET_NAME.getName(),
                        ID.getName(),
                        ENTITY_SETS.getName(),
                        ENTITY_SET_ID.getName(),
                        DATA.getName(),
                        VERSION.getName(),
                        PROPERTY_TYPE_ID.getName()
                );

        return new PostgresIterable<>( () -> {
            try {
                Connection connection = hds.getConnection();
                PreparedStatement ps = connection.prepareStatement( getPropertyTypeSummary );
                ps.setObject( 1, propertyTypeId );
                ResultSet rs = ps.executeQuery();
                return new StatementHolder( connection, ps, rs );
            } catch ( SQLException e ) {
                logger.error( "Unable to create statement holder!", e );
                throw new IllegalStateException( "Unable to create statement holder.", e );
            }
        }, rs -> {
            try {
                return ResultSetAdapters.propertyUsageSummary( rs );
            } catch ( SQLException e ) {
                logger.error( "Unable to load property summary information.", e );
                throw new IllegalStateException( "Unable to load property summary information.", e );
            }
        } );
    }

    public Map<UUID, Long> countEntitySetsOfEntityTypes( Set<UUID> entityTypeIds ) {
        String query =
                "SELECT " + ENTITY_TYPE_ID.getName() + ", COUNT(*) FROM " + ENTITY_SETS.getName() + " WHERE " + ENTITY_TYPE_ID
                        .getName() + " = ANY( ? ) GROUP BY " + ENTITY_TYPE_ID.getName();
        try ( Connection connection = hds.getConnection();
              PreparedStatement ps = connection.prepareStatement( query ) ) {
            Map<UUID, Long> result = Maps.newHashMap();
            Array arr = PostgresArrays.createUuidArray( connection, entityTypeIds );
            ps.setArray( 1, arr );
            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                result.put( ResultSetAdapters.entityTypeId( rs ), ResultSetAdapters.count( rs ) );
            }

            return result;
        } catch ( SQLException e ) {
            logger.debug( "Unable to count entity sets for entity type ids {}", entityTypeIds, e );
            return Map.of();
        }
    }

}
