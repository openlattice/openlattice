package com.openlattice.data.storage;

import com.openlattice.data.Entity;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.postgres.DataTables;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.postgres.streams.PostgresIterable;
import com.openlattice.postgres.streams.StatementHolder;
import com.zaxxer.hikari.HikariDataSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.*;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresDataManager {
    // TODO:
    protected final Logger logger = LoggerFactory.getLogger( getClass() );

    private final HikariDataSource hds;

    public PostgresDataManager( HikariDataSource hds ) {
        this.hds = hds;
    }

    public String getEntitiesInEntitySetQuery( UUID entitySetId, Set<PropertyType> authorizedPropertyTypes ) {
        String esTableName = DataTables.quote( DataTables.entityTableName( entitySetId ) );

        StringBuilder subQueries = new StringBuilder( "" );
        StringBuilder where = new StringBuilder( "" );

        for ( PropertyType pt : authorizedPropertyTypes ) {
            String fqn = DataTables.quote( pt.getType().getFullQualifiedNameAsString() );
            String ptTableName = DataTables.quote( DataTables.propertyTableName( pt.getId() ) );
            subQueries = subQueries
                    .append( " LEFT JOIN (SELECT id, array_agg(" )
                    .append( fqn )
                    .append( ") as " )
                    .append( ptTableName )
                    .append( " FROM " )
                    .append( ptTableName )
                    .append( " WHERE entity_set_id='" )
                    .append( entitySetId.toString() )
                    .append( "' AND version > 0 group by id) as " )
                    .append( ptTableName )
                    .append( " USING (id) " );
        }

        return new StringBuilder( "SELECT * FROM " ).append( esTableName ).append( subQueries ).append( where )
                .append( ";" ).toString();
    }

    @SuppressFBWarnings( value = "ODR_OPEN_DATABASE_RESOURCE", justification = "Connection handled by CountdownConnectionCloser" )
    public Stream<Entity> getEntitiesInEntitySet( UUID entitySetId, Set<PropertyType> authorizedPropertyTypes ) {
        final Set<UUID> authorizedPropertyTypeIds = authorizedPropertyTypes.stream().map( PropertyType::getId )
                .collect( Collectors.toSet() );

        return new PostgresIterable<>( () -> {
            final ResultSet rs;
            final Connection connection;
            final Statement statement;
            try {
                connection = hds.getConnection();
                statement = connection.createStatement();
                rs = statement
                        .executeQuery( getEntitiesInEntitySetQuery( entitySetId, authorizedPropertyTypes ) );
                return new StatementHolder( connection, statement, rs );
            } catch ( SQLException e ) {
                logger.error( "Unable to create statement holder!", e );
                throw new IllegalStateException( "Unable to create statement holder.", e );
            }
        }, rs -> {
            try {
                return ResultSetAdapters.entity( rs, authorizedPropertyTypeIds );
            } catch ( SQLException e ) {
                logger.error( "Unable to load entity information.", e );
                throw new IllegalStateException( "Unable to load entity information.", e );
            }
        } ).stream();
    }

    public void markEntitySetAsNeedingIndexing( UUID entitySetId ) {
        final String reindexSql = reindexSql( entitySetId );

        try (
                Connection connection = hds.getConnection();
                Statement stmt = connection.createStatement()
        ) {
            stmt.execute( reindexSql );
        } catch ( SQLException e ) {
            logger.error( "Unable to mark entity set {} as needing indexing", entitySetId, e );
        }

    }

    private static String reindexSql( UUID entitySetId ) {
        return "UPDATE entity_key_ids SET last_index = '-infinity' WHERE entity_set_id ='"
                + entitySetId.toString()
                + "'";
    }
}
