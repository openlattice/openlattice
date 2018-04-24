package com.openlattice.data.storage;

import com.dataloom.streams.StreamUtil;
import com.openlattice.data.Entity;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.postgres.CountdownConnectionCloser;
import com.openlattice.postgres.DataTables;
import com.openlattice.postgres.KeyIterator;
import com.openlattice.postgres.ResultSetAdapters;
import com.zaxxer.hikari.HikariDataSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PostgresDataManager {
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
        Set<UUID> authorizedPropertyTypeIds = authorizedPropertyTypes.stream().map( PropertyType::getId )
                .collect( Collectors.toSet() );

        try {
            Connection connection = hds.getConnection();
            CallableStatement ps = connection
                    .prepareCall( getEntitiesInEntitySetQuery( entitySetId, authorizedPropertyTypes ) );

            ResultSet rs = ps.executeQuery();

            KeyIterator keyIterator = new KeyIterator<>( rs,
                    new CountdownConnectionCloser( rs, connection, 1 ),
                    row -> ResultSetAdapters.entity( row, authorizedPropertyTypeIds ) );

            return StreamUtil.stream( () -> keyIterator );

        } catch ( SQLException e ) {
            logger.debug( "Unable to load entity set data.", e );
            return Stream.empty();
        }
    }
}
