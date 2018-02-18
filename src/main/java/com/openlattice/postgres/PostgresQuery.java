package com.openlattice.postgres;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PostgresQuery {
    public static final String SELECT   = "SELECT ";
    public static final String DELETE   = "DELETE ";
    public static final String UPDATE   = "UPDATE ";
    public static final String DISTINCT = "DISTINCT ";
    public static final String TRUNCATE = "TRUCATE TABLE ";
    public static final String INSERT   = "INSERT INTO ";
    public static final String VALUES   = " VALUES ";
    public static final String FROM     = " FROM ";
    public static final String WHERE    = " WHERE ";
    public static final String SET      = " SET ";
    public static final String AND      = " AND ";
    public static final String OR       = " OR ";
    public static final String LIMIT    = " LIMIT ? ";
    public static final String OFFSET   = " OFFSET ? ";
    public static final String EQ       = " = ";
    public static final String EQ_VAR   = " = ?";
    public static final String ALL      = "*";
    public static final String END      = ";";

    public static String selectColsFrom( String table, List<String> columns ) {
        return selectColsFrom( ImmutableList.of( table ), columns );
    }

    public static String selectColsFrom( List<String> tables, List<String> columns ) {
        String colsString = columns.size() == 0 ? ALL : commaJoin( columns );
        return SELECT.concat( colsString ).concat( FROM ).concat( commaJoin( tables ) ).concat( " " );
    }

    public static String selectDistinctFrom( String table, List<String> columns ) {
        String colsString = columns.size() == 0 ? ALL : commaJoin( columns );
        return SELECT.concat( DISTINCT ).concat( colsString ).concat( FROM ).concat( table ).concat( " " );
    }

    public static String selectFrom( String table ) {
        return selectColsFrom( table, ImmutableList.of() );
    }

    public static String deleteFrom( String table ) {
        return DELETE.concat( FROM ).concat( table ).concat( " " );
    }

    public static String truncate( String table ) {
        return TRUNCATE.concat( table );
    }

    public static String insertRow( String table, List<String> columns ) {
        return INSERT.concat( table ).concat( "(" ).concat( commaJoin( columns ) ).concat( ")" ).concat( VALUES )
                .concat( "(" )
                .concat( Stream.generate( () -> "?" ).limit( columns.size() ).collect( Collectors.joining( ", " ) ) )
                .concat( ");" );
    }

    public static String update( String table ) {
        return UPDATE.concat( table ).concat( " " );
    }

    public static String eq( String column ) {
        return column.concat( EQ_VAR );
    }

    public static String colsEq( List<String> columns ) {
        return columns.stream().map( PostgresQuery::eq ).collect( Collectors.joining( AND ) );
    }

    public static String whereEq( List<String> columns ) {
        return WHERE.concat( colsEq( columns ) );
    }

    public static String setEq( List<String> columns ) {
        return SET.concat( colsEq( columns ) );
    }

    public static String whereEq( List<String> columns, boolean end ) {
        return end ? whereEq( columns ).concat( END ) : whereEq( columns );
    }

    public static String whereColsAreEq( String col1, String col2 ) {
        return WHERE.concat( col1 ).concat( EQ ).concat( col2 ).concat( " " );
    }

    public static String valueInArray( String col ) {
        return " ? = ANY(".concat( col ).concat( ") " );
    }

    public static String valueInArray( String col, boolean end ) {
        return end ? valueInArray( col ).concat( END ) : valueInArray( col );
    }

    public static String valuesInArray( String col ) {
        return " ? <@ ".concat( col ).concat( " " );
    }

    public static String valuesInArray( String col, boolean end ) {
        return end ? valuesInArray( col ).concat( END ) : valuesInArray( col );
    }

    public static String colValueInArray( String col ) {
        return col.concat( " = ANY(?) " );
    }

    // helpers

    private static String commaJoin( List<String> values ) {
        return values.stream().collect( Collectors.joining( ", " ) );
    }
}