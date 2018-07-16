/*
 * Copyright (C) 2018. OpenLattice, Inc
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

package com.openlattice.postgres;

import static com.openlattice.postgres.PostgresColumn.ENTITY_SET_ID;
import static com.openlattice.postgres.PostgresColumn.HASH;
import static com.openlattice.postgres.PostgresColumn.ID;
import static com.openlattice.postgres.PostgresColumn.ID_VALUE;
import static com.openlattice.postgres.PostgresColumn.LAST_INDEX_FIELD;
import static com.openlattice.postgres.PostgresColumn.LAST_WRITE_FIELD;
import static com.openlattice.postgres.PostgresColumn.VERSION;
import static com.openlattice.postgres.PostgresColumn.VERSIONS;
import static com.openlattice.postgres.PostgresDatatype.TIMESTAMPTZ;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.openlattice.authorization.Permission;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.PostgresEdmTypeConverter;
import com.openlattice.edm.type.PropertyType;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DataTables {
    public static final  String                             VALUE_FIELD = "value";
    public static final  PostgresColumnDefinition           LAST_WRITE  = new PostgresColumnDefinition( LAST_WRITE_FIELD,
            TIMESTAMPTZ )
            .notNull();
    public static final  PostgresColumnDefinition           LAST_INDEX  = new PostgresColumnDefinition( LAST_INDEX_FIELD,
            TIMESTAMPTZ )
            .notNull();
    public static final  PostgresColumnDefinition           READERS     = new PostgresColumnDefinition(
            "readers",
            PostgresDatatype.UUID );
    public static final  PostgresColumnDefinition           WRITERS     = new PostgresColumnDefinition(
            "writers",
            PostgresDatatype.UUID );
    public static final  PostgresColumnDefinition           OWNERS      = new PostgresColumnDefinition(
            "owners",
            PostgresDatatype.UUID );

    public static final FullQualifiedName COUNT_FQN = new FullQualifiedName( "openlattice", "@count" );
    public static final FullQualifiedName ID_FQN = new FullQualifiedName( "openlattice", "@id" );
    public static final FullQualifiedName LAST_WRITE_FQN = new FullQualifiedName( "openlattice", "@lastWrite" );
    public static final FullQualifiedName LAST_INDEX_FQN = new FullQualifiedName( "openlattice", "@lastIndex" );

    private static final Map<UUID, PostgresTableDefinition> ES_TABLES   = Maps.newConcurrentMap();
    private static final Encoder                            encoder     = Base64.getEncoder();

    private static Set<FullQualifiedName> unindexedProperties = Sets
            .newConcurrentHashSet( Arrays
                    .asList(
                            new FullQualifiedName( "incident.narrative" ),
                            new FullQualifiedName( "person.picture" ),
                            new FullQualifiedName( "person.mugshot" ) ) );

    public static String propertyTableName( UUID propertyTypeId ) {
        return "pt_" + propertyTypeId.toString();
    }

    public static String entityTableName( UUID entitySetId ) {
        return "es_" + entitySetId.toString();
    }

    public static String quote( String s ) {
        return "\"" + s + "\"";
    }

    public static PostgresColumnDefinition value( PropertyType pt ) {
        //We name the column after the full qualified name of the property so that in joins it transfers cleanly
        return new PostgresColumnDefinition( quote( pt.getType().getFullQualifiedNameAsString() ),
                PostgresEdmTypeConverter.map( pt.getDatatype() ) );

    }

    public static String mapPermissionToPostgresPrivilege( Permission p ) {
        switch ( p ) {
            default:
                return p.name();
        }
    }

    public static PostgresTableDefinition buildEntitySetTableDefinition( EntitySet entitySet ) {
        return buildEntitySetTableDefinition( entitySet.getId() );
    }

    public static PostgresTableDefinition buildEntitySetTableDefinition( UUID entitySetId ) {
        return ES_TABLES.computeIfAbsent( entitySetId, DataTables::doBuildEntitySetTableDefinition );
    }

    public static PostgresTableDefinition doBuildEntitySetTableDefinition( UUID entitySetId ) {
        PostgresTableDefinition ptd = new PostgresTableDefinition( quote( entityTableName( entitySetId ) ) )
                .addColumns( ID, VERSION, VERSIONS, LAST_WRITE, LAST_INDEX, READERS, WRITERS, OWNERS );

        String idxPrefix = entityTableName( entitySetId );

        PostgresIndexDefinition lastWriteIndex = new PostgresIndexDefinition( ptd, LAST_WRITE )
                .name( quote( idxPrefix + "_last_write_idx" ) )
                .ifNotExists()
                .desc();
        PostgresIndexDefinition lastIndexedIndex = new PostgresIndexDefinition( ptd, LAST_INDEX )
                .name( quote( idxPrefix + "_last_indexed_idx" ) )
                .ifNotExists()
                .desc();

        PostgresIndexDefinition readersIndex = new PostgresIndexDefinition( ptd, READERS )
                .name( quote( idxPrefix + "_readers_idx" ) )
                .ifNotExists();
        PostgresIndexDefinition writersIndex = new PostgresIndexDefinition( ptd, WRITERS )
                .name( quote( idxPrefix + "_writers_idx" ) )
                .ifNotExists();
        PostgresIndexDefinition ownersIndex = new PostgresIndexDefinition( ptd, OWNERS )
                .name( quote( idxPrefix + "_owners_idx" ) )
                .ifNotExists();

        ptd.addIndexes( lastWriteIndex, lastIndexedIndex, readersIndex, writersIndex, ownersIndex );

        return ptd;
    }

    public static PostgresTableDefinition buildPropertyTableDefinition(
            EntitySet entitySet,
            PropertyType propertyType ) {
        return buildPropertyTableDefinition( propertyType );
    }

    public static PostgresTableDefinition buildPropertyTableDefinition(
            PropertyType propertyType ) {
        final String idxPrefix = propertyTableName( propertyType.getId() );

        PostgresColumnDefinition valueColumn = value( propertyType );
        PostgresTableDefinition ptd = new PostgresTableDefinition(
                quote( idxPrefix ) )
                .addColumns(
                        ENTITY_SET_ID, 
                        ID_VALUE,
                        HASH,
                        valueColumn,
                        VERSION,
                        VERSIONS,
                        LAST_WRITE,
                        READERS,
                        WRITERS,
                        OWNERS )
                .primaryKey( ENTITY_SET_ID, ID_VALUE, HASH );

        PostgresIndexDefinition idIndex = new PostgresIndexDefinition( ptd, ID_VALUE )
                .name( quote( idxPrefix + "_id_idx" ) )
                .ifNotExists();

        //Byte arrays are generally too large to be indexed by postgres
        if ( unindexedProperties.contains( propertyType.getDatatype().getFullQualifiedName() ) ) {
            PostgresIndexDefinition valueIndex = new PostgresIndexDefinition( ptd, valueColumn )
                    .name( quote( idxPrefix + "_value_idx" ) )
                    .ifNotExists();

            ptd.addIndexes( valueIndex );
        }

        PostgresIndexDefinition entitySetIdIndex = new PostgresIndexDefinition( ptd, ENTITY_SET_ID )
                .name( quote( idxPrefix + "_entity_set_id_idx" ) )
                .ifNotExists();

        PostgresIndexDefinition versionIndex = new PostgresIndexDefinition( ptd, LAST_WRITE )
                .name( quote( idxPrefix + "_version_idx" ) )
                .ifNotExists()
                .desc();

        //TODO: Re-consider the value of having gin index on versions field. Checking if a value was written
        //in a specific version seems like a rare operations
        PostgresIndexDefinition versionsIndex = new PostgresIndexDefinition( ptd, VERSIONS )
                .name( quote( idxPrefix + "_versions_idx" ) )
                .method( IndexMethod.GIN )
                .ifNotExists();

        PostgresIndexDefinition lastWriteIndex = new PostgresIndexDefinition( ptd, LAST_WRITE )
                .name( quote( idxPrefix + "_last_write_idx" ) )
                .ifNotExists()
                .desc();

        PostgresIndexDefinition readersIndex = new PostgresIndexDefinition( ptd, READERS )
                .name( quote( idxPrefix + "_readers_idx" ) )
                .ifNotExists();

        PostgresIndexDefinition writersIndex = new PostgresIndexDefinition( ptd, WRITERS )
                .name( quote( idxPrefix + "_writers_idx" ) )
                .ifNotExists();

        PostgresIndexDefinition ownersIndex = new PostgresIndexDefinition( ptd, OWNERS )
                .name( quote( idxPrefix + "_owners_idx" ) )
                .ifNotExists();

        ptd.addIndexes(
                idIndex,
                entitySetIdIndex,
                versionIndex,
                versionsIndex,
                lastWriteIndex,
                readersIndex,
                writersIndex,
                ownersIndex );

        return ptd;
    }
}
