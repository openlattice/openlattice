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

import static com.openlattice.postgres.PostgresColumn.ID;
import static com.openlattice.postgres.PostgresColumn.ID_VALUE;
import static com.openlattice.postgres.PostgresColumn.LAST_INDEX_FIELD;
import static com.openlattice.postgres.PostgresColumn.LAST_WRITE_FIELD;
import static com.openlattice.postgres.PostgresColumn.VERSION;
import static com.openlattice.postgres.PostgresColumn.VERSIONS;
import static com.openlattice.postgres.PostgresDatatype.TIMESTAMPTZ;

import com.google.common.collect.Maps;
import com.openlattice.authorization.Permission;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.PostgresEdmTypeConverter;
import com.openlattice.edm.type.PropertyType;
import java.util.Map;
import java.util.UUID;

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
    private static final Map<UUID, PostgresTableDefinition> ES_TABLES   = Maps.newConcurrentMap();

    public static String propertyTableName( UUID entitySetId, UUID propertyTypeId ) {
        return entitySetId.toString() + "_" + propertyTypeId.toString();
    }

    public static String entityTableName( UUID entitySetId ) {
        return entitySetId.toString();
    }

    public static String quote( String s ) {
        return "\"" + s + "\"";
    }

    public static PostgresColumnDefinition value( PropertyType pt ) {
        return new PostgresColumnDefinition( VALUE_FIELD, PostgresEdmTypeConverter.map( pt.getDatatype() ) );

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
                .addColumns( ID, VERSION, LAST_WRITE, LAST_INDEX, READERS, WRITERS, OWNERS );

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
        return buildPropertyTableDefinition( entitySet.getId(), propertyType );
    }

    public static PostgresTableDefinition buildPropertyTableDefinition(
            UUID entitySetId,
            PropertyType propertyType ) {
        PostgresColumnDefinition valueColumn = value( propertyType );
        PostgresTableDefinition ptd = new PostgresTableDefinition(
                quote( propertyTableName( entitySetId, propertyType.getId() ) ) )
                .addColumns( ID_VALUE, valueColumn, VERSION, VERSIONS, LAST_WRITE, READERS, WRITERS, OWNERS )
                .primaryKey( ID_VALUE, valueColumn )
                .setUnique( valueColumn );

        String idxPrefix = propertyTableName( entitySetId, propertyType.getId() );

        PostgresIndexDefinition valueIndex = new PostgresIndexDefinition( ptd, valueColumn )
                .name( quote( idxPrefix + "_value_idx" ) )
                .ifNotExists();

        PostgresIndexDefinition versionIndex = new PostgresIndexDefinition( ptd, LAST_WRITE )
                .name( quote( idxPrefix + "_version_idx" ) )
                .ifNotExists()
                .desc();

        PostgresIndexDefinition versionsIndex = new PostgresIndexDefinition( ptd, LAST_WRITE )
                .name( quote( idxPrefix + "_versions_idx" ) )
                .method( IndexMethod.GIN )
                .ifNotExists()
                .desc();

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

        ptd.addIndexes( valueIndex, versionIndex, versionsIndex, readersIndex, writersIndex, ownersIndex );

        return ptd;
    }
}
