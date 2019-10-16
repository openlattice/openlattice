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

import com.openlattice.edm.PostgresEdmTypeConverter;
import com.openlattice.edm.type.PropertyType;
import java.util.UUID;

import static com.openlattice.postgres.PostgresColumn.*;
import static com.openlattice.postgres.PostgresDatatype.TIMESTAMPTZ;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DataTables {
    public static final PostgresColumnDefinition LAST_INDEX     = new PostgresColumnDefinition(
            LAST_INDEX_FIELD,
            TIMESTAMPTZ )
            .withDefault( "'-infinity'" )
            .notNull();
    public static final PostgresColumnDefinition LAST_LINK      = new PostgresColumnDefinition(
            LAST_LINK_FIELD,
            TIMESTAMPTZ )
            .withDefault( "'-infinity'" )
            .notNull();
    public static final PostgresColumnDefinition LAST_WRITE     = new PostgresColumnDefinition(
            LAST_WRITE_FIELD,
            TIMESTAMPTZ )
            .withDefault( "'-infinity'" )
            .notNull();
    public static final  PostgresColumnDefinition READERS        = new PostgresColumnDefinition(
            "readers",
            PostgresDatatype.UUID );
    public static final  PostgresColumnDefinition WRITERS        = new PostgresColumnDefinition(
            "writers",
            PostgresDatatype.UUID );

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
}
