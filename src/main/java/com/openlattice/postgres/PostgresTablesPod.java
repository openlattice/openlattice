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

package com.openlattice.postgres;

import java.lang.reflect.Modifier;
import java.util.stream.Stream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * When included as a pod this class automatically registers core openlattice tables for running the system.
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
public class PostgresTablesPod {
    @Bean
    public PostgresTables postgresTables() {
        return () -> Stream.concat(
                Stream.of( PostgresTable.class.getFields() ),
                Stream.of( PostgresTable.class.getDeclaredFields() ) )
                .filter( field -> Modifier.isStatic( field.getModifiers() )
                        && Modifier.isFinal( field.getModifiers() ) )
                .filter( field -> field.getType().equals( PostgresTableDefinition.class ) )
                .map( field -> {
                    try {
                        return (PostgresTableDefinition) field.get( null );
                    } catch ( IllegalAccessException e ) {
                        return null;
                    }
                } )
                .filter( type -> type != null );
    }
}
