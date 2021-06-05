

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

package com.openlattice.conductor.rpc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.rhizome.configuration.Configuration;
import com.kryptnostic.rhizome.configuration.ConfigurationKey;
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey;
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration;

import java.util.Objects;

@ReloadableConfiguration( uri = "mapbox.yaml" )
public class MapboxConfiguration implements Configuration {
    private static final long             serialVersionUID = -3847142110887587615L;
    private static final ConfigurationKey key              = new SimpleConfigurationKey( "mapbox.yaml" );

    private static final String MAPBOX_TOKEN_FIELD         = "mapboxToken";

    private final String              mapboxToken;

    @JsonCreator
    public MapboxConfiguration( @JsonProperty( MAPBOX_TOKEN_FIELD ) String mapboxToken ) {
        this.mapboxToken = mapboxToken;
    }

    @JsonProperty( MAPBOX_TOKEN_FIELD )
    public String getMapboxToken() {
        return mapboxToken;
    }

    @Override
    @JsonIgnore
    public ConfigurationKey getKey() {
        return key;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;
        MapboxConfiguration that = (MapboxConfiguration) o;
        return Objects.equals( mapboxToken, that.mapboxToken );
    }

    @Override public int hashCode() {
        return Objects.hash( mapboxToken );
    }

    @JsonIgnore
    public static ConfigurationKey key() {
        return key;
    }
}
