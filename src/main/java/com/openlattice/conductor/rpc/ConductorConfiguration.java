

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

@ReloadableConfiguration( uri = "conductor.yaml" )
public class ConductorConfiguration implements Configuration {
    private static final long             serialVersionUID = -3847142110887587615L;
    private static final ConfigurationKey key              = new SimpleConfigurationKey( "conductor.yaml" );

    private static final String REPORT_EMAIL_ADDRESS_FIELD = "reportEmailAddress";
    private static final String SEARCH_CONFIGURATION_FIELD = "searchConfiguration";

    private final String              reportEmailAddress;
    private final SearchConfiguration searchConfiguration;

    @JsonCreator
    public ConductorConfiguration(
            @JsonProperty( REPORT_EMAIL_ADDRESS_FIELD ) String reportEmailAddress,
            @JsonProperty( SEARCH_CONFIGURATION_FIELD ) SearchConfiguration searchConfiguration ) {
        this.reportEmailAddress = reportEmailAddress;
        this.searchConfiguration = searchConfiguration;
    }

    @JsonProperty( REPORT_EMAIL_ADDRESS_FIELD )
    public String getReportEmailAddress() {
        return reportEmailAddress;
    }

    @JsonProperty( SEARCH_CONFIGURATION_FIELD )
    public SearchConfiguration getSearchConfiguration() {
        return searchConfiguration;
    }

    @Override
    @JsonIgnore
    public ConfigurationKey getKey() {
        return key;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof ConductorConfiguration ) ) { return false; }
        ConductorConfiguration that = (ConductorConfiguration) o;
        return Objects.equals( reportEmailAddress, that.reportEmailAddress ) &&
                Objects.equals( searchConfiguration, that.searchConfiguration );
    }

    @Override public int hashCode() {

        return Objects.hash( reportEmailAddress, searchConfiguration );
    }

    @Override public String toString() {
        return "ConductorConfiguration{" +
                "reportEmailAddress='" + reportEmailAddress + '\'' +
                ", searchConfiguration=" + searchConfiguration +
                '}';
    }

    @JsonIgnore
    public static ConfigurationKey key() {
        return key;
    }
}
