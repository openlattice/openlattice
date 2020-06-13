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
 */

package com.openlattice.twilio;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.rhizome.configuration.Configuration;
import com.kryptnostic.rhizome.configuration.ConfigurationKey;
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey;
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration;

@ReloadableConfiguration( uri = "twilio.yaml" )
public class TwilioConfiguration implements Configuration {

    protected static ConfigurationKey key = new SimpleConfigurationKey(
            "twilio.yaml" );

    private static final String SID_PROPERTY        = "sid";
    private static final String TOKEN_PROPERTY      = "token";
    private static final String SHORT_CODE_PROPERTY = "shortCode";
    private static final String CALLBACK_BASE_URL   = "callbackBaseUrl";
    private static final String CODEX_ENABLED       = "codexEnabled";

    private final String sid;
    private final String token;
    private final String shortCode;
    private final String callbackBaseUrl;
    private final boolean codexEnabled;

    @JsonCreator
    public TwilioConfiguration(
            @JsonProperty( SID_PROPERTY ) String sid,
            @JsonProperty( TOKEN_PROPERTY ) String token,
            @JsonProperty( SHORT_CODE_PROPERTY ) String shortCode,
            @JsonProperty( CALLBACK_BASE_URL ) String callbackBaseUrl,
            @JsonProperty( CODEX_ENABLED ) boolean codexEnabled ) {

        this.sid = sid;
        this.token = token;
        this.shortCode = shortCode;
        this.callbackBaseUrl = callbackBaseUrl;
        this.codexEnabled = codexEnabled;
    }

    @JsonProperty( SID_PROPERTY )
    public String getSid() {
        return sid;
    }

    @JsonProperty( TOKEN_PROPERTY )
    public String getToken() {
        return token;
    }

    @JsonProperty( SHORT_CODE_PROPERTY )
    public String getShortCode() {
        return shortCode;
    }

    @JsonProperty( CALLBACK_BASE_URL )
    public String getCallbackBaseUrl() {
        return callbackBaseUrl;
    }

    @JsonProperty( CODEX_ENABLED )
    public boolean isCodexEnabled() {
        return codexEnabled;
    }

    @Override
    @JsonIgnore
    public ConfigurationKey getKey() {
        return key;
    }

    @JsonIgnore
    public static ConfigurationKey key() {
        return key;
    }
}
