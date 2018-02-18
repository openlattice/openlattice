/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.exceptions;

import java.net.HttpURLConnection;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonFormat(
    shape = JsonFormat.Shape.OBJECT )
public enum ApiExceptions {
    RESOURCE_NOT_FOUND_EXCEPTION( "resourceNotFound", HttpURLConnection.HTTP_NOT_FOUND ),
    ILLEGAL_ARGUMENT_EXCEPTION( "illegalArgument", HttpURLConnection.HTTP_BAD_REQUEST ),
    ILLEGAL_STATE_EXCEPTION( "illegalState", HttpURLConnection.HTTP_INTERNAL_ERROR ),
    TYPE_EXISTS_EXCEPTION( "typeExists", HttpURLConnection.HTTP_CONFLICT ),
    FORBIDDEN_EXCEPTION( "forbidden", HttpURLConnection.HTTP_FORBIDDEN ),
    TOKEN_REFRESH_EXCEPTION( "tokenNeedsRefresh", HttpURLConnection.HTTP_UNAUTHORIZED ),
    AUTH0_TOKEN_EXCEPTION( "tokenError", HttpURLConnection.HTTP_UNAUTHORIZED ),
    OTHER_EXCEPTION( "otherError", HttpURLConnection.HTTP_INTERNAL_ERROR );

    private String type;
    private int    code;

    private ApiExceptions( String type, int code ) {
        this.type = type;
        this.code = code;
    }

    @JsonProperty( SerializationConstants.TYPE_FIELD )
    public String getType() {
        return type;
    }

    @JsonProperty( SerializationConstants.HTTP_STATUS_CODE )
    public int getCode() {
        return code;
    }
}
