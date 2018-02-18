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

package com.openlattice.authorization.paging;

import com.dataloom.mappers.ObjectMappers;
import com.datastax.driver.core.PagingState;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.openlattice.authorization.Principal;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthorizedObjectsPagingFactory {
    private static final Logger logger = LoggerFactory.getLogger( AuthorizedObjectsPagingFactory.class );

    private static final Encoder encoder = Base64.getUrlEncoder();
    private static final Decoder decoder = Base64.getUrlDecoder();

    private AuthorizedObjectsPagingFactory() {
    }

    public static AuthorizedObjectsPagingInfo createSafely( Principal principal, PagingState pagingState ) {
        return ( principal == null ) ? null : new AuthorizedObjectsPagingInfo( principal, pagingState );
    }

    public static String encode( AuthorizedObjectsPagingInfo info ) {
        if ( info == null ) {
            return null;
        }

        final byte[] bytes;
        try {
            bytes = ObjectMappers.getSmileMapper().writeValueAsBytes( info );
        } catch ( JsonProcessingException e ) {
            logger.error( "Error serializing AuthorizedObjectsPagingInfo: " + info );
            return null;
        }
        return new String( encoder.encode( bytes ), StandardCharsets.UTF_8 );
    }

    public static AuthorizedObjectsPagingInfo decode( String token ) {
        if ( token == null ) {
            return null;
        }

        final byte[] bytes = token.getBytes( StandardCharsets.UTF_8 );

        try {
            return ObjectMappers.getSmileMapper().readValue( decoder.decode( bytes ),
                    AuthorizedObjectsPagingInfo.class );
        } catch ( IOException e ) {
            logger.error( "Error deserializing AuthorizedObjectsPagingInfo." );
            return null;
        }
    }
}
