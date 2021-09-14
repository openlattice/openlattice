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

package com.openlattice;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Deprecated( since = "Use ApiHelpers.kt instead")
public class ApiUtil {
    private static final Encoder encoder = Base64.getEncoder();
    private static final Logger  logger  = LoggerFactory.getLogger( ApiUtil.class );

    private ApiUtil() {}

    public static String generateDefaultEntityId( List<UUID> keys, Map<UUID, Set<Object>> entityDetails ) {
        return generateDefaultEntityId(
                Preconditions.checkNotNull( keys, "Key properties must be configured for entity id generation." )
                        .stream(),
                entityDetails );
    }

    public static String generateDefaultEntityId( Stream<UUID> keys, Map<UUID, Set<Object>> entityDetails ) {
        String entityId = keys.map( entityDetails::get )
                .filter( Objects::nonNull )
                .map( ApiUtil::joinObjectsAsString )
                .map( ApiHelpers::toUtf8Bytes )
                .map( encoder::encodeToString )
                .collect( Collectors.joining( "," ) );
        Preconditions.checkArgument( entityId.length() > 0, "Entity ids cannot be empty strings" );
        return entityId;
    }

    private static String joinObjectsAsString( Set<Object> s ) {
        return s.stream()
                .map( ApiUtil::toBytes )
                .map( ByteBuffer::wrap )
                .sorted()
                .map( ByteBuffer::array )
                .map( encoder::encodeToString )
                .collect( Collectors.joining( "," ) );
    }

    protected static byte[] toBytes( Object o ) {
        if ( o instanceof String ) {
            return ApiHelpers.toUtf8Bytes( (String) o );
        }
        try {
            return ObjectMappers.getJsonMapper().writeValueAsBytes( o );
        } catch ( JsonProcessingException e ) {
            logger.error( "Unable to serialize object for building entity id", e );
            return new byte[ 0 ];
        }
    }
}
