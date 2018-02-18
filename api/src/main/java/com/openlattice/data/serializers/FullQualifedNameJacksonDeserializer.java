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

package com.openlattice.data.serializers;

import java.io.IOException;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class FullQualifedNameJacksonDeserializer extends StdDeserializer<FullQualifiedName> {
    private static final long serialVersionUID = 4245054290957537357L;

    protected FullQualifedNameJacksonDeserializer() {
        this( null );
    }

    public FullQualifedNameJacksonDeserializer( Class<FullQualifiedName> clazz ) {
        super( clazz );
    }

    public static void registerWithMapper( ObjectMapper mapper ) {
        SimpleModule module = new SimpleModule();
        module.addDeserializer( FullQualifiedName.class, new FullQualifedNameJacksonDeserializer() );
        mapper.registerModule( module );
    }

    @Override
    public FullQualifiedName deserialize( JsonParser jp, DeserializationContext ctxt )
            throws IOException, JsonProcessingException {
        
        JsonNode node = jp.getCodec().readTree( jp );
        return new FullQualifiedName(
                node.get( SerializationConstants.NAMESPACE_FIELD ).asText(),
                node.get( SerializationConstants.NAME_FIELD ).asText() );
    }
}
