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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.openlattice.client.serialization.SerializationConstants;
import java.io.IOException;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

public class FullQualifiedNameJacksonSerializer {
    private static final SimpleModule module           =
            new SimpleModule( FullQualifiedNameJacksonSerializer.class.getName() );

    static {
        module.addSerializer( FullQualifiedName.class, new FullQualifiedNameJacksonSerializer.Serializer() );
        module.addDeserializer( FullQualifiedName.class, new FullQualifiedNameJacksonSerializer.Deserializer() );
    }

    public static void registerWithMapper( ObjectMapper mapper ) {
        mapper.registerModule( module );
    }

    public static class Serializer extends StdSerializer<FullQualifiedName> {

        public Serializer() {
            this( FullQualifiedName.class );
        }

        public Serializer( Class<FullQualifiedName> clazz ) {
            super( clazz );
        }

        @Override
        public void serialize( FullQualifiedName value, JsonGenerator jgen, SerializerProvider provider )
                throws IOException {
            jgen.writeStartObject();
            jgen.writeStringField( SerializationConstants.NAMESPACE_FIELD, value.getNamespace() );
            jgen.writeStringField( SerializationConstants.NAME_FIELD, value.getName() );
            jgen.writeEndObject();
        }
    }

    public static class Deserializer extends StdDeserializer<FullQualifiedName> {

        public Deserializer() {
            this( FullQualifiedName.class );
        }

        protected Deserializer( Class<FullQualifiedName> clazz ) {
            super( clazz );
        }

        @Override
        public FullQualifiedName deserialize( JsonParser jp, DeserializationContext ctxt ) throws IOException {
            JsonNode node = jp.getCodec().readTree( jp );
            return new FullQualifiedName(
                    node.get( SerializationConstants.NAMESPACE_FIELD ).asText(),
                    node.get( SerializationConstants.NAME_FIELD ).asText() );
        }

    }
}
