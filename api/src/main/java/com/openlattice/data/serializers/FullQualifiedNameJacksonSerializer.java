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
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class FullQualifiedNameJacksonSerializer extends StdSerializer<FullQualifiedName> {
    private static final long         serialVersionUID = 642017294181795076L;
    private static final SimpleModule module           = new SimpleModule();

    static {
        module.addSerializer( FullQualifiedName.class, new FullQualifiedNameJacksonSerializer() );
    }

    public FullQualifiedNameJacksonSerializer() {
        this( FullQualifiedName.class );
    }

    public FullQualifiedNameJacksonSerializer( Class<FullQualifiedName> clazz ) {
        super( clazz );
    }

    @Override
    public void serialize( FullQualifiedName value, JsonGenerator jgen, SerializerProvider provider )
            throws IOException, JsonGenerationException {
        jgen.writeStartObject();
        jgen.writeStringField( SerializationConstants.NAMESPACE_FIELD, value.getNamespace() );
        jgen.writeStringField( SerializationConstants.NAME_FIELD, value.getName() );
        jgen.writeEndObject();
    }

    public static void registerWithMapper( ObjectMapper mapper ) {
        mapper.registerModule( module );
    }
}
