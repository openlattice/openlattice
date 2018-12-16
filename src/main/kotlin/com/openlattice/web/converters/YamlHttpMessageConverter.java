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

package com.openlattice.web.converters;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import com.openlattice.web.mediatypes.CustomMediaType;
import java.io.IOException;
import java.lang.reflect.Type;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.AbstractGenericHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

public class YamlHttpMessageConverter extends AbstractGenericHttpMessageConverter<Object> {

    private final ObjectMapper mapper = ObjectMappers.getYamlMapper();
    
    public YamlHttpMessageConverter() {
        super( CustomMediaType.TEXT_YAML );
    }
    
    @Override
    public Iterable<Multimap<FullQualifiedName, ?>> read(
            Type type,
            Class<?> contextClass,
            HttpInputMessage inputMessage ) throws IOException, HttpMessageNotReadableException {
        throw new UnsupportedOperationException( "YAML is not a supported input format" );
    }

    @Override
    protected void writeInternal(
            Object t,
            Type type,
            HttpOutputMessage outputMessage ) throws IOException, HttpMessageNotWritableException {
        mapper.writeValue( outputMessage.getBody(), t );
        
    }

    @Override
    protected boolean supports( Class<?> clazz ) {
        return Object.class.isAssignableFrom( clazz );
    }

    @Override
    protected Object readInternal(
            Class<? extends Object> clazz,
            HttpInputMessage inputMessage ) throws IOException, HttpMessageNotReadableException {
        throw new UnsupportedOperationException( "YAML is not a supported input format" );
    }

}
