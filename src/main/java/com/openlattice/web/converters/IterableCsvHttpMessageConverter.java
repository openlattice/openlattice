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

import java.io.IOException;
import java.lang.reflect.Type;

import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.AbstractGenericHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.collect.Multimap;

public class IterableCsvHttpMessageConverter
        extends AbstractGenericHttpMessageConverter<Iterable<Multimap<String, ?>>> {

    private final CsvMapper csvMapper = new CsvMapper();

    public IterableCsvHttpMessageConverter() {
        csvMapper.registerModule( new AfterburnerModule() );
        csvMapper.registerModule( new GuavaModule() );
        csvMapper.registerModule( new JodaModule() );
    }

    @Override
    public Iterable<Multimap<String, ?>> read( Type type, Class<?> contextClass, HttpInputMessage inputMessage )
            throws IOException, HttpMessageNotReadableException {
        throw new UnsupportedOperationException( "CSV is not a supported input format" );
    }

    @Override
    protected void writeInternal( Iterable<Multimap<String, ?>> t, Type type, HttpOutputMessage outputMessage )
            throws IOException, HttpMessageNotWritableException {
        CsvSchema schema = null;

        for ( Multimap<String, ?> obj : t ) {
            if ( schema == null ) {
                schema = fromMultimap( obj );
            }
            // TODO: Flatten or drop multiple output values into obj.
            csvMapper.writer( schema ).writeValue( outputMessage.getBody(), obj.asMap() );
        }

    }

    @Override
    protected boolean supports( Class<?> clazz ) {
        return Iterable.class.isAssignableFrom( clazz );
    }

    @Override
    protected Iterable<Multimap<String, ?>> readInternal(
            Class<? extends Iterable<Multimap<String, ?>>> clazz,
            HttpInputMessage inputMessage )
            throws IOException, HttpMessageNotReadableException {
        throw new UnsupportedOperationException( "CSV is not a supported input format" );
    }

    public CsvSchema fromMultimap( Multimap<String, ?> m ) {
        Builder schemaBuilder = CsvSchema.builder();

        m.keySet().stream().forEach( type -> schemaBuilder.addColumn( type, ColumnType.ARRAY ) );
        schemaBuilder.setUseHeader( true );
        return schemaBuilder.build();
    }

}
