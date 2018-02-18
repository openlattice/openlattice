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

package com.openlattice.hazelcast.serializers;

import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.core.type.TypeReference;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.hazelcast.StreamSerializerTypeIds;
import com.openlattice.search.requests.SearchResult;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class SearchResultStreamSerializer implements SelfRegisteringStreamSerializer<SearchResult> {

    private static final TypeReference<List<Map<String, Object>>> hitType = new TypeReference<List<Map<String, Object>>>() {
    };

    @Override
    public void write( ObjectDataOutput out, SearchResult object ) throws IOException {
        out.writeLong( object.getNumHits() );
        out.writeByteArray( ObjectMappers.getSmileMapper().writeValueAsBytes( object.getHits() ) );
    }

    @Override
    public SearchResult read( ObjectDataInput in ) throws IOException {
        long numHits = in.readLong();
        List<Map<String, Object>> hits = ObjectMappers.getSmileMapper().readValue( in.readByteArray(), hitType );
        return new SearchResult( numHits, hits );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.SEARCH_RESULT.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public Class<? extends SearchResult> getClazz() {
        return SearchResult.class;
    }

}
