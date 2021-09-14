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

import com.openlattice.search.requests.SearchResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class SearchResultStreamSerializerTest
        extends AbstractStreamSerializerTest<SearchResultStreamSerializer, SearchResult>
        implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 8177027116300219868L;

    @Override
    protected SearchResultStreamSerializer createSerializer() {
        return new SearchResultStreamSerializer();
    }

    @Override
    protected SearchResult createInput() {
        Map<String, Object> firstHit = Maps.newHashMap();
        Map<String, Object> secondHit = Maps.newHashMap();
        firstHit.put( "color", "green" );
        firstHit.put( "size", "seven" );
        secondHit.put( "color", "blue" );
        secondHit.put( "size", "four" );
        List<Map<String, Object>> hits = Lists.newArrayList();
        hits.add( firstHit );
        hits.add( secondHit );
        return new SearchResult( Long.parseLong( "2" ), hits );
    }

}
