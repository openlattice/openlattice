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

package com.openlattice.search.requests;

import java.util.List;
import java.util.Map;

public class SearchResult {

    private final long                      numHits;
    private final List<Map<String, Object>> hits;

    public SearchResult( long numHits, List<Map<String, Object>> hits ) {
        this.numHits = numHits;
        this.hits = hits;
    }

    public long getNumHits() {
        return numHits;
    }

    public List<Map<String, Object>> getHits() {
        return hits;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( hits == null ) ? 0 : hits.hashCode() );
        result = prime * result + (int) ( numHits ^ ( numHits >>> 32 ) );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        SearchResult other = (SearchResult) obj;
        if ( hits == null ) {
            if ( other.hits != null ) return false;
        } else if ( !hits.equals( other.hits ) ) return false;
        if ( numHits != other.numHits ) return false;
        return true;
    }
}