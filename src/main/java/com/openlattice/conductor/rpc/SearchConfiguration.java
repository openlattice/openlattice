

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

package com.openlattice.conductor.rpc;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Optional;
import javax.xml.bind.annotation.XmlType.DEFAULT;

public class SearchConfiguration implements Serializable {
    private static final long   serialVersionUID      = 1018452800248369401L;
    private static final String ELASTICSEARCH_URL     = "elasticsearchUrl";
    private static final String ELASTICSEARCH_CLUSTER = "elasticsearchCluster";
    private static final String ELASTICSEARCH_PORT    = "elasticsearchPort";
    private static final String NUM_REPLICAS          = "numReplicas";
    private static final String NUM_SHARDS            = "numShards";

    private static final int    DEFAULT_NUM_REPLICAS  = 2;
    private static final int    DEFAULT_NUM_SHARDS    = 5;

    private final String elasticsearchUrl;
    private final String elasticsearchCluster;
    private final int    elasticsearchPort;
    private final int    numReplicas;
    private final int    numShards;

    @JsonCreator
    public SearchConfiguration(
            @JsonProperty( ELASTICSEARCH_URL ) String elasticsearchUrl,
            @JsonProperty( ELASTICSEARCH_CLUSTER ) String elasticsearchCluster,
            @JsonProperty( ELASTICSEARCH_PORT ) int elasticsearchPort,
            @JsonProperty( NUM_REPLICAS ) Optional<Integer> numReplicas,
            @JsonProperty( NUM_SHARDS ) Optional<Integer> numShards ) {
        this.elasticsearchUrl = elasticsearchUrl;
        this.elasticsearchCluster = elasticsearchCluster;
        this.elasticsearchPort = elasticsearchPort;
        this.numReplicas = numReplicas.orElse( DEFAULT_NUM_REPLICAS );
        this.numShards = numShards.orElse( DEFAULT_NUM_SHARDS );
    }

    @JsonProperty( ELASTICSEARCH_URL )
    public String getElasticsearchUrl() {
        return elasticsearchUrl;
    }

    @JsonProperty( ELASTICSEARCH_CLUSTER )
    public String getElasticsearchCluster() {
        return elasticsearchCluster;
    }

    @JsonProperty( ELASTICSEARCH_PORT )
    public int getElasticsearchPort() {
        return elasticsearchPort;
    }

    @JsonProperty( NUM_REPLICAS )
    public int getNumReplicas() {
        return numReplicas;
    }

    @JsonProperty( NUM_SHARDS )
    public int getNumShards() {
        return numShards;
    }
}

