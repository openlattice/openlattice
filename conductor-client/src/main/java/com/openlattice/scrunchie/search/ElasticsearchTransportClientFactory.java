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

package com.openlattice.scrunchie.search;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ElasticsearchTransportClientFactory {
    public static final  Logger      logger = LoggerFactory.getLogger( ElasticsearchTransportClientFactory.class );
    private static final RateLimiter r      = RateLimiter.create( 1.0 / 30.0 );
    private String clientHost;
    private Integer clientTransportPort;
    private Integer clientRestPort;
    private String  cluster;

    public ElasticsearchTransportClientFactory(
            String clientHost,
            Integer clientTransportPort,
            Integer clientRestPort,
            String cluster ) {
        this.clientHost = clientHost;
        this.clientTransportPort = clientTransportPort;
        this.clientRestPort = clientRestPort;
        this.cluster = cluster;
    }

    public Client getClient() {
        if ( clientHost == null ) {
            logger.info( "no server passed in, logging to database" );
            return null;
        }

        logger.info( "getting kindling elasticsearch transport client on " + clientHost + ":" + clientTransportPort
                + " with elasticsearch cluster " + cluster );
        System.setProperty( "es.set.netty.runtime.available.processors", "false" );
        Settings settings = Settings.builder().put( "cluster.name", cluster ).build();
        TransportClient client = new PreBuiltTransportClient( settings );
        try {
            client.addTransportAddress( new TransportAddress(
                    InetAddress.getByName(clientHost),
                    clientTransportPort )
            );
        } catch ( UnknownHostException e ) {
            throw new IllegalStateException( "Unable to resolve elasticsearch host: " + this.clientHost, e );
        }

        if ( isConnected( client ) ) {
            return client;
        } else {
            return null;
        }
    }

    public RestHighLevelClient getRestClient() {
        if ( clientHost == null ) {
            logger.info( "no server passed in, logging to database" );
            return null;
        }

        logger.info( "getting kindling elasticsearch rest client on " + clientHost + ":" + clientRestPort
                + " with elasticsearch cluster " + cluster );
        System.setProperty( "es.set.netty.runtime.available.processors", "false" );

        try {
            RestHighLevelClient client = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(clientHost, clientRestPort, "http"))
            );
            if ( isRestConnected( client.getLowLevelClient() ) ) {
                return client;
            } else {
                return null;
            }
        } catch ( Exception e ) {
            throw new IllegalStateException( "Unable to resolve elasticsearch host: " + this.clientHost, e );
        }
    }

    public static boolean isRestConnected(RestClient restClient) {
        if ( !r.tryAcquire() ) {
            return true;
        }
        if ( restClient == null ) {
            logger.info( "not connected to elasticsearch" );
            return false;
        }
        List<Node> restNodes = restClient.getNodes();
        if (restNodes.isEmpty()) {
            try {
                restClient.close();
            } catch (IOException e) {
                logger.info("no elasticsearch nodes found. connection closed.");
                return false;
            }
        }
        logger.info( "connected to elasticsearch rest nodes: " + restNodes.toString() );
        return true;
    }

    public static boolean isConnected( Client someClient ) {
        if ( !r.tryAcquire() ) {
            return true;
        }
        if ( someClient == null ) {
            logger.info( "not connected to elasticsearch" );
            return false;
        }
        if ( someClient instanceof TransportClient ) {
            TransportClient client = (TransportClient) someClient;
            List<DiscoveryNode> nodes = client.connectedNodes();
            if ( nodes.isEmpty() ) {
                logger.error( "no elasticsearch nodes found" );
                client.close();
                return false;
            } else {
                logger.info( "connected to elasticsearch nodes: " + nodes.toString() );
                return true;
            }
        } else {
            NodeClient client = (NodeClient) someClient;
            ClusterStateRequest request = new ClusterStateRequest();
            Future<ClusterStateResponse> response = client.admin().cluster().state( request );
            try {
                response.get();
                logger.info( "connected to elasticsearch" );
                return true;
            } catch ( InterruptedException | ExecutionException e ) {
                logger.error( "not connected to elasticsearch" );
                client.close();
                return false;
            }
        }
    }

}
