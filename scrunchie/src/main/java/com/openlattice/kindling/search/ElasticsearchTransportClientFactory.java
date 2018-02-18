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

package com.openlattice.kindling.search;

import com.google.common.util.concurrent.RateLimiter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchTransportClientFactory {
    public static final Logger logger = LoggerFactory.getLogger( ElasticsearchTransportClientFactory.class );
    private static final RateLimiter r = RateLimiter.create( 1.0 / 30.0 );
    private String  clientTransportHost;
    private Integer clientTransportPort;
    private String  cluster;

    public ElasticsearchTransportClientFactory(
            String clientTransportHost,
            Integer clientTransportPort,
            String cluster ) {
        this.clientTransportHost = clientTransportHost;
        this.clientTransportPort = clientTransportPort;
        this.cluster = cluster;
    }

    public Client getClient() throws UnknownHostException {
        if ( this.clientTransportHost == null ) {
            logger.info( "no server passed in, logging to database" );
            return null;
        }

        logger.info( "getting kindling elasticsearch client on " + clientTransportHost + ":" + clientTransportPort
                + " with elasticsearch cluster " + cluster );
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        Settings settings = Settings.builder().put( "cluster.name", cluster ).build();
        TransportClient client = new PreBuiltTransportClient( settings );
        client.addTransportAddress( new TransportAddress(
                InetAddress.getByName( this.clientTransportHost ),
                this.clientTransportPort )
        );

        if ( isConnected( client ) ) {
            return client;
        } else {
            return null;
        }
    }

    public static boolean isConnected( Client someClient ) {
        if ( r.tryAcquire() ) {
            if ( someClient == null ) {
                logger.info( "not connected to elasticsearch" );
                return false;
            } else if ( someClient instanceof TransportClient ) {
                TransportClient client = (TransportClient) someClient;
                List<DiscoveryNode> nodes = client.connectedNodes();
                if ( nodes.isEmpty() ) {
                    logger.info( "no elasticsearch nodes found" );
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
                    logger.info( "not connected to elasticsearch" );
                    client.close();
                    return false;
                }
            }
        }
        return true;
    }

}
