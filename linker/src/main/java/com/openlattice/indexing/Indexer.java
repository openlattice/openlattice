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

package com.openlattice.indexing;

import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils.Pods;
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod;
import com.openlattice.indexing.pods.*;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.aws.AwsS3Pod;
import com.openlattice.datastore.cassandra.CassandraTablesPod;
import com.openlattice.hazelcast.pods.MapstoresPod;
import com.openlattice.hazelcast.pods.SharedStreamSerializersPod;
import com.openlattice.jdbc.JdbcPod;
import com.openlattice.mail.pods.MailServicePod;
import com.openlattice.mail.services.MailService;
import com.openlattice.postgres.PostgresPod;
import com.openlattice.postgres.PostgresTablesPod;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Indexer extends RhizomeApplicationServer {
    public static final Class<?>[] rhizomePods = new Class<?>[]{
            RegistryBasedHazelcastInstanceConfigurationPod.class,
            Auth0Pod.class };

    public static final Class<?>[] conductorPods = new Class<?>[]{
            IndexerPostConfigurationServicesPod.class,
            IndexerServicesPod.class,
            SharedStreamSerializersPod.class,
            PlasmaCoupling.class,
            MailServicePod.class,
            Auth0Pod.class,
            CassandraTablesPod.class,
            MapstoresPod.class,
            JdbcPod.class,
            PostgresTablesPod.class,
            PostgresPod.class,
            Auth0Pod.class,
            AwsS3Pod.class,
            GraphProcessorPod.class
    };

    public static final Class<?>[] webPods = new Class<?>[]{ IndexerServletsPod.class, IndexerSecurityPod.class };

    public Indexer() {
        super( Pods.concatenate( RhizomeApplicationServer.DEFAULT_PODS, webPods, rhizomePods, conductorPods ) );
    }

    @Override
    public void sprout( String... activeProfiles ) {
        super.sprout( activeProfiles );
        getContext().getBean( MailService.class ).processEmailRequestsQueue();
    }

    public static void main( String[] args ) {
        new Indexer().sprout( args );
    }
}
