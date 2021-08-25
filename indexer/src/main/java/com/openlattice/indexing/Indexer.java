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

import com.kryptnostic.rhizome.configuration.websockets.BaseRhizomeServer;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils.Pods;
import com.openlattice.auditing.pods.AuditingConfigurationPod;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.aws.AwsS3Pod;
import com.openlattice.datastore.pods.ByteBlobServicePod;
import com.openlattice.hazelcast.pods.HazelcastQueuePod;
import com.openlattice.hazelcast.pods.MapstoresPod;
import com.openlattice.hazelcast.pods.SharedStreamSerializersPod;
import com.openlattice.indexing.pods.IndexerPostConfigurationServicesPod;
import com.openlattice.indexing.pods.IndexerSecurityPod;
import com.openlattice.indexing.pods.IndexerServicesPod;
import com.openlattice.indexing.pods.IndexerServletsPod;
import com.openlattice.indexing.pods.PlasmaCoupling;
import com.openlattice.ioc.providers.LateInitProvidersPod;
import com.openlattice.jdbc.JdbcPod;
import com.openlattice.postgres.PostgresPod;
import com.openlattice.postgres.pods.ExternalDatabaseConnectionManagerPod;
import com.openlattice.transporter.TransporterConfigurationPod;
import com.openlattice.transporter.pods.TransporterPod;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Indexer extends BaseRhizomeServer {

    private static final Class<?>[] indexerPods = new Class<?>[]{
            AuditingConfigurationPod.class,
            Auth0Pod.class,
            AwsS3Pod.class,
            ByteBlobServicePod.class,
            ExternalDatabaseConnectionManagerPod.class,
            HazelcastQueuePod.class,
            IndexerServicesPod.class,
            IndexerPostConfigurationServicesPod.class,
            JdbcPod.class,
            MapstoresPod.class,
            PlasmaCoupling.class,
            PostgresPod.class,
            SharedStreamSerializersPod.class,
            TransporterPod.class,
            TransporterConfigurationPod.class,
            LateInitProvidersPod.class
    };

    private static final Class<?>[] webPods = new Class<?>[]{ IndexerServletsPod.class, IndexerSecurityPod.class };

    public Indexer() {
        super( Pods.concatenate( webPods, RhizomeApplicationServer.DEFAULT_PODS, indexerPods ) );
    }

    @Override
    public void start( String... profiles ) throws Exception {
        super.start( profiles );
    }

    public static void main( String[] args ) throws Exception {
        new Indexer().start( args );
    }
}
