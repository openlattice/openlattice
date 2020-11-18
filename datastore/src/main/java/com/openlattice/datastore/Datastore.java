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

package com.openlattice.datastore;

import com.dataloom.mappers.ObjectMappers;
import com.kryptnostic.rhizome.configuration.websockets.BaseRhizomeServer;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils.Pods;
import com.openlattice.auditing.pods.AuditingConfigurationPod;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.aws.AwsS3Pod;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.datastore.pods.ByteBlobServicePod;
import com.openlattice.datastore.pods.DatastoreSecurityPod;
import com.openlattice.datastore.pods.DatastoreServicesPod;
import com.openlattice.datastore.pods.DatastoreServletsPod;
import com.openlattice.hazelcast.pods.HazelcastQueuePod;
import com.openlattice.hazelcast.pods.MapstoresPod;
import com.openlattice.hazelcast.pods.NearCachesPod;
import com.openlattice.hazelcast.pods.SharedStreamSerializersPod;
import com.openlattice.jdbc.JdbcPod;
import com.openlattice.postgres.PostgresPod;
import com.openlattice.postgres.pods.ExternalDatabaseConnectionManagerPod;
import com.openlattice.principals.PermissionsManagerPod;
import com.openlattice.tasks.pods.TaskSchedulerPod;
import com.openlattice.transporter.TransporterConfigurationPod;
import com.openlattice.transporter.pods.TransporterPod;

public class Datastore extends BaseRhizomeServer {
    private static final Class<?>[] datastorePods = new Class<?>[] {
            AuditingConfigurationPod.class,
            Auth0Pod.class,
            AwsS3Pod.class,
            ByteBlobServicePod.class,
            DatastoreServicesPod.class,
            ExternalDatabaseConnectionManagerPod.class,
            HazelcastQueuePod.class,
            JdbcPod.class,
            MapstoresPod.class,
            NearCachesPod.class,
            PermissionsManagerPod.class,
            PostgresPod.class,
            SharedStreamSerializersPod.class,
            TaskSchedulerPod.class,
            TransporterPod.class,
            TransporterConfigurationPod.class
    };

    private static final Class<?>[] webPods       = new Class<?>[] {
            DatastoreServletsPod.class,
            DatastoreSecurityPod.class, };

    static {
        ObjectMappers.foreach( FullQualifiedNameJacksonSerializer::registerWithMapper );
    }

    public Datastore( Class<?>... pods ) {
        super( Pods.concatenate(
                pods,
                webPods,
                RhizomeApplicationServer.DEFAULT_PODS,
                datastorePods ) );
    }

    @Override public void start( String... profiles ) throws Exception {
        super.start( profiles );
    }

    public static void main( String[] args ) throws Exception {
        Datastore datastore = new Datastore();
        datastore.start( args );
    }
}
