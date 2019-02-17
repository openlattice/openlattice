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
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod;
import com.openlattice.auditing.pods.AuditingConfigurationPod;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.aws.AwsS3Pod;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.datastore.pods.ByteBlobServicePod;
import com.openlattice.datastore.pods.DatastoreNeuronPod;
import com.openlattice.datastore.pods.DatastoreSecurityPod;
import com.openlattice.datastore.pods.DatastoreServicesPod;
import com.openlattice.datastore.pods.DatastoreServletsPod;
import com.openlattice.hazelcast.pods.SharedStreamSerializersPod;
import com.openlattice.jdbc.JdbcPod;
import com.openlattice.postgres.PostgresPod;

public class Datastore extends BaseRhizomeServer {
    public static final Class<?>[] datastorePods = new Class<?>[] {
            ByteBlobServicePod.class,
            DatastoreServicesPod.class,
            SharedStreamSerializersPod.class,
            AwsS3Pod.class,
            JdbcPod.class,
            DatastoreNeuronPod.class,
            PostgresPod.class,
            AuditingConfigurationPod.class
    };
    public static final Class<?>[] rhizomePods   = new Class<?>[] {
            RegistryBasedHazelcastInstanceConfigurationPod.class,
            Auth0Pod.class };
    public static final Class<?>[] webPods       = new Class<?>[] {
            DatastoreServletsPod.class,
            DatastoreSecurityPod.class, };

    static {
        ObjectMappers.foreach( FullQualifiedNameJacksonSerializer::registerWithMapper );
    }

    public Datastore( Class<?>... pods ) {
        super( Pods.concatenate(
                pods,
                webPods,
                rhizomePods,
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
