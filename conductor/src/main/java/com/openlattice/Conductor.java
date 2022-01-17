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

package com.openlattice;

import com.geekbeast.mappers.mappers.ObjectMappers;
import com.geekbeast.rhizome.core.RhizomeApplicationServer;
import com.geekbeast.auth0.Auth0Pod;
import com.geekbeast.aws.AwsS3Pod;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.datastore.pods.ByteBlobServicePod;
import com.openlattice.hazelcast.pods.HazelcastQueuePod;
import com.openlattice.hazelcast.pods.MapstoresPod;
import com.openlattice.hazelcast.pods.SharedStreamSerializersPod;
import com.openlattice.ioc.providers.LateInitProvidersPod;
import com.geekbeast.jdbc.JdbcPod;
import com.openlattice.mail.pods.MailServicePod;
import com.openlattice.pods.ConductorEdmSyncPod;
import com.openlattice.pods.ConductorPostInitializationPod;
import com.openlattice.pods.ConductorServicesPod;
import com.geekbeast.postgres.PostgresPod;
import com.openlattice.postgres.PostgresTablesPod;
import com.openlattice.postgres.pods.ExternalDatabaseConnectionManagerPod;
import com.geekbeast.pods.TaskSchedulerPod;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Conductor extends RhizomeApplicationServer {
    static final Class<?>[] conductorPods = new Class<?>[] {
            Auth0Pod.class,
            AwsS3Pod.class,
            ByteBlobServicePod.class,
            ConductorPostInitializationPod.class,
            ConductorServicesPod.class,
            ConductorEdmSyncPod.class,
            ExternalDatabaseConnectionManagerPod.class,
            HazelcastQueuePod.class,
            JdbcPod.class,
            MailServicePod.class,
            MapstoresPod.class,
            PlasmaCoupling.class,
            PostgresPod.class,
            PostgresTablesPod.class,
            SharedStreamSerializersPod.class,
            TaskSchedulerPod.class,
            // TransporterPod.class,
            // TransporterInitPod.class,
            // TransporterConfigurationPod.class,
            LateInitProvidersPod.class
    };

    static {
        ObjectMappers.foreach( FullQualifiedNameJacksonSerializer::registerWithMapper );
    }

    public Conductor() {
        super( conductorPods );
    }

    @Override
    public void sprout( String... activeProfiles ) {
        super.sprout( activeProfiles );
    }

    public static void main( String[] args ) {
        new Conductor().sprout( args );
    }
}
