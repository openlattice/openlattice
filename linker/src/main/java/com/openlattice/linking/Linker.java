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

package com.openlattice.linking;

import com.dataloom.mappers.ObjectMappers;
import com.kryptnostic.rhizome.configuration.websockets.BaseRhizomeServer;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils.Pods;
import com.openlattice.auditing.pods.AuditingConfigurationPod;
import com.openlattice.auth0.Auth0Pod;
import com.openlattice.aws.AwsS3Pod;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.hazelcast.pods.HazelcastQueuePod;
import com.openlattice.hazelcast.pods.MapstoresPod;
import com.openlattice.hazelcast.pods.SharedStreamSerializersPod;
import com.openlattice.ioc.providers.LateInitProvidersPod;
import com.openlattice.jdbc.JdbcPod;
import com.openlattice.linking.pods.LinkerPostConfigurationServicesPod;
import com.openlattice.linking.pods.LinkerSecurityPod;
import com.openlattice.linking.pods.LinkerServicesPod;
import com.openlattice.linking.pods.LinkerServletsPod;
import com.openlattice.linking.pods.PlasmaCoupling;
import com.openlattice.postgres.PostgresPod;
import com.openlattice.postgres.PostgresTablesPod;
import com.openlattice.postgres.pods.ExternalDatabaseConnectionManagerPod;
import com.openlattice.tasks.pods.TaskSchedulerPod;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Linker extends BaseRhizomeServer {

    private static final Class<?>[] conductorPods = new Class<?>[]{
            AuditingConfigurationPod.class,
            Auth0Pod.class,
            AwsS3Pod.class,
            ExternalDatabaseConnectionManagerPod.class,
            JdbcPod.class,
            LinkerPostConfigurationServicesPod.class,
            LinkerServicesPod.class,
            MapstoresPod.class,
            HazelcastQueuePod.class,
            PlasmaCoupling.class,
            PostgresPod.class,
            SharedStreamSerializersPod.class,
            TaskSchedulerPod.class,
            LateInitProvidersPod.class
    };

    private static final Class<?>[] webPods = new Class<?>[]{ LinkerServletsPod.class, LinkerSecurityPod.class };

    static {
        ObjectMappers.foreach( FullQualifiedNameJacksonSerializer::registerWithMapper );
    }

    public Linker() {
        super( Pods.concatenate( RhizomeApplicationServer.DEFAULT_PODS, webPods, conductorPods ) );
    }

    @Override
    public void start( String... profiles ) throws Exception {
        super.start( profiles );
    }

    public static void main( String[] args ) throws Exception {
        new Linker().start( args );
    }
}
