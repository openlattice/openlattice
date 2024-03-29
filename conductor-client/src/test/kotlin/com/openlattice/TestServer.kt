/*
 * Copyright (C) 2019. OpenLattice, Inc.
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
 *
 */
package com.openlattice

import com.google.common.eventbus.EventBus
import com.hazelcast.core.HazelcastInstance
import com.geekbeast.rhizome.configuration.ConfigurationConstants
import com.geekbeast.rhizome.core.RhizomeApplicationServer
import com.geekbeast.rhizome.pods.ConfigurationLoaderPod
import com.openlattice.assembler.pods.AssemblerConfigurationPod
import com.openlattice.auditing.pods.AuditingConfigurationPod
import com.geekbeast.auth0.Auth0Pod
import com.openlattice.data.storage.DataSourceResolver
import com.openlattice.datastore.constants.DatastoreProfiles
import com.openlattice.edm.PostgresEdmManager
import com.openlattice.test.DataSourceResolverPodForTests
import com.openlattice.hazelcast.pods.MapstoresPod
import com.openlattice.hazelcast.pods.SharedStreamSerializersPod
import com.openlattice.hazelcast.pods.TestPod
import com.openlattice.ioc.providers.LateInitProvidersPod
import com.geekbeast.jdbc.JdbcPod
import com.geekbeast.postgres.PostgresPod
import com.openlattice.postgres.PostgresTablesPod
import com.openlattice.postgres.pods.ExternalDatabaseConnectionManagerPod
import com.zaxxer.hikari.HikariDataSource

open class TestServer {
    companion object {

        @JvmField
        val testServer = RhizomeApplicationServer(
            Auth0Pod::class.java,
            AssemblerConfigurationPod::class.java,
            MapstoresPod::class.java,
            JdbcPod::class.java,
            PostgresPod::class.java,
            SharedStreamSerializersPod::class.java,
            PostgresTablesPod::class.java,
            AuditingConfigurationPod::class.java,
            ExternalDatabaseConnectionManagerPod::class.java,
            DataSourceResolverPodForTests::class.java,
            LateInitProvidersPod::class.java,
            TestPod::class.java,
            ConfigurationLoaderPod::class.java
        )

        @JvmField
        val hazelcastInstance: HazelcastInstance

        @JvmField
        val hds: HikariDataSource

        @JvmField
        val dsr: DataSourceResolver

        init {
            testServer.sprout(
                ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE, PostgresPod.PROFILE,
                DatastoreProfiles.MEDIA_LOCAL_PROFILE)

            hazelcastInstance = testServer.context.getBean(HazelcastInstance::class.java)
            //This should work as tests aren't sharded all will all share the default datasource
            hds = testServer.context.getBean(HikariDataSource::class.java)
            dsr = testServer.context.getBean(DataSourceResolver::class.java)

            val edm = PostgresEdmManager(hds)

            testServer.context.getBean(EventBus::class.java)
                    .register(edm)
        }
    }
}
