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

package com.geekbeast.tasks

import com.geekbeast.tasks.HazelcastInitializationTask
import com.geekbeast.tasks.HazelcastTaskDependencies
import com.openlattice.data.DataGraphService
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.DataSourceResolver
import com.openlattice.hazelcast.serializers.decorators.ByteBlobDataManagerAware
import com.openlattice.hazelcast.serializers.decorators.DataGraphAware
import com.openlattice.hazelcast.serializers.decorators.MetastoreAware
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.ids.IdGenerationServiceDependent
import com.openlattice.ioc.providers.LateInitAware
import com.openlattice.ioc.providers.LateInitProvider
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import javax.inject.Inject

private val logger = LoggerFactory.getLogger(PostConstructInitializerTaskDependencies::class.java)

/**
 * This class is a more structed equivalent ot using @PostConstruct which being deprecated.
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Component
class PostConstructInitializerTaskDependencies : HazelcastTaskDependencies {

    /*
    @Inject
    private lateinit var transporterDatastore: TransporterDatastore

    @Inject
    private lateinit var transporterDependent: Set<TransporterDependent<out Any>>
    */

    @Inject
    private lateinit var byteBlobDataManager: ByteBlobDataManager

    @Inject
    private lateinit var idService: HazelcastIdGenerationService

    @Inject
    private lateinit var byteBlobDataManagerAware: Set<ByteBlobDataManagerAware>

    @Inject
    private lateinit var idGenerationServiceDependent: Set<IdGenerationServiceDependent>

    @Inject
    private lateinit var dataGraphAware: Set<DataGraphAware>

    @Inject
    private lateinit var metastoreAware: Set<MetastoreAware>

    @Inject
    private lateinit var lateInitAware: Set<LateInitAware>

    @Inject
    private lateinit var dataGraphService: DataGraphService

    @Inject
    private lateinit var resolver: DataSourceResolver

    @Inject
    private lateinit var lateInitProvider: LateInitProvider

    @Component
    class PostConstructInitializerTask : HazelcastInitializationTask<PostConstructInitializerTaskDependencies> {
        override fun getInitialDelay(): Long {
            return 0
        }

        override fun initialize(dependencies: PostConstructInitializerTaskDependencies) {
            /*
            dependencies.transporterDependent.forEach {
                it.init(dependencies.transporterDatastore)
                logger.info("Initialized ${it.javaClass} with TransporterDatastore")
            }
            */

            dependencies.byteBlobDataManagerAware.forEach {
                it.setByteBlobDataManager(dependencies.byteBlobDataManager)
                logger.info("Initialized ${it.javaClass} with ByteBlobDataManager")
            }

            dependencies.idGenerationServiceDependent.forEach {
                it.init(dependencies.idService)
                logger.info("Initialized ${it.javaClass} with HazelcastIdGenerationService")
            }

            dependencies.metastoreAware.forEach {
                it.setDataSourceResolver(dependencies.resolver)
                logger.info("Initialized ${it.javaClass} with resolver")
            }

            dependencies.dataGraphAware.forEach {
                it.setDataGraphService(dependencies.dataGraphService)
                logger.info("Initialized ${it.javaClass} with data graph service")
            }

            dependencies.lateInitAware.forEach {
                it.setLateInitProvider(dependencies.lateInitProvider)
                logger.info("Initialized ${it.javaClass} with late init provider")
            }
        }

        override fun after(): Set<Class<out HazelcastInitializationTask<*>>> {
            return setOf()
        }

        override fun getName(): String {
            return Task.POST_INITIALIZER.name
        }

        override fun getDependenciesClass(): Class<out PostConstructInitializerTaskDependencies> {
            return PostConstructInitializerTaskDependencies::class.java
        }

        override fun isRunOnceAcrossCluster(): Boolean {
            return false
        }

        override fun getDependency(): PostConstructInitializerTaskDependencies {
            return super.getDependency()
        }
    }
}
